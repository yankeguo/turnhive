package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// headerForwarded marks a request that has already been forwarded by
// another node, preventing forwarding loops.
const headerForwarded = "X-Turnhive-Forwarded"

// routeSession ensures the request is handled on the node that owns the
// session, adopting the session from storage when it is cold. It reports
// whether the session is local and the caller should proceed; otherwise
// the response has already been written.
func (c *Controller) routeSession(w http.ResponseWriter, r *http.Request, id string) bool {
	return c.routeSessionMode(w, r, id, true)
}

// routeSessionMode is routeSession with control over adoption. The cancel
// endpoint passes allowAdopt=false: a cold session has no running turn by
// definition, so adopting it (S3 reads, an etcd claim) just to answer
// no_turn_running would be wasted work.
func (c *Controller) routeSessionMode(w http.ResponseWriter, r *http.Request, id string, allowAdopt bool) bool {
	if _, ok := c.sessions.Load(id); ok {
		return true
	}
	// An adoption is in flight on this node (a concurrent request is
	// recovering this cold session): wait for it instead of duplicating
	// the work — or 404ing a forwarded request whose owner is still
	// mid-adoption.
	if ch, ok := c.adopting.Load(id); ok {
		select {
		case <-ch.(chan struct{}):
		case <-r.Context().Done():
			// The in-flight adoption outlived the client (or its
			// patience): say so instead of hanging the connection open
			// with no status at all.
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "session adoption in progress"})
			return false
		}
		if _, ok = c.sessions.Load(id); ok {
			return true
		}
	}
	// Already forwarded once: the owner does not have this session,
	// so it does not exist. Never forward a second time.
	if r.Header.Get(headerForwarded) != "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return false
	}

	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	addr, ok, err := c.registry.SessionOwner(ctx, id)
	if err != nil {
		log.Printf("lookup session %s owner: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to locate session"})
		return false
	}
	if !ok {
		if !allowAdopt {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
			return false
		}
		// No owner record: the session may be cold — its owner node died
		// (the lease took the record down) or it was evicted past
		// cold_timeout. Adopt it from storage when a spec exists.
		adoptCtx, adoptCancel := context.WithTimeout(r.Context(), adoptTimeout)
		defer adoptCancel()
		adopted, aerr := c.adoptSession(adoptCtx, id)
		switch {
		case errors.Is(aerr, errClaimLost):
			// A concurrent adoption claimed the session: serve it locally
			// when this node won in the meantime, otherwise re-resolve
			// and forward to the winner. The first lookup context may be
			// expired by the adoption attempt; use a fresh one.
			if _, ok = c.sessions.Load(id); ok {
				return true
			}
			retryCtx, retryCancel := context.WithTimeout(r.Context(), lookupTimeout)
			defer retryCancel()
			addr, ok, err = c.registry.SessionOwner(retryCtx, id)
			if err != nil || !ok {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
				return false
			}
		case aerr != nil:
			log.Printf("adopt session %s: %v", id, aerr)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to recover session"})
			return false
		case adopted:
			return true
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
			return false
		}
	}
	p, err := c.proxy(addr)
	if err != nil {
		// The address came from etcd (written by another, possibly
		// misbehaving or outdated node): report a bad gateway instead of
		// taking this process down.
		log.Printf("session %s owner address: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "invalid session owner address"})
		return false
	}
	p.ServeHTTP(w, r)
	return false
}

// proxy returns the cached reverse proxy for the given owner node
// address. The address comes from etcd, so it is validated here.
func (c *Controller) proxy(addr string) (*httputil.ReverseProxy, error) {
	if p, ok := c.proxies.Load(addr); ok {
		return p.(*httputil.ReverseProxy), nil
	}
	target, err := url.Parse(addr)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("invalid node address %q", addr)
	}
	p := &httputil.ReverseProxy{
		FlushInterval: -1, // stream responses (SSE) without buffering
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = target.Host
			pr.Out.Header.Set(headerForwarded, "1")
		},
	}
	actual, _ := c.proxies.LoadOrStore(addr, p)
	return actual.(*httputil.ReverseProxy), nil
}
