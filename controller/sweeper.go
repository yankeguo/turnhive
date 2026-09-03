package controller

import (
	"context"
	"log"
	"time"
)

// runSweeper periodically releases the sandboxes of sessions that have
// been idle past idleTimeout, and evicts sessions idle past coldTimeout
// to cold storage (when configured), until Close.
func (c *Controller) runSweeper() {
	defer c.sweeperDone.Done()
	interval := c.idleTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.sweeperStop:
			return
		case <-ticker.C:
			c.reapIdleSandboxes()
			if c.coldTimeout > 0 {
				c.evictColdSessions()
			}
		}
	}
}

// evictColdSessions retires every session that has been inactive for at
// least coldTimeout: it leaves memory and etcd entirely and lives on
// only in S3 (spec, history, persisted files), where any node adopts it
// on the next request. Eviction reuses the teardown actions of DELETE
// (cancel turn — none is running by definition — stop renewal, release
// sandbox) plus closing the event hub so live SSE subscribers reconnect
// and resynchronize.
func (c *Controller) evictColdSessions() {
	c.sessions.Range(func(_, v any) bool {
		sess, ok := v.(*Session)
		if !ok {
			return true
		}
		// filesMu orders the detach against a concurrent file attach:
		// the attach either already injected into this sandbox (fine —
		// the record is in the manifest for the next build too) or sees
		// the sandbox gone and defers to the next build.
		sess.filesMu.Lock()
		sb, stop, evicted := sess.takeIfCold(c.coldTimeout)
		sess.filesMu.Unlock()
		if !evicted {
			return true
		}
		c.sessions.Delete(sess.ID)
		if stop != nil {
			stop()
		}
		if sb != nil {
			log.Printf("session %s idle past %s, releasing sandbox %s", sess.ID, c.coldTimeout, sb.Name)
			releaseSandbox(sb)
		}
		sess.hub.closeAll()
		ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
		if err := c.registry.UnregisterSession(ctx, sess.ID); err != nil {
			log.Printf("unregister cold session %s: %v", sess.ID, err)
		}
		cancel()
		log.Printf("session %s idle past %s, evicted to cold storage", sess.ID, c.coldTimeout)
		return true
	})
}

// ReregisterSessions re-registers the ownership of every session held in
// memory. It is wired to registry.OnReconnected: an etcd keepalive loss
// takes down the node record and every session record with it, and this
// restores the latter once the node record is back.
func (c *Controller) ReregisterSessions(ctx context.Context) {
	c.sessions.Range(func(_, v any) bool {
		sess, ok := v.(*Session)
		if !ok {
			return true
		}
		if err := c.registry.RegisterSession(ctx, sess.ID); err != nil {
			log.Printf("re-register session %s: %v", sess.ID, err)
		}
		return true
	})
}

// reapIdleSandboxes releases the sandbox of every session that has been
// inactive for at least idleTimeout. The session record, its event hub
// and its persisted files survive; the sandbox is rebuilt on the next
// message (see ensureSandbox).
func (c *Controller) reapIdleSandboxes() {
	c.sessions.Range(func(_, v any) bool {
		sess, ok := v.(*Session)
		if !ok {
			return true
		}
		// filesMu orders the detach against a concurrent file attach
		// (see evictColdSessions).
		sess.filesMu.Lock()
		sb, stop := sess.takeSandboxIfIdle(c.idleTimeout)
		sess.filesMu.Unlock()
		if sb == nil {
			return true
		}
		if stop != nil {
			stop()
		}
		log.Printf("session %s idle past %s, releasing sandbox %s", sess.ID, c.idleTimeout, sb.Name)
		releaseSandbox(sb)
		return true
	})
}
