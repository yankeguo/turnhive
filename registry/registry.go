// Package registry implements etcd-backed node discovery, liveness, and
// session ownership for turnhive.
//
// Key layout under the configured prefix (default "turnhive"):
//
//	{prefix}/nodes/{nodeID}       -> JSON {"addr": "http://10.0.0.1:8080"}
//	{prefix}/sessions/{sessionID} -> nodeID
//
// Both kinds of keys are attached to the owning node's lease, so a node
// that crashes or loses its keepalive automatically takes its node record
// and all of its session records down with it.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// nodeRecord is the value stored at {prefix}/nodes/{nodeID}.
type nodeRecord struct {
	Addr string `json:"addr"`
}

// Registry registers the local node and its sessions in etcd and answers
// ownership lookups for any session in the cluster.
type Registry struct {
	client    *clientv3.Client
	prefix    string
	nodeID    string
	advertise string
	ttl       time.Duration

	done      chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex
	leaseID clientv3.LeaseID
}

// New creates a Registry. RegisterNode must be called before any session
// operation.
func New(client *clientv3.Client, prefix, nodeID, advertise string, ttl time.Duration) *Registry {
	return &Registry{
		client:    client,
		prefix:    prefix,
		nodeID:    nodeID,
		advertise: advertise,
		ttl:       ttl,
		done:      make(chan struct{}),
	}
}

func (r *Registry) nodeKey(nodeID string) string {
	return fmt.Sprintf("%s/nodes/%s", r.prefix, nodeID)
}

func (r *Registry) sessionKey(sessionID string) string {
	return fmt.Sprintf("%s/sessions/%s", r.prefix, sessionID)
}

// RegisterNode grants the node lease, publishes the node record under it,
// and starts a background keepalive loop. The keepalive stops when ctx is
// cancelled; the lease then expires after the TTL.
func (r *Registry) RegisterNode(ctx context.Context) error {
	lease, err := r.client.Grant(ctx, int64(r.ttl/time.Second))
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}

	value, err := json.Marshal(nodeRecord{Addr: r.advertise})
	if err != nil {
		return fmt.Errorf("marshal node record: %w", err)
	}
	if _, err = r.client.Put(ctx, r.nodeKey(r.nodeID), string(value), clientv3.WithLease(lease.ID)); err != nil {
		return fmt.Errorf("put node record: %w", err)
	}

	keepAliveCh, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("start lease keepalive: %w", err)
	}
	go func() {
		for range keepAliveCh {
		}
		// The keepalive channel only closes early when the etcd client has
		// given up on the lease (e.g. after a network partition): the node
		// record expires and every RegisterSession would fail with "lease
		// not found" from here on. Re-register with backoff until success
		// or shutdown. Note that session ownership records bound to the
		// lost lease are NOT restored by this recovery; sessions created
		// while the node was unregistered stay unregistered (their owner
		// lookup fails open until the session ends).
		if ctx.Err() != nil {
			return
		}
		log.Printf("registry: lease keepalive channel closed, re-registering node")
		backoff := time.Second
		for {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-r.done:
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := r.RegisterNode(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("registry: re-register node: %v (retrying in %s)", err, backoff)
				backoff = min(backoff*2, 30*time.Second)
				continue
			}
			log.Printf("registry: node re-registered")
			return
		}
	}()

	r.mu.Lock()
	r.leaseID = lease.ID
	r.mu.Unlock()
	return nil
}

// Close revokes the node lease, immediately deleting the node record and
// every session record owned by this node, and stops keepalive recovery.
func (r *Registry) Close(ctx context.Context) error {
	r.closeOnce.Do(func() { close(r.done) })
	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()
	if leaseID == 0 {
		return nil
	}
	if _, err := r.client.Revoke(ctx, leaseID); err != nil {
		return fmt.Errorf("revoke lease: %w", err)
	}
	r.mu.Lock()
	r.leaseID = 0
	r.mu.Unlock()
	return nil
}

// RegisterSession records that sessionID is owned by this node. The record
// is attached to the node lease, so it disappears if the node dies.
func (r *Registry) RegisterSession(ctx context.Context, sessionID string) error {
	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()
	if leaseID == 0 {
		return fmt.Errorf("node is not registered")
	}
	if _, err := r.client.Put(ctx, r.sessionKey(sessionID), r.nodeID, clientv3.WithLease(leaseID)); err != nil {
		return fmt.Errorf("put session record: %w", err)
	}
	return nil
}

// UnregisterSession removes the ownership record for sessionID.
func (r *Registry) UnregisterSession(ctx context.Context, sessionID string) error {
	if _, err := r.client.Delete(ctx, r.sessionKey(sessionID)); err != nil {
		return fmt.Errorf("delete session record: %w", err)
	}
	return nil
}

// SessionOwner resolves the advertise address of the node that owns
// sessionID. ok is false when the session does not exist or its owner node
// is no longer alive.
func (r *Registry) SessionOwner(ctx context.Context, sessionID string) (addr string, ok bool, err error) {
	resp, err := r.client.Get(ctx, r.sessionKey(sessionID))
	if err != nil {
		return "", false, fmt.Errorf("get session record: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	nodeID := string(resp.Kvs[0].Value)
	if nodeID == r.nodeID {
		return r.advertise, true, nil
	}

	nodeResp, err := r.client.Get(ctx, r.nodeKey(nodeID))
	if err != nil {
		return "", false, fmt.Errorf("get node record: %w", err)
	}
	if len(nodeResp.Kvs) == 0 {
		return "", false, nil
	}
	var record nodeRecord
	if err = json.Unmarshal(nodeResp.Kvs[0].Value, &record); err != nil {
		return "", false, fmt.Errorf("parse node record: %w", err)
	}
	return record.Addr, true, nil
}
