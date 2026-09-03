package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/agent"
)

// allocateTimeout bounds sandbox allocation; ironhive may block up to 30s
// server-side waiting for a standby pod.
const allocateTimeout = 40 * time.Second

// skillsRoot is the sandbox directory, relative to the sandbox's working
// directory, where skill tarballs are installed; it is read-only for the
// agent. turnhive deliberately does not assume the sandbox's working
// directory: all tool paths are relative to it.
const skillsRoot = ".agents/skills"

// releaseSessionSandbox rolls back a failed session creation: detaches
// the sandbox, stops its lease renewal and releases it.
func releaseSessionSandbox(sess *Session) {
	if sb, stop := sess.takeSandbox(); sb != nil {
		if stop != nil {
			stop()
		}
		releaseSandbox(sb)
	}
}

// skillRefsOf converts the session spec's skills to agent skill refs.
func skillRefsOf(skills []SkillSpec) []agent.SkillRef {
	refs := make([]agent.SkillRef, 0, len(skills))
	for _, s := range skills {
		refs = append(refs, agent.SkillRef{Name: s.Name, Description: s.Description, ObjectKey: s.ObjectKey})
	}
	return refs
}

// buildLoop creates the agent Loop for a session from its spec and the
// given sandbox. Called at creation and every time the sandbox is
// rebuilt after an idle reap.
func (c *Controller) buildLoop(sess *Session, sandbox *ironhive.Sandbox) *agent.Loop {
	req := sess.Spec
	skillRefs := skillRefsOf(req.Skills)
	externalTools := make([]agent.ExternalToolSpec, 0, len(req.Tools))
	for _, t := range req.Tools {
		externalTools = append(externalTools, agent.ExternalToolSpec{Name: t.Name, Description: t.Description, Parameters: t.Parameters})
	}
	mcpServers := make([]agent.MCPServerSpec, 0, len(req.MCPServers))
	for _, m := range req.MCPServers {
		mcpServers = append(mcpServers, agent.MCPServerSpec{Name: m.Name, URL: m.URL, Headers: m.Headers, Transport: m.Transport})
	}
	return agent.NewLoop(agent.LoopConfig{
		ModelURL:      req.Model.URL,
		ModelHeaders:  req.Model.Headers,
		ModelName:     req.Model.Name,
		SystemPrompt:  agent.BuildSystemPrompt(req.Prompt.System, skillRefs, skillsRoot),
		Sandbox:       sandbox,
		SupportImage:  slices.Contains(req.Model.Features, ModelFeatureSupportImage),
		PersistStore:  c.store,
		SessionID:     sess.ID,
		OnPersisted:   sess.recordPersisted,
		ExternalTools: externalTools,
		Waiter:        sess,
		History:       agent.S3History(c.store, sess.ID),
		MaxContext:    req.Model.MaxContext,
		MCPServers:    mcpServers,
		OnMCPStatus: func(st agent.MCPServerStatus) {
			if st.Err != nil {
				log.Printf("session %s mcp %s: %v", sess.ID, st.Name, st.Err)
			} else {
				log.Printf("session %s mcp %s: %d tools mounted", sess.ID, st.Name, st.ToolCount)
			}
		},
		// A backgrounded command that exits on its own is queued and
		// drained into a synthesized user turn; a busy session holds the
		// queue until the running turn completes.
		OnBackgroundExit: func(info agent.BgProcessExit) {
			log.Printf("session %s background process %d exited (code %d)", sess.ID, info.Pid, info.ExitCode)
			sess.recordBackgroundExit(info)
			c.drainBackgroundExits(sess)
		},
	})
}

// ensureSandbox makes sure the session holds a live sandbox. When the
// sandbox was reaped for idleness (sessions outlive sandboxes), it is
// rebuilt: allocate, reinstall skills, restore persisted files, rebuild
// the agent Loop (its history reloads from S3) and restart lease
// renewal.
func (c *Controller) ensureSandbox(ctx context.Context, sess *Session) error {
	if _, ok := sess.liveSandbox(); ok {
		return nil
	}
	sandbox, err := c.ironhive.Allocate(ctx, sess.Spec.Ironhive.Pool, c.sandboxLease)
	if err != nil {
		return fmt.Errorf("allocate sandbox from pool %q: %w", sess.Spec.Ironhive.Pool, err)
	}
	if err = agent.InstallSkills(ctx, sandbox, c.store, skillRefsOf(sess.Spec.Skills), skillsRoot, agent.PresignURLTTL); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("install skills: %w", err)
	}
	if err = agent.RestorePersisted(ctx, sandbox, c.store, sess.Persisted(), agent.PresignURLTTL); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("restore persisted files: %w", err)
	}
	if err = agent.InjectFiles(ctx, sandbox, c.fileStore, sess.Files(), agent.PresignURLTTL); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("inject user files: %w", err)
	}
	l := c.buildLoop(sess, sandbox)
	if err = l.LoadHistory(ctx); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("load history: %w", err)
	}
	sess.setLoop(l)
	// Keep the sandbox lease alive while the session holds it; without
	// renewal ironhive destroys the sandbox when the lease expires.
	renewCtx, stopRenew := context.WithCancel(context.Background())
	go c.renewSandbox(renewCtx, sandbox)
	if !sess.setSandbox(sandbox, stopRenew) {
		// The session was deleted (or the node is shutting down) while
		// the sandbox was being rebuilt; stop the renewal and release
		// the sandbox instead of leaking both.
		stopRenew()
		releaseSandbox(sandbox)
		return errors.New("session closed during sandbox rebuild")
	}
	return nil
}

// renewSandbox renews the sandbox lease at half-lease intervals until ctx
// is cancelled (session deleted or node shutdown). Failures are only
// logged: the turn that next touches a dead sandbox reports the error.
func (c *Controller) renewSandbox(ctx context.Context, sb *ironhive.Sandbox) {
	interval := c.sandboxLease / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, lookupTimeout)
			if err := sb.Renew(renewCtx, c.sandboxLease); err != nil {
				log.Printf("renew sandbox %s: %v", sb.Name, err)
			}
			cancel()
		}
	}
}

// releaseSandbox destroys a session's sandbox on a detached context, so
// cleanup survives client disconnects. Failures are only logged: the
// sandbox lease guarantees eventual reclamation by ironhive.
func releaseSandbox(sb *ironhive.Sandbox) {
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	if err := sb.Release(ctx); err != nil {
		log.Printf("release sandbox %s: %v", sb.Name, err)
	}
}
