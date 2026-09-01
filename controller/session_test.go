package controller

import (
	"strings"
	"testing"

	"github.com/yankeguo/turnhive/agent"
)

// validSessionRequest returns a minimal request that passes Validate.
func validSessionRequest() CreateSessionRequest {
	return CreateSessionRequest{
		Model: ModelSpec{
			URL:      "http://llm/v1/chat/completions",
			Protocol: ProtocolOpenAICompletions,
			Name:     "m",
		},
		Prompt:   PromptSpec{System: "sys"},
		Ironhive: IronhiveSpec{Pool: "default"},
	}
}

func TestCreateSessionRequestValidateModelParams(t *testing.T) {
	// max_context and the known flag pass.
	req := validSessionRequest()
	req.Model.MaxContext = 131072
	req.Model.Features = []string{ModelFeatureSupportImage}
	if err := req.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	// Negative max_context is rejected.
	req = validSessionRequest()
	req.Model.MaxContext = -1
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), "max_context") {
		t.Fatalf("expected max_context error, got %v", err)
	}

	// Unknown features are rejected, naming the offending entry.
	req = validSessionRequest()
	req.Model.Features = []string{ModelFeatureSupportImage, "support_video"}
	if err := req.Validate(); err == nil || !strings.Contains(err.Error(), `unknown feature "support_video"`) {
		t.Fatalf("expected unknown feature error, got %v", err)
	}

	// Features default to empty and pass.
	req = validSessionRequest()
	if err := req.Validate(); err != nil {
		t.Fatalf("default request rejected: %v", err)
	}
}

func TestSessionRecordPersisted(t *testing.T) {
	sess := &Session{}
	sess.recordPersisted(agent.PersistedObject{Path: "b.txt", ObjectKey: "sessions/s/persisted/b.txt", Size: 1})
	sess.recordPersisted(agent.PersistedObject{Path: "a.txt", ObjectKey: "sessions/s/persisted/a.txt", Size: 2})
	// Re-persisting a path replaces the entry.
	sess.recordPersisted(agent.PersistedObject{Path: "b.txt", ObjectKey: "sessions/s/persisted/b.txt", Size: 3})

	got := sess.Persisted()
	if len(got) != 2 {
		t.Fatalf("expected 2 objects after dedup, got %+v", got)
	}
	// Sorted by path; b.txt carries the latest size.
	if got[0].Path != "a.txt" || got[1].Path != "b.txt" || got[1].Size != 3 {
		t.Fatalf("unexpected persisted objects: %+v", got)
	}
}
