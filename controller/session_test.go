package controller

import (
	"strings"
	"testing"
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
