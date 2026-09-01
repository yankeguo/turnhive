package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/config"
	"github.com/yankeguo/turnhive/storage"
)

func TestBuildSystemPrompt(t *testing.T) {
	got := BuildSystemPrompt("base prompt", []SkillRef{
		{Name: "pdf", Description: "Work with PDF files"},
		{Name: "sql", Description: "Query databases"},
	}, "/skills")
	want := "base prompt\n\n" +
		"### Skill: pdf\n\nWork with PDF files\n\n(Skill files on disk, read-only: /skills/pdf/)\n\n" +
		"### Skill: sql\n\nQuery databases\n\n(Skill files on disk, read-only: /skills/sql/)"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	if got := BuildSystemPrompt("base prompt", nil, "/skills"); got != "base prompt" {
		t.Fatalf("expected unchanged prompt, got %q", got)
	}
}

// newPresignStore builds a Store whose presigning works offline — the
// endpoint is never contacted by PresignGet.
func newPresignStore(t *testing.T) *storage.Store {
	t.Helper()
	insecure := false
	store, err := storage.New(config.S3Config{
		Endpoint:        "s3.test.invalid:9000",
		Region:          "us-east-1",
		Bucket:          "bucket",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		UseSSL:          &insecure,
		ForcePathStyle:  true,
		Prefix:          "turnhive",
	})
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	return store
}

func TestInstallSkills(t *testing.T) {
	sb, f := newFakeIronhive(t)
	store := newPresignStore(t)

	skills := []SkillRef{
		{Name: "pdf", Description: "Work with PDF files", ObjectKey: "skills/pdf.tar"},
		{Name: "sql", Description: "Query databases", ObjectKey: "skills/sql.tar"},
	}
	if err := InstallSkills(context.Background(), sb, store, skills, "/skills", time.Minute); err != nil {
		t.Fatalf("InstallSkills: %v", err)
	}

	calls := f.recordedTarCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 tar PUTs, got %d", len(calls))
	}
	if calls[0].Path != "/skills/pdf" || calls[1].Path != "/skills/sql" {
		t.Fatalf("unexpected tar paths %+v", calls)
	}
	for i, key := range []string{"turnhive/skills/pdf.tar", "turnhive/skills/sql.tar"} {
		if !strings.Contains(calls[i].URL, key) {
			t.Fatalf("expected presigned url for %s, got %q", key, calls[i].URL)
		}
		if !strings.Contains(calls[i].URL, "X-Amz-Signature=") {
			t.Fatalf("expected presigned-looking url, got %q", calls[i].URL)
		}
	}
}

func TestInstallSkillsFailure(t *testing.T) {
	store := newPresignStore(t)

	// The agent endpoint answers every tar PUT with a 500.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /controller/v1/allocate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"sandbox": "s", "leaseExpires": %s}`, jsonString(time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)))
	})
	mux.HandleFunc("/agent/v1/tar", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"message": "boom"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	sb, err := ironhive.NewClient(srv.URL).Allocate(context.Background(), "default", time.Minute)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}

	err = InstallSkills(context.Background(), sb, store, []SkillRef{
		{Name: "pdf", ObjectKey: "skills/pdf.tar"},
	}, "/skills", time.Minute)
	if err == nil || !strings.Contains(err.Error(), `install skill "pdf"`) {
		t.Fatalf("expected install error, got %v", err)
	}
}
