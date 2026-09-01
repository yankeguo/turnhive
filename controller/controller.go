// Package controller holds the HTTP handlers and routing for turnhive.
package controller

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

// RegisterRoutes registers all routes on the given mux using the Go 1.22+
// method-and-pattern routing syntax.
func RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /v1/sessions", handleCreateSession)
	mux.HandleFunc("POST /v1/sessions/{id}/messages", handleCreateMessage)
	mux.HandleFunc("DELETE /v1/sessions/{id}", handleDeleteSession)
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleCreateSession is a placeholder: it returns a random session ID
// without allocating any real resource.
func handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var b [16]byte
	_, _ = rand.Read(b[:])
	writeJSON(w, http.StatusCreated, map[string]string{"id": hex.EncodeToString(b[:])})
}

// handleCreateMessage is a placeholder for the streaming (SSE) interaction
// endpoint.
func handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "not implemented"})
}

// handleDeleteSession is a placeholder: it accepts any session ID and
// releases nothing.
func handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
