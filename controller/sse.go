package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/yankeguo/turnhive/agent"
)

// sseKeepalive is the interval between SSE comment lines that keep the
// connection alive through proxies.
const sseKeepalive = 15 * time.Second

// sseKeepaliveComment is the comment line emitted between events so idle
// periods (long tool calls, external tool waits, no turn at all) do not
// trip proxy timeouts.
const sseKeepaliveComment = ": keepalive\n\n"

// writeSSEFrame writes one event as an SSE frame: the sequence number in
// the id field (used as Last-Event-ID on reconnect), the event name and
// the JSON payload.
func writeSSEFrame(w io.Writer, ev hubEvent) {
	_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.seq, ev.name, ev.data)
}

// syncMessage is one message of the merged conversation history carried
// by the sync event: completed turns condensed to their {user,
// assistant} pairs, never raw deltas.
type syncMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// writeSSESync writes the sync control event a subscriber receives on
// connect: the currently running turn ("" when idle), the latest
// sequence number, the full merged history and the session's persisted
// objects, so the client can synchronize its state in one frame before
// the backlog replay. It deliberately carries no SSE id: it is a control
// frame that does not occupy an event sequence number, and an id would
// make EventSource record a stale Last-Event-ID, replaying already
// received frames on reconnect (clients take the seq from the payload).
func writeSSESync(w io.Writer, currentTurn string, latestSeq int64, messages []syncMessage, persisted []agent.PersistedObject, files []agent.FileRecord) {
	payload, err := json.Marshal(struct {
		TurnID    string                  `json:"turn_id"`
		Seq       int64                   `json:"seq"`
		Messages  []syncMessage           `json:"messages"`
		Persisted []agent.PersistedObject `json:"persisted"`
		Files     []agent.FileRecord      `json:"files"`
	}{TurnID: currentTurn, Seq: latestSeq, Messages: messages, Persisted: persisted, Files: files})
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: sync\ndata: %s\n\n", payload)
}
