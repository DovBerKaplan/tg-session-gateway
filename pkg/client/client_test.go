package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// fakeGateway answers like the real botapi plane: Telegram envelope,
// idempotency suppression on sendMessage, queue-ish getUpdates.
type fakeGateway struct {
	mu       sync.Mutex
	sent     []map[string]any
	idemKeys []string
	queue    []json.RawMessage
	consumed int
}

func (f *fakeGateway) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer app-secret" {
			w.WriteHeader(401)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": 401})
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)

		switch r.URL.Path {
		case "/tgapi/botTOK/sendMessage":
			f.mu.Lock()
			key := r.Header.Get("Idempotency-Key")
			dup := key != "" && len(f.idemKeys) > 0 && f.idemKeys[len(f.idemKeys)-1] == key
			if !dup {
				f.sent = append(f.sent, body)
				f.idemKeys = append(f.idemKeys, key)
			}
			f.mu.Unlock()
			if dup {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok": true, "result": nil, "idempotent_replay": true,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": 42},
			})
		case "/tgapi/botTOK/getUpdates":
			f.mu.Lock()
			offset, _ := body["offset"].(float64)
			var out []map[string]any
			for f.consumed < len(f.queue) {
				var u map[string]any
				_ = json.Unmarshal(f.queue[f.consumed], &u)
				id, _ := u["update_id"].(float64)
				if offset > 0 && id < offset { // Telegram: offset=N returns id >= N
					f.consumed++
					continue
				}
				out = append(out, u)
				f.consumed++
				if len(out) >= 2 {
					break
				}
			}
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": out})
		case "/tgapi/botTOK/flooded":
			w.WriteHeader(429)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": false, "error_code": 429, "description": "Too Many Requests",
				"parameters": map[string]any{"retry_after": 7},
			})
		case "/tgapi/botTOK/owned":
			w.WriteHeader(403)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": false, "error_code": 403, "description": "Conflict: gateway owns this method",
			})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	})
}

func newFake(t *testing.T) (*Client, *fakeGateway) {
	f := &fakeGateway{}
	srv := httptest.NewServer(f.handler(t))
	t.Cleanup(srv.Close)
	c := NewBotAPI(srv.URL, "app-secret", "TOK")
	c.http = srv.Client()
	return c, f
}

func TestSendMessageAutoIdempotency(t *testing.T) {
	c, f := newFake(t)
	m, err := c.SendMessage(context.Background(), 7, "hi", nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 42 {
		t.Fatalf("message_id = %d", m.ID)
	}
	// same key twice: the fake suppresses on repeated identical keys —
	// force a replay by reusing the client's explicit path
	_, err = c.SendMessageIdempotent(context.Background(), "fixed-key", 7, "hi", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.SendMessageIdempotent(context.Background(), "fixed-key", 7, "hi", nil)
	if !errors.Is(err, ErrReplayed) {
		t.Fatalf("expected ErrReplayed, got %v", err)
	}
	if len(f.sent) != 2 {
		t.Fatalf("upstream saw %d sends, want 2 (first + explicit)", len(f.sent))
	}
	if f.idemKeys[0] == "" {
		t.Fatal("auto Idempotency-Key missing")
	}
}

func TestPullUpdatesTracksOffset(t *testing.T) {
	c, f := newFake(t)
	for i := int64(1); i <= 3; i++ {
		raw, _ := json.Marshal(map[string]any{"update_id": i, "message": map[string]any{"text": "x"}})
		f.queue = append(f.queue, raw)
	}
	batch, err := c.PullUpdates(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].UpdateID != 1 { // fake caps batches at 2
		t.Fatalf("batch1 = %v", batch)
	}
	batch, err = c.PullUpdates(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0].UpdateID != 3 {
		t.Fatalf("batch2 = %v", batch)
	}
	if c.Offset() != 3 {
		t.Fatalf("cursor = %d, want 3", c.Offset())
	}
	_, err = c.PullUpdates(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAPIErrorHelpers(t *testing.T) {
	c, _ := newFake(t)
	_, err := c.Call(context.Background(), "flooded", nil)
	after, limited := IsRateLimited(err)
	if !limited || after != 7 {
		t.Fatalf("IsRateLimited = %v, %v (err=%v)", after, limited, err)
	}
	_, err = c.Call(context.Background(), "owned", nil)
	if !IsForbidden(err) {
		t.Fatalf("IsForbidden = false for %v", err)
	}
	if IsUnsupported(err) {
		t.Fatal("403 misclassified as 501")
	}
}

func TestMTProtoURLShape(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer srv.Close()
	c := NewMTProto(srv.URL, "app-secret", "main")
	if _, err := c.Call(context.Background(), "send_message", map[string]any{"text": "x"}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/sessions/main/send_message" {
		t.Fatalf("path = %s", gotPath)
	}
}
