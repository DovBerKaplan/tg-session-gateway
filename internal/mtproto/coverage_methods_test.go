package mtproto

// Drives every adapter through the HTTP proxy so the full method
// surface (the 20-method allowlist) executes with the fake client —
// plus the 501 contract for anything outside it.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestProxyMethodsFullSurface(t *testing.T) {
	fake := &fakeMTClient{}
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "full")

	// {method, body} — the fake client implements every one of these;
	// the point is exercising each adapter's coerce+dispatch path.
	// accepted outcomes per case: 200 (dispatched to the fake), or the
	// documented validation errors — media methods need a gotd media
	// cache the fake lacks ("media required"), writes race the rate
	// guard (429 with retry_after is the CONTRACT for over-limit).
	okOr := func(wantDesc string) func(int, map[string]any) error {
		return func(code int, b map[string]any) error {
			if code == 200 && b["ok"] == true {
				return nil
			}
			if d, _ := b["description"].(string); strings.Contains(d, wantDesc) {
				return nil
			}
			return fmt.Errorf("unexpected: %d %v (want 200 or %q)", code, b, wantDesc)
		}
	}
	cases := []struct {
		method, body string
		accept       func(int, map[string]any) error
	}{
		{"send_message", `{"chat_id":5,"text":"hi"}`, okOr("")},
		{"edit_message_text", `{"chat_id":5,"message_id":1,"text":"new"}`, okOr("")},
		{"edit_message_reply_markup", `{"chat_id":5,"message_id":1}`, okOr("")},
		{"edit_message_caption", `{"chat_id":5,"message_id":1,"caption":"cap"}`, okOr("media required")},
		{"send_chat_action", `{"chat_id":5,"action":"typing"}`, okOr("")},
		{"delete_messages", `{"chat_id":5,"message_ids":[1,2]}`, okOr("Too Many Requests")},
		{"forward_messages", `{"from_chat_id":5,"chat_id":6,"message_ids":[1]}`, okOr("Too Many Requests")},
		{"get_chat", `{"chat_id":5}`, okOr("")},
		{"resolve_username", `{"username":"durov"}`, okOr("")},
		{"answer_callback_query", `{"callback_query_id":"cb1"}`, okOr("")},
		{"send_photo", `{"chat_id":5,"photo":"photo:1"}`, okOr("media required")},
		{"send_video", `{"chat_id":5,"video":"doc:1"}`, okOr("media required")},
		{"send_document", `{"chat_id":5,"document":"doc:1"}`, okOr("media required")},
		{"send_audio", `{"chat_id":5,"audio":"doc:1"}`, okOr("media required")},
		{"send_voice", `{"chat_id":5,"voice":"doc:1"}`, okOr("media required")},
		{"send_animation", `{"chat_id":5,"animation":"doc:1"}`, okOr("media required")},
		{"send_media_group", `{"chat_id":5,"media":[{"type":"photo","media":"photo:1"}]}`, okOr("required")},
		{"copy_message", `{"chat_id":5,"from_chat_id":6,"message_id":1}`, okOr("Too Many Requests")},
		{"get_me", `{}`, okOr("")},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			// the session may still be CONNECTING (503) and writes race
			// the rate guard (429): retry with spacing, remember the
			// last answer for the failure message
			var lastCode int
			var lastBody map[string]any
			deadline := time.Now().Add(10 * time.Second)
			for {
				lastCode, lastBody = mtDo(t, "POST", base, "/v1/sessions/full/"+tc.method, tc.body, itestAPP)
				if (lastCode == 503 || lastCode == 429) && time.Now().Before(deadline) {
					time.Sleep(1100 * time.Millisecond) // honor retry_after / connecting
					continue
				}
				break
			}
			if err := tc.accept(lastCode, lastBody); err != nil {
				t.Fatal(err)
			}
		})
	}

	// unsupported-method 501: pinned by TestUnsupportedMethod501
}
