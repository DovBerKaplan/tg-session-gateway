package mtproto

// Golden-file conformance: the recorded JSON is what the REAL Python
// sidecar (mtgateway, FakeClient harness — only Telegram faked)
// delivered to consumers via getUpdates for classic Pyrogram to_dict
// shapes. Regenerate with:
//
//	.venv/bin/python tests/conformance_go/record_golden_mt.py
//
// SerializeUpdate is Go's port of that contract. The Python dicts carry
// the raw-params layout (peer_id, from_id, …); the Go serializer emits
// the consumer-facing subset (chat, from_user_id, …) — the assertions
// below pin each equivalence explicitly instead of byte-comparing
// trees that are documented supersets/subsets of each other.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/tg"
)

// goldenCase binds a recorded golden file to the equivalent gotd update.
type goldenCase struct {
	name  string
	goUpd tg.UpdateClass
	// updateID mirrors the corpus's update_id (the mt sidecar consumer key).
	updateID int64
}

func goldenCases() []goldenCase {
	return []goldenCase{
		{
			name:     "new_message_private",
			updateID: 9001,
			goUpd: &tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:      42,
					Date:    1700000000,
					Message: "hello world",
					PeerID:  &tg.PeerUser{UserID: 10},
					FromID:  &tg.PeerUser{UserID: 20},
					Entities: []tg.MessageEntityClass{
						&tg.MessageEntityBold{Offset: 0, Length: 5},
						&tg.MessageEntityBlockquote{Offset: 6, Length: 5, Collapsed: true},
					},
				},
				Pts: 100, PtsCount: 1,
			},
		},
		{
			name:     "new_message_group",
			updateID: 9002,
			goUpd: &tg.UpdateNewMessage{
				Message: &tg.Message{
					ID: 43, Date: 1700000001, Message: "in the group",
					PeerID: &tg.PeerChat{ChatID: 77},
				},
				Pts: 101, PtsCount: 1,
			},
		},
		{
			name:     "new_message_channel",
			updateID: 9003,
			goUpd: &tg.UpdateNewChannelMessage{
				Message: &tg.Message{
					ID: 44, Date: 1700000002, Message: "channel post",
					PeerID: &tg.PeerChannel{ChannelID: 1234},
				},
				Pts: 102, PtsCount: 1,
			},
		},
		{
			name:     "edit_message",
			updateID: 9004,
			goUpd: &tg.UpdateEditMessage{
				Message: &tg.Message{
					ID: 42, Date: 1700000000, Message: "hello world (edited)",
					PeerID: &tg.PeerUser{UserID: 10},
				},
				Pts: 103, PtsCount: 0,
			},
		},
		{
			name:     "callback_query",
			updateID: 9005,
			goUpd: &tg.UpdateBotCallbackQuery{
				QueryID: 555, UserID: 20,
				Peer:         &tg.PeerUser{UserID: 10},
				MsgID:        7,
				ChatInstance: -5500000000000000000,
				Data:         []byte("payload"),
			},
		},
		{
			name:     "message_with_reply_markup",
			updateID: 9006,
			goUpd: &tg.UpdateNewMessage{
				Message: &tg.Message{
					ID: 45, Date: 1700000003, Message: "pick one",
					PeerID: &tg.PeerUser{UserID: 10},
					ReplyMarkup: &tg.ReplyInlineMarkup{
						Rows: []tg.KeyboardInlineButtonRow{
							{Buttons: []tg.KeyboardInlineButton{{Text: "Open"}}},
						},
					},
				},
				Pts: 104, PtsCount: 1,
			},
		},
	}
}

func TestGoldenAgainstPythonSidecar(t *testing.T) {
	dir := "testdata/golden"
	for _, tc := range goldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, tc.name+".json"))
			if err != nil {
				t.Fatalf("golden missing (%v); regenerate with tests/conformance_go/record_golden_mt.py", err)
			}
			var golden map[string]any
			if err := json.Unmarshal(raw, &golden); err != nil {
				t.Fatalf("golden unparseable: %v", err)
			}

			got := SerializeUpdate(tc.goUpd)
			if got == nil {
				t.Fatal("SerializeUpdate dropped the update")
			}
			// the delivery layer attaches the consumer's update id —
			// in Python it rode in-band inside the dict, in Go it is
			// assigned at pushUpdate time (persisted as the queue id)
			got["update_id"] = tc.updateID

			// constructor + envelope fields match verbatim
			assertEq(t, golden, got, "_")
			assertEq(t, golden, got, "update_id")
			for _, k := range []string{"pts", "pts_count"} {
				if golden[k] != nil {
					assertEq(t, golden, got, k)
				}
			}

			switch tc.name {
			case "callback_query":
				// every consumer field matches verbatim (data pinned as
				// str — Python dead-letters real bytes payloads)
				for _, k := range []string{"query_id", "user_id", "msg_id", "data", "chat_instance"} {
					assertEq(t, golden, got, k)
				}
				gp := golden["peer"].(map[string]any)
				assertPeerMatches(t, gp, got["peer"].(map[string]any))
			default:
				gm := golden["message"].(map[string]any)
				gom := got["message"].(map[string]any)
				assertEq(t, gm, gom, "id")
				assertEq(t, gm, gom, "date")
				assertEq(t, gm, gom, "message")

				// from_user_id (Go) is derived from from_id (Pyrogram)
				if fid, ok := gm["from_id"].(map[string]any); ok {
					assertEq(t, map[string]any{"from_user_id": fid["user_id"]},
						gom, "from_user_id")
				}

				// chat (Go) is derived from peer_id (Pyrogram): users
				// stay positive, basic groups flip negative, channels
				// keep the raw id
				peer := gm["peer_id"].(map[string]any)
				wantChat := map[string]any{}
				switch peer["_"] {
				case "PeerUser":
					wantChat = map[string]any{"id": peer["user_id"], "type": "private"}
				case "PeerChat":
					wantChat = map[string]any{"id": negNum(peer["chat_id"]), "type": "chat"}
				case "PeerChannel":
					wantChat = map[string]any{"id": peer["channel_id"], "type": "channel"}
				}
				gochat := gom["chat"].(map[string]any)
				for k, v := range wantChat {
					gotV, ok := gochat[k]
					if !ok || num(gotV) != num(v) {
						t.Fatalf("chat.%s = %v (%T), Python-derived = %v", k, gotV, gotV, v)
					}
				}

				// entities: every Go entity field must equal the golden's
				assertEntities(t, gm, gom)

				// reply markup round-shape (constructor + button text)
				if grm, ok := gm["reply_markup"].(map[string]any); ok && grm["_"] == "ReplyInlineMarkup" {
					gorm, ok := gom["reply_markup"].(map[string]any)
					if !ok || gorm["_"] != "InlineKeyboardMarkup" {
						t.Fatalf("reply_markup constructor mismatch: %v vs %v", grm["_"], gom["reply_markup"])
					}
				}
			}
		})
	}
}

func assertEq(t *testing.T, golden, got map[string]any, key string) {
	t.Helper()
	gv, inGolden := golden[key]
	gotv, inGot := got[key]
	if !inGolden || !inGot {
		t.Fatalf("key %q missing: golden=%v got=%v", key, inGolden, inGot)
	}
	if num(gv) != num(gotv) {
		t.Fatalf("key %q: golden=%v (%T), go=%v (%T)", key, gv, gv, gotv, gotv)
	}
}

// num normalizes JSON float64 vs Go int64/int for numeric comparison;
// non-numbers pass through so == semantics apply to strings/bools.
func num(v any) any {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return v
}

// negNum is num() negated (for the Pyrogram→Bot-API group-id flip).
func negNum(v any) any {
	if n, ok := num(v).(int64); ok {
		return -n
	}
	return v
}

func assertPeerMatches(t *testing.T, goldenPeer, gotPeer map[string]any) {
	t.Helper()
	if goldenPeer["_"] != gotPeer["_"] {
		t.Fatalf("peer constructor: golden=%v go=%v", goldenPeer["_"], gotPeer["_"])
	}
	for _, k := range []string{"user_id", "chat_id", "channel_id"} {
		if goldenPeer[k] != nil {
			assertEq(t, goldenPeer, gotPeer, k)
		}
	}
}

func assertEntities(t *testing.T, goldenMsg, gotMsg map[string]any) {
	t.Helper()
	gEnts, _ := goldenMsg["entities"].([]any)
	gotEnts, _ := gotMsg["entities"].([]any)
	if len(gEnts) == 0 {
		return
	}
	if len(gotEnts) != len(gEnts) {
		t.Fatalf("entity count: golden=%d go=%d", len(gEnts), len(gotEnts))
	}
	for i, ge := range gEnts {
		g := ge.(map[string]any)
		got := gotEnts[i].(map[string]any)
		for _, k := range []string{"_", "offset", "length"} {
			assertEq(t, g, got, k)
		}
		// the pyrofork migration reason: expandable blockquote flag
		if g["_"] == "MessageEntityBlockquote" {
			assertEq(t, g, got, "collapsed")
		}
	}
}
