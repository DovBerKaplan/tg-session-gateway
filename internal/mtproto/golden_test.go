package mtproto

// Golden-file conformance against pyrofork's OWN serialization: the
// recorded JSON is json.loads(str(raw_update)) for raw TL objects
// built with the real pyrofork constructors — the natural update
// contract (stage 2.0/2.3 of the revised plan). The Python sidecar
// never delivered this shape (pyrofork has no to_dict — it dropped
// real updates); the GO sidecar's SerializeUpdate implements it.
// Regenerate: .venv/bin/python tests/conformance_go/record_golden_raw.py
//
// The raw shape carries the raw-params layout ("types."-prefixed
// constructor names, peer_id/from_id, bytes as their repr); the Go
// serializer emits the consumer-facing subset (bare constructor
// names, chat, from_user_id, data as string) — every assertion below
// pins the equivalence explicitly.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

			// constructor matches modulo the raw "types." namespace prefix
			if want := strings.TrimPrefix(fmt.Sprint(golden["_"]), "types."); fmt.Sprint(got["_"]) != want {
				t.Fatalf("constructor: raw=%v go=%v", golden["_"], got["_"])
			}
			// update_id is a delivery-layer field (assigned at push time)
			for _, k := range []string{"pts", "pts_count"} {
				if golden[k] != nil {
					assertEq(t, golden, got, k)
				}
			}

			switch tc.name {
			case "callback_query":
				// data rides as bytes in the raw shape → str() renders
				// the repr (b'payload'); Go emits the decoded string
				for _, k := range []string{"query_id", "user_id", "msg_id", "chat_instance"} {
					assertEq(t, golden, got, k)
				}
				want := strings.TrimSuffix(strings.TrimPrefix(fmt.Sprint(golden["data"]), "b'"), "'")
				if fmt.Sprint(got["data"]) != want {
					t.Fatalf("data: raw=%v go=%v", golden["data"], got["data"])
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
	if want := strings.TrimPrefix(fmt.Sprint(goldenPeer["_"]), "types."); fmt.Sprint(gotPeer["_"]) != want {
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
		if want := strings.TrimPrefix(fmt.Sprint(g["_"]), "types."); fmt.Sprint(got["_"]) != want {
			t.Fatalf("entity %d constructor: raw=%v go=%v", i, g["_"], got["_"])
		}
		for _, k := range []string{"offset", "length"} {
			assertEq(t, g, got, k)
		}
		// the pyrofork migration reason: expandable blockquote flag
		if g["_"] == "MessageEntityBlockquote" {
			assertEq(t, g, got, "collapsed")
		}
	}
}
