package mtproto

// Serialization + coercion unit tests: the Pyrogram-dict JSON shapes
// are the consumer contract (golden files recorded from the Python
// sidecar pin them at cutover; these pin the structural invariants).

import (
	"encoding/json"
	"testing"

	"github.com/gotd/td/tg"
)

func TestSerializeUpdateNewMessage(t *testing.T) {
	u := &tg.UpdateNewMessage{
		Message: &tg.Message{
			ID:      42,
			Date:    1700000000,
			PeerID:  &tg.PeerUser{UserID: 10},
			FromID:  &tg.PeerUser{UserID: 20},
			Message: "hello",
			Entities: []tg.MessageEntityClass{
				&tg.MessageEntityBold{Offset: 0, Length: 2},
				&tg.MessageEntityBlockquote{Offset: 3, Length: 2, Collapsed: true},
			},
		},
		Pts: 100, PtsCount: 1,
	}
	d := SerializeUpdate(u)
	if d["_"] != "UpdateNewMessage" {
		t.Fatalf("constructor = %v", d["_"])
	}
	msg := d["message"].(map[string]any)
	if msg["id"] != int64(42) || msg["message"] != "hello" {
		t.Fatalf("message = %v", msg)
	}
	if msg["from_user_id"] != int64(20) {
		t.Fatalf("from_user_id = %v", msg["from_user_id"])
	}
	chat := msg["chat"].(map[string]any)
	if chat["id"] != int64(10) || chat["type"] != "private" {
		t.Fatalf("chat = %v", chat)
	}
	entities := msg["entities"].([]any)
	bq := entities[1].(map[string]any)
	if bq["_"] != "MessageEntityBlockquote" || bq["collapsed"] != true {
		t.Fatalf("blockquote expandable flag lost: %v", bq)
	}

	// JSON round-trips (the wire shape)
	if raw, err := json.Marshal(d); err != nil || len(raw) == 0 {
		t.Fatalf("marshal: %v %s", err, raw)
	}
}

func TestSerializeChatIdShapes(t *testing.T) {
	// Pyrogram marks basic-group chats with NEGATIVE ids
	chat := SerializeChat(&tg.PeerChat{ChatID: 7})
	if chat["id"] != int64(-7) || chat["type"] != "chat" {
		t.Fatalf("group chat id = %v", chat)
	}
	// channels keep the -100-prefixed positive raw id here (peer form)
	ch := SerializeChat(&tg.PeerChannel{ChannelID: 1234})
	if ch["id"] != int64(1234) || ch["type"] != "channel" {
		t.Fatalf("channel chat = %v", ch)
	}
}

func TestSerializeCallbackQuery(t *testing.T) {
	u := &tg.UpdateBotCallbackQuery{
		QueryID: 5, UserID: 20,
		Peer:  &tg.PeerUser{UserID: 10},
		MsgID: 7, ChatInstance: 99, Data: []byte("payload"),
	}
	d := SerializeUpdate(u)
	if d["_"] != "UpdateBotCallbackQuery" {
		t.Fatalf("constructor = %v", d["_"])
	}
	if d["data"] != "payload" {
		t.Fatalf("data = %v", d["data"])
	}
	if s, ok := d["chat_instance"].(string); !ok || s != "99" {
		t.Fatalf("chat_instance = %v", d["chat_instance"])
	}
}

func TestSerializeUnknownUpdatePassthrough(t *testing.T) {
	d := SerializeUpdate(&tg.UpdateReadHistoryOutbox{Peer: &tg.PeerUser{UserID: 1}, MaxID: 5})
	if d == nil {
		t.Fatal("unknown update vanished")
	}
	if d["_"] != "UpdateReadHistoryOutbox" {
		t.Fatalf("constructor passthrough = %v", d["_"])
	}
}

func TestSerializeReplyMarkupRoundTrip(t *testing.T) {
	kb := CoerceReplyMarkup(map[string]any{
		"inline_keyboard": []any{
			[]any{
				map[string]any{"text": "Open", "url": "https://example.com"},
				map[string]any{"text": "Go", "callback_data": "go"},
				map[string]any{"text": "Buy", "buy": true},
			},
		},
	})
	out := SerializeReplyMarkup(kb)
	if out["_"] != "InlineKeyboardMarkup" {
		t.Fatalf("constructor = %v", out["_"])
	}
	rows := out["inline_keyboard"].([]any)
	btns := rows[0].([]any)
	url := btns[0].(map[string]any)
	if url["url"] != "https://example.com" {
		t.Fatalf("url button = %v", url)
	}
	cb := btns[1].(map[string]any)
	if cb["callback_data"] != "go" {
		t.Fatalf("callback button = %v", cb)
	}
	buy := btns[2].(map[string]any)
	if buy["buy"] != true {
		t.Fatalf("buy button = %v", buy)
	}
}

func TestCoerceReplyMarkupVariants(t *testing.T) {
	// reply keyboard with resize + persistence
	kb := CoerceReplyMarkup(map[string]any{
		"keyboard":        []any{[]any{map[string]any{"text": "Hi"}}},
		"resize_keyboard": true,
		"is_persistent":   true,
	})
	rk, ok := kb.(*tg.ReplyKeyboardMarkup)
	if !ok || !rk.Resize || !rk.Persistent || len(rk.Rows) != 1 {
		t.Fatalf("reply keyboard = %#v", kb)
	}
	if rk.Rows[0].Buttons[0].Text != "Hi" {
		t.Fatalf("button = %#v", rk.Rows[0].Buttons[0])
	}

	// force reply
	if _, ok := CoerceReplyMarkup(map[string]any{"force_reply": true}).(*tg.ReplyKeyboardForceReply); !ok {
		t.Fatal("force_reply coercion failed")
	}
	// remove keyboard
	if _, ok := CoerceReplyMarkup(map[string]any{"remove_keyboard": true}).(*tg.ReplyKeyboardHide); !ok {
		t.Fatal("remove_keyboard coercion failed")
	}
	// switch_inline_query_current_chat maps to SamePeer
	sw := CoerceReplyMarkup(map[string]any{
		"inline_keyboard": []any{[]any{
			map[string]any{"text": "t", "switch_inline_query_current_chat": "q"},
		}},
	}).(*tg.ReplyInlineMarkup)
	st := sw.Rows[0].Buttons[0].Type.(*tg.InlineButtonTypeSwitchInline)
	if st.Query != "q" || !st.SamePeer {
		t.Fatalf("switch_inline = %#v", st)
	}
	// nil/garbage → nil (sent without markup)
	if CoerceReplyMarkup(nil) != nil || CoerceReplyMarkup("junk") != nil {
		t.Fatal("non-map markup must coerce to nil")
	}
}

func TestFlattenUpdatesShapes(t *testing.T) {
	// UpdateShortMessage synthesizes UpdateNewMessage with a Message
	got := flattenUpdates(&tg.UpdateShortMessage{
		ID: 9, UserID: 10, Message: "short", Pts: 5, Date: 1,
	})
	if len(got) != 1 {
		t.Fatalf("flattened %d updates", len(got))
	}
	nm, ok := got[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("type = %T", got[0])
	}
	if m, ok := nm.Message.(*tg.Message); !ok || m.Message != "short" {
		t.Fatalf("synthesized message = %#v", nm.Message)
	}

	// batch updates fan out one-per-update
	got = flattenUpdates(&tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateReadHistoryOutbox{MaxID: 1},
			&tg.UpdateReadHistoryOutbox{MaxID: 2},
		},
	})
	if len(got) != 2 {
		t.Fatalf("batch fan-out = %d", len(got))
	}
}
