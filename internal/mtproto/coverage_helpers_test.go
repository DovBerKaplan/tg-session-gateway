package mtproto

// Round 2: pure helpers and message-extraction logic from the gotd
// client and app — table-driven, no Telegram machinery.

import (
	"errors"
	"net/http"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

func TestWalkMessagesShapes(t *testing.T) {
	msg := func(id int) *tg.Message { return &tg.Message{ID: id} }

	full := &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateNewMessage{Message: msg(7)},
		&tg.UpdateNewChannelMessage{Message: msg(9)},
	}}
	var seen []int
	walkMessages(full, func(m *tg.Message) bool {
		seen = append(seen, m.ID)
		return false
	})
	if len(seen) != 2 || seen[0] != 7 || seen[1] != 9 {
		t.Fatalf("walked %v", seen)
	}

	short := &tg.UpdateShort{Update: &tg.UpdateNewMessage{Message: msg(3)}}
	seen = nil
	walkMessages(short, func(m *tg.Message) bool {
		seen = append(seen, m.ID)
		return true // stop after first
	})
	if len(seen) != 1 || seen[0] != 3 {
		t.Fatalf("short walked %v", seen)
	}

	seen = nil
	walkMessages(&tg.Updates{}, func(m *tg.Message) bool { seen = append(seen, m.ID); return false })
	if len(seen) != 0 {
		t.Fatal("empty updates must walk nothing")
	}

	// non-Message update classes are skipped, not a crash
	skip := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateUserTyping{}}}
	walkMessages(skip, func(m *tg.Message) bool { t.Fatal("must not be called"); return true })

	// extractSentMessage takes the FIRST message with the caller's chat
	sent := extractSentMessage(full, 42)
	if sent.ID != 7 || sent.ChatID != 42 {
		t.Fatalf("extractSentMessage = %+v", sent)
	}
	sent = extractSentMessage(&tg.Updates{}, 42)
	if sent.ID != 0 {
		t.Fatalf("no message must be zero: %+v", sent)
	}
}

func TestInputPeerOf(t *testing.T) {
	if p, ok := inputPeerOf(5).(*tg.InputPeerUser); !ok || p.UserID != 5 {
		t.Fatalf("positive id: %#v", inputPeerOf(5))
	}
	// -100 without the channel magnitude is a plain chat
	if p, ok := inputPeerOf(-100).(*tg.InputPeerChat); !ok || p.ChatID != 100 {
		t.Fatalf("small negative id: %#v", inputPeerOf(-100))
	}
	// Bot API channel form: -100xxxxxxxxxx
	if p, ok := inputPeerOf(-1000000000001).(*tg.InputPeerChannel); !ok || p.ChannelID != 1 {
		t.Fatalf("channel id: %#v", inputPeerOf(-1000000000001))
	}
}

func TestIntIDsAndRandomID(t *testing.T) {
	if got := intIDs([]int64{1, 2, 3}); len(got) != 3 || got[2] != 3 {
		t.Fatalf("intIDs = %v", got)
	}
	a, b := randomID(), randomID()
	if a == b {
		t.Fatal("randomID must differ across calls")
	}
	if a <= 0 {
		t.Fatal("randomID must be positive (Telegram ids are)")
	}
}

func TestAppPureHelpers(t *testing.T) {
	if !isTooLarge(&http.MaxBytesError{}) {
		t.Fatal("MaxBytesError must classify as too-large")
	}
	if isTooLarge(errors.New("nope")) {
		t.Fatal("plain errors must not classify as too-large")
	}
	if mustJSON(map[string]any{"ok": true})[0] != '{' {
		t.Fatal("mustJSON must marshal")
	}
	s := randSuffix(8)
	if len(s) != 8 {
		t.Fatalf("randSuffix(8) = %q", s)
	}
}

func TestBotTokenUnregistered(t *testing.T) {
	// an unregistered alias has no stored token: empty, not a crash
	dir := t.TempDir()
	st, err := store.OpenMT(dir, "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=", "normal", dir+"/backups", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	g := &gotdClient{cfg: itestConfig(dir), alias: "ghost", storeDB: st}
	if tok := g.botToken(); tok != "" {
		t.Fatalf("unregistered alias token = %q", tok)
	}
}
