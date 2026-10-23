package rateguard

import (
	"testing"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
)

func testRate() config.RateConfig {
	return config.RateConfig{
		PrivateRate: 1, PrivateBurst: 3,
		GroupRate: 20.0 / 60.0, GroupBurst: 2,
		GlobalRate: 30, GlobalBurst: 30,
		ReadRate: 20, PaidRate: 1000,
	}
}

func TestMethodKind(t *testing.T) {
	cases := map[string]string{
		"SendMessage":         "write",
		"answerCallbackQuery": "fast",
		"getMe":               "read",
		"getChat":             "read",
		"GETMYCOMMANDS":       "read",
		"sendPhoto":           "write",
		"answerInlineQuery":   "fast",
	}
	for m, want := range cases {
		if got := MethodKind(m); got != want {
			t.Errorf("MethodKind(%q) = %q, want %q", m, got, want)
		}
	}
}

func TestPrivateBurstThenThrottle(t *testing.T) {
	g := NewGuard(testRate())
	chat := int64(10)
	for i := 0; i < 3; i++ { // burst 3 admitted
		if !g.TryAcquire("sendmessage", &chat) {
			t.Fatalf("send %d should be admitted within burst", i)
		}
	}
	if g.TryAcquire("sendmessage", &chat) {
		t.Fatal("4th send in a burst must throttle (burst=3)")
	}
	other := int64(20)
	if !g.TryAcquire("sendmessage", &other) {
		t.Fatal("per-chat scoping broken: other chat must not be muted")
	}
}

func TestReal429PausesChatNotOthers(t *testing.T) {
	g := NewGuard(testRate())
	chat := int64(10)
	g.Report429(&chat, 5)
	if g.TryAcquire("sendmessage", &chat) {
		t.Fatal("paused chat must not admit")
	}
	other := int64(20)
	if !g.TryAcquire("sendmessage", &other) {
		t.Fatal("429 on one chat must not pause other chats")
	}
}

func TestEgressPauseFromBotWide429(t *testing.T) {
	g := NewGuard(testRate())
	g.Report429(nil, 3)
	chat := int64(10)
	if g.TryAcquire("sendmessage", &chat) {
		t.Fatal("bot-wide egress pause must block writes")
	}
	if g.ChatSendState(&chat) != "paused" {
		t.Fatalf("ChatSendState = %q, want paused", g.ChatSendState(&chat))
	}
}

func TestFastPathNeverThrottled(t *testing.T) {
	g := NewGuard(testRate())
	chat := int64(10)
	g.Report429(&chat, 60)
	if !g.TryAcquire("answercallbackquery", &chat) {
		t.Fatal("fast path must bypass chat pause")
	}
}

func TestChatSendStateDrained(t *testing.T) {
	g := NewGuard(testRate())
	chat := int64(10)
	for i := 0; i < 3; i++ {
		g.TryAcquire("sendmessage", &chat)
	}
	if s := g.ChatSendState(&chat); s != "empty" {
		t.Fatalf("ChatSendState = %q, want empty", s)
	}
}

func TestParseFloodWait(t *testing.T) {
	cases := []struct {
		in   string
		typ  string
		wait int
	}{
		{"Telegram flood: FloodWait(42)", "flood_wait", 42},
		{"FloodPremiumWait(10)", "flood_premium", 10},
		{"SlowmodeWait(5)", "slowmode", 5},
		{"PeerFloodInvalid", "peer_flood", 300},
		{"[420 FLOOD_WAIT_X] - A wait of 42 seconds is required", "flood_wait", 42},
		{"[420 SLOWMODE_WAIT_X] - A wait of 7 seconds is required", "slowmode", 7},
	}
	for _, c := range cases {
		info := ParseFloodWait(c.in)
		if info == nil {
			t.Fatalf("ParseFloodWait(%q) = nil", c.in)
		}
		if info.Type != c.typ || info.Wait != c.wait {
			t.Errorf("ParseFloodWait(%q) = %+v, want type=%s wait=%d", c.in, info, c.typ, c.wait)
		}
	}
	if ParseFloodWait("some unrelated error") != nil {
		t.Error("unrelated error must parse to nil")
	}
}

func TestMTAdaptiveBackoff(t *testing.T) {
	g := NewMTGuard(testRate(), config.MTProtoRate{FloodBackoffBase: 1.5, PeerFloodCooldown: 300, SilentRate: 5})
	chat := int64(10)

	g.ReportFloodWait("FloodWait(10)", &chat) // offence 1 → 10s
	if w := g.AdaptiveWait(&FloodInfo{Type: "flood_wait", Wait: 10}, &chat); w != 10 {
		t.Fatalf("first offence adaptive wait = %d, want 10", w)
	}
	info2 := g.ReportFloodWait("FloodWait(10)", &chat) // offence 2 → 15s
	if info2 == nil {
		t.Fatal("second flood must parse")
	}
	if w := g.AdaptiveWait(info2, &chat); w != 15 {
		t.Fatalf("second offence adaptive wait = %d, want 15", w)
	}
	if g.TryAcquire("send_message", &chat) {
		t.Fatal("flooded peer must be paused")
	}
	g.ClearPeerBackoff(chat)
	// chat bucket itself is paused by ReportFloodWait; after clearing the
	// peer backoff the silent admission still sees the bucket pause —
	// verifying separation of the two layers is the point of this check.
	other := int64(20)
	if !g.TryAcquire("send_message", &other) {
		t.Fatal("peer flood scoping broken: other chat muted")
	}
}

func TestSilentMethods(t *testing.T) {
	g := NewMTGuard(testRate(), config.MTProtoRate{SilentRate: 5})
	if !g.TryAcquire("resolve_username", nil) {
		t.Fatal("silent method should use silent bucket")
	}
}

func TestJitteredNeverBelowBase(t *testing.T) {
	for i := 0; i < 100; i++ {
		if got := Jittered(2.0, 60.0); got < 2.0 {
			t.Fatalf("Jittered below base: %v", got)
		}
	}
}
