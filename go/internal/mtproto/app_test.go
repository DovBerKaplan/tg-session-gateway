package mtproto

// Integration: the whole MTProto sidecar in one process — real App,
// real store, real singleton lock files, real rate guard; only the
// Telegram client is swapped for a fake that records what would have
// gone over the wire. Port of tests/test_integration_mtgateway.py.
//
// Covers the seven-point bar: register → lock BEFORE connect →
// updates → send → idempotency → pause_sending → synthetic 429 scoped
// per chat → real FLOOD_WAIT parsing → second sidecar refused before
// any Telegram contact.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/cryptoutil"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/store"
)

const (
	itestToken = "710000:mt-itest"
	itestADM   = "Bearer mt-adm"
	itestAPP   = "Bearer mt-app"
)

// fakeMTClient stands in for the gotd client; records every touch.
type fakeMTClient struct {
	mu           sync.Mutex
	connectCalls int
	connected    bool
	sent         []sentRec
	floodOnceCh  int64 // >0: first send to this chat raises FloodWait(42)
	flooded      bool
}

type sentRec struct {
	chatID int64
	text   string
}

func (f *fakeMTClient) Connect(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectCalls++
	f.connected = true
	return nil
}

func (f *fakeMTClient) Disconnect(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected = false
	return nil
}

func (f *fakeMTClient) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func (f *fakeMTClient) Ping(ctx context.Context) error { return nil }

func (f *fakeMTClient) Me(ctx context.Context) (BotInfo, error) {
	return BotInfo{Username: "fake_bot", ID: 42}, nil
}

func (f *fakeMTClient) WarmPeers(ctx context.Context) error { return nil }

func (f *fakeMTClient) SendMessage(ctx context.Context, p SendParams) (SentMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.floodOnceCh != 0 && f.floodOnceCh == p.ChatID && !f.flooded {
		f.flooded = true
		return SentMessage{}, fmt.Errorf("Telegram flood: FloodWait(42)")
	}
	f.sent = append(f.sent, sentRec{p.ChatID, p.Text})
	return SentMessage{ID: int64(len(f.sent)), ChatID: p.ChatID}, nil
}

func (f *fakeMTClient) EditMessageText(ctx context.Context, p EditTextParams) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) EditMessageReplyMarkup(ctx context.Context, p EditMarkupParams) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) SendChatAction(ctx context.Context, chatID int64, action string) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) DeleteMessages(ctx context.Context, chatID int64, ids []int64, revoke bool) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) ForwardMessages(ctx context.Context, p ForwardParams) ([]SentMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []SentMessage{}
	for _, id := range p.IDs {
		f.sent = append(f.sent, sentRec{p.ToChatID, fmt.Sprintf("fwd:%d", id)})
		out = append(out, SentMessage{ID: id, ChatID: p.ToChatID})
	}
	return out, nil
}

func (f *fakeMTClient) GetChat(ctx context.Context, chatID int64) (map[string]any, error) {
	return dict("_", "Chat", "id", chatID, "type", "private"), nil
}

func (f *fakeMTClient) ResolveUsername(ctx context.Context, username string) (int64, error) {
	return 77, nil
}

func (f *fakeMTClient) AnswerCallbackQuery(ctx context.Context, id, text string, showAlert bool, url string, cacheTime int) (EditResult, error) {
	return EditResult{Ok: true}, nil
}

func (f *fakeMTClient) DownloadMedia(ctx context.Context, fileID string) ([]byte, error) {
	return nil, fmt.Errorf("media not found")
}

func (f *fakeMTClient) UploadMedia(ctx context.Context, kind, filename string, data []byte) (string, error) {
	return "", fmt.Errorf("upload not wired in fake")
}

func (f *fakeMTClient) startCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connectCalls
}

func (f *fakeMTClient) sentTexts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.sent))
	for i, r := range f.sent {
		out[i] = r.text
	}
	return out
}

// ── helpers ────────────────────────────────────────────────────────

func itestConfig(dir string) config.MTProto {
	return itestConfigWithKey(dir, mustTestKey())
}

func itestConfigWithKey(dir, key string) config.MTProto {
	return config.MTProto{
		Host: "127.0.0.1", Port: 0,
		AdminSecret: "mt-adm",
		AppSecret:   "mt-app",
		DataDir:     dir,
		FernetKey:   key,
		APIID:       12345,
		APIHash:     strings.Repeat("deadbeef", 8),
		SessionDir:  filepath.Join(dir, "sessions"),
		BackupsDir:  filepath.Join(dir, "backups"),
		Rate: config.RateConfig{
			PrivateRate: 1, PrivateBurst: 3,
			GroupRate: 20.0 / 60.0, GroupBurst: 2,
			GlobalRate: 30, GlobalBurst: 30,
			ReadRate: 20, PaidRate: 1000,
		},
		MTRate: config.MTProtoRate{
			FloodBackoffBase: 1.5, SlowmodeRespect: true,
			PeerFloodCooldown: 300, SilentRate: 5,
		},
		Policy: config.PolicyConfig{
			OnLimit: "reject", QueueWaitTimeout: 0.5,
			UpdateQueueMax: 5000, UpdateTTLS: 3600,
		},
		Storage:        config.StorageConfig{Durability: "normal", MaintenanceInterval: 0},
		MetricsEnabled: true,
	}
}

func mustTestKey() string {
	k, err := cryptoutil.GenerateKey()
	if err != nil {
		panic(err)
	}
	return k
}

func newTestApp(t *testing.T, fake *fakeMTClient, dir string) (*App, *httptest.Server) {
	t.Helper()
	app, err := NewApp(itestConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	app.Mgr.ClientFactory = func(alias string) Client { return fake }
	ts := httptest.NewServer(app.srv.Handler)
	t.Cleanup(func() {
		ts.Close()
		app.Close()
	})
	return app, ts
}

func newTestAppKey(t *testing.T, fake *fakeMTClient, dir, key string) (*App, *httptest.Server) {
	t.Helper()
	app, err := NewApp(itestConfigWithKey(dir, key))
	if err != nil {
		t.Fatal(err)
	}
	app.Mgr.ClientFactory = func(alias string) Client { return fake }
	ts := httptest.NewServer(app.srv.Handler)
	t.Cleanup(func() {
		ts.Close()
		app.Close()
	})
	return app, ts
}

func mtDo(t *testing.T, method, base, path, body, auth string) (int, map[string]any) {
	t.Helper()
	raw := mtRaw(t, method, base+path, body, auth)
	out := map[string]any{}
	if len(raw.body) > 0 {
		if err := json.Unmarshal([]byte(raw.body), &out); err != nil {
			t.Fatalf("non-JSON response (%d): %s", raw.code, raw.body)
		}
	}
	return raw.code, out
}

type rawResp struct {
	code int
	body string
}

func mtRaw(t *testing.T, method, url, body, auth string) rawResp {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return rawResp{resp.StatusCode, string(data)}
}

func waitUntil(t *testing.T, what string, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func regADM(t *testing.T, base, alias string) {
	t.Helper()
	code, body := mtDo(t, "POST", base, "/v1/admin/sessions",
		fmt.Sprintf(`{"alias":%q,"token":%q}`, alias, itestToken), itestADM)
	if code != 200 {
		t.Fatalf("register failed: %d %v", code, body)
	}
}

func waitLive(t *testing.T, app *App, fake *fakeMTClient, alias string) {
	t.Helper()
	waitUntil(t, "client start", 5*time.Second, func() bool {
		return fake.startCalls() >= 1 && app.Mgr.Get(alias) != nil &&
			app.Mgr.Get(alias).CurrentState() == StateLive
	})
}

// ── the lifecycle bar ──────────────────────────────────────────────

func TestRegisterLockSendUpdatesIdempotency(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d1"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")
	if fake.startCalls() != 1 { // started ONCE — the sidecar owns it
		t.Fatalf("start calls = %d", fake.startCalls())
	}

	// the singleton lock exists on disk
	if _, err := os.Stat(filepath.Join(app.Cfg.SessionDir, "b1.lock")); err != nil {
		t.Fatalf("lock file missing: %v", err)
	}

	// app secret required
	code, _ := mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"chat_id":10,"text":"x"}`, "")
	if code != 401 {
		t.Fatalf("unauthenticated send = %d, want 401", code)
	}

	// send → forwarded to the (fake) client verbatim
	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"chat_id":10,"text":"hi"}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("send = %d %v", code, body)
	}
	if got := fake.sentTexts(); len(got) != 1 || got[0] != "hi" {
		t.Fatalf("sent = %v", got)
	}

	// token route answers the same contract
	code, body = mtDo(t, "POST", base, "/v1/token/bot"+itestToken+"/send_message",
		`{"chat_id":10,"text":"tok"}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("token-route send = %d %v", code, body)
	}

	// unknown token → 404
	code, _ = mtDo(t, "POST", base, "/v1/token/bot999:x/send_message",
		`{"chat_id":10,"text":"x"}`, itestAPP)
	if code != 404 {
		t.Fatalf("unknown token = %d, want 404", code)
	}

	// updates from Telegram → served to the app via pull
	app.Mgr.Get("b1").pushUpdate(map[string]any{
		"update_id": 1, "message": map[string]any{"text": "hello"},
	})
	code, body = mtDo(t, "POST", base, "/v1/sessions/b1/getUpdates",
		`{"timeout":0,"limit":10}`, itestAPP)
	if code != 200 {
		t.Fatalf("getUpdates = %d", code)
	}
	if !resultHasText(body, "hello") {
		t.Fatalf("hello not pulled: %v", body)
	}

	// idempotency: same key twice → one send
	code, _ = mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"_idempotency_key":"k1","chat_id":10,"text":"once"}`, itestAPP)
	code, body = mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"_idempotency_key":"k1","chat_id":10,"text":"once"}`, itestAPP)
	if body["idempotent_replay"] != true {
		t.Fatalf("replay not flagged: %v", body)
	}
	if got := fake.sentTexts(); len(got) != 3 { // hi, tok, once — exactly one "once"
		t.Fatalf("sends after idempotency = %v", got)
	}

	// admin audit recorded the registration
	_, body = mtDo(t, "GET", base, "/v1/admin/audit", "", itestADM)
	entries := body["entries"].([]any)
	if len(entries) == 0 {
		t.Fatal("audit empty")
	}
	last := entries[len(entries)-1].(map[string]any)
	// entries newest-first or oldest-first — accept the register anywhere
	// but require the LAST action to exist; register is the first action.
	if entries[0].(map[string]any)["action"] != "register" {
		t.Fatalf("first audit entry = %v, want register", entries[0])
	}
	_ = last
}

func resultHasText(body map[string]any, text string) bool {
	results, _ := body["result"].([]any)
	for _, r := range results {
		if m, ok := r.(map[string]any); ok {
			if msg, ok := m["message"].(map[string]any); ok {
				if msg["text"] == text {
					return true
				}
			}
		}
	}
	return false
}

func TestSecondSidecarRefusedBeforeTelegram(t *testing.T) {
	dir := t.TempDir()
	sessDir := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// a LIVE holder: our own pid, fresh heartbeat → lock is not stale
	lockPath := filepath.Join(sessDir, "b1.lock")
	now := float64(time.Now().UnixNano()) / 1e9
	if err := os.WriteFile(lockPath, []byte(fmt.Sprintf("%d\n%f\n", os.Getpid(), now)), 0o644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, dir)
	base := ts.URL

	code, _ := mtDo(t, "POST", base, "/v1/admin/sessions",
		`{"alias":"b1","token":"`+itestToken+`"}`, itestADM)
	if code != 200 {
		t.Fatalf("register = %d (accepted for start)", code)
	}

	// ...but the client NEVER reached Telegram
	waitUntil(t, "lock refusal", 3*time.Second, func() bool {
		s := app.Mgr.Get("b1")
		return s != nil && s.CurrentState() == StateError
	})
	if fake.startCalls() != 0 {
		t.Fatalf("Telegram was contacted despite the lock: %d", fake.startCalls())
	}

	// refused sessions STAY registered and visible (error state) —
	// sends answer 503, never 404
	code, _ = mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"chat_id":10,"text":"x"}`, itestAPP)
	if code != 503 {
		t.Fatalf("send while refused = %d, want 503", code)
	}
	_, body := mtDo(t, "GET", base, "/v1/admin/status", "", itestADM)
	sessions := body["sessions"].([]any)
	if sessions[0].(map[string]any)["state"] != "error" {
		t.Fatalf("state = %v, want error", sessions[0])
	}

	// holder gone → explicit restart (clears the cooldown) recovers
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	code, body = mtDo(t, "POST", base, "/v1/admin/sessions/b1/restart", "", itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("restart = %d %v", code, body)
	}
	waitUntil(t, "retry start", 5*time.Second, func() bool { return fake.startCalls() == 1 })
}

func TestPauseSendingBlocksWritesAllowsUpdates(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d3"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	_, body := mtDo(t, "POST", base, "/v1/admin/sessions/b1/pause_sending", "", itestADM)
	if body["ok"] != true {
		t.Fatalf("pause_sending = %v", body)
	}

	code, _ := mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"chat_id":10,"text":"x"}`, itestAPP)
	if code != 423 { // locked
		t.Fatalf("send while paused = %d, want 423", code)
	}
	if len(fake.sentTexts()) != 0 { // nothing left the building
		t.Fatalf("paused send leaked: %v", fake.sentTexts())
	}

	// updates keep flowing while sends are paused (E4)
	app.Mgr.Get("b1").pushUpdate(map[string]any{
		"update_id": 2, "message": map[string]any{"text": "still listening"},
	})
	_, body = mtDo(t, "POST", base, "/v1/sessions/b1/getUpdates", `{"timeout":0}`, itestAPP)
	if !resultHasText(body, "still listening") {
		t.Fatalf("updates muted by pause_sending: %v", body)
	}
}

func TestSynthetic429ScopedPerChat(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d4"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	codes := []int{}
	for i := 0; i < 6; i++ { // private burst is 3 — the tail must 429
		code, _ := mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
			fmt.Sprintf(`{"chat_id":10,"text":"spam%d"}`, i), itestAPP)
		codes = append(codes, code)
	}
	saw429 := false
	for _, c := range codes {
		if c == 429 {
			saw429 = true
		}
	}
	if !saw429 {
		t.Fatalf("no synthetic 429: %v", codes)
	}

	// the hot chat is throttled, a different chat is NOT muted
	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"chat_id":20,"text":"other chat"}`, itestAPP)
	if code != 200 {
		t.Fatalf("per-chat scoping broken: %d %v", code, body)
	}
}

func TestRealFloodWaitParsedAndReported(t *testing.T) {
	fake := &fakeMTClient{floodOnceCh: 10}
	app, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d5"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_message",
		`{"chat_id":10,"text":"will flood"}`, itestAPP)
	if code != 429 {
		t.Fatalf("flood send = %d %v", code, body)
	}
	if !strings.Contains(fmt.Sprint(body["description"]), "FLOOD_WAIT") {
		t.Fatalf("description = %v", body["description"])
	}
	params := body["parameters"].(map[string]any)
	if params["retry_after"] != float64(42) {
		t.Fatalf("retry_after = %v, want 42", params["retry_after"])
	}

	// stats surface the REAL flood wait (not synthetic)
	_, body = mtDo(t, "GET", base, "/v1/admin/status", "", itestADM)
	s := body["sessions"].([]any)[0].(map[string]any)
	if s["real_flood_wait"] != float64(1) {
		t.Fatalf("real_flood_wait = %v", s["real_flood_wait"])
	}
	if s["synthetic_429"] != float64(0) {
		t.Fatalf("synthetic_429 = %v", s["synthetic_429"])
	}
}

// TestUnsupportedMethod501 pins the Go contract change: methods outside
// the adapter allowlist answer 501 with the supported list (the Python
// sidecar getattr'd ANY Pyrogram method — never promised here).
func TestUnsupportedMethod501(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d6"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/scroll_to_seam_carpet",
		`{"chat_id":10}`, itestAPP)
	if code != 501 {
		t.Fatalf("unsupported method = %d, want 501", code)
	}
	if !strings.Contains(fmt.Sprint(body["description"]), "unsupported method") {
		t.Fatalf("description = %v", body["description"])
	}
	if !strings.Contains(fmt.Sprint(body["description"]), "send_message") {
		t.Fatalf("description lacks the allowlist: %v", body["description"])
	}
}

// TestGetUpdatesOffsetMode pins Bot API offset semantics: offset = the
// FIRST update wanted; delivery does NOT ack.
func TestGetUpdatesOffsetMode(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d7"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	s := app.Mgr.Get("b1")
	for i := 1; i <= 3; i++ {
		s.pushUpdate(map[string]any{
			"update_id": i, "message": map[string]any{"text": fmt.Sprintf("m%d", i)},
		})
	}

	// offset=2 → everything below acked; returns seq 2 and 3
	_, body := mtDo(t, "POST", base, "/v1/sessions/b1/getUpdates",
		`{"timeout":0,"offset":2}`, itestAPP)
	if n := len(body["result"].([]any)); n != 2 {
		t.Fatalf("offset=2 returned %d rows, want 2: %v", n, body["result"])
	}

	// unacked rows replay until the consumer advances the offset
	_, body = mtDo(t, "POST", base, "/v1/sessions/b1/getUpdates",
		`{"timeout":0,"offset":2}`, itestAPP)
	if n := len(body["result"].([]any)); n != 2 {
		t.Fatalf("replay lost rows: %d", n)
	}

	// offset=4 acks everything → empty
	_, body = mtDo(t, "POST", base, "/v1/sessions/b1/getUpdates",
		`{"timeout":0,"offset":4}`, itestAPP)
	if n := len(body["result"].([]any)); n != 0 {
		t.Fatalf("offset=4 returned %d rows, want 0", n)
	}
	if depth := app.Store.QueueDepth("b1"); depth != 0 {
		t.Fatalf("queue depth after full ack = %d", depth)
	}
}

// TestMetricsExposure pins the metric names monitoring/alerts.yml reads.
func TestMetricsExposure(t *testing.T) {
	fake := &fakeMTClient{}
	app2, ts := newTestApp(t, fake, filepath.Join(t.TempDir(), "d8"))
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app2, fake, "b1")

	w := mtRaw(t, "GET", base+"/metrics", "", "")
	text := w.body
	for _, want := range []string{
		`tg_gateway_session_state{alias="b1"} 1`,
		"tg_gateway_update_queue_depth",
		"tg_gateway_real_flood_wait_total",
		"tg_gateway_synthetic_429_total",
		"tg_gateway_lock_reclaims_total",
		"tg_gateway_loop_lag_seconds",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, text)
		}
	}
}

// TestSafeModeReadyz: a crashed-looping sidecar refuses readiness and
// auto-restore until the admin acks, then restores.
func TestSafeModeReadyz(t *testing.T) {
	dir := t.TempDir()
	// a session already registered from the previous (crashing) run,
	// encrypted with the SAME key the app will load
	key := mustTestKey()
	seed, err := store.OpenMT(dir, key, "normal", filepath.Join(dir, "backups"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Register("sm", itestToken); err != nil {
		t.Fatal(err)
	}
	seed.Close()

	// crash-loop signature: many recent boots
	boots := make([]float64, 7)
	now := float64(time.Now().UnixNano()) / 1e9
	for i := range boots {
		boots[i] = now - 1
	}
	raw, _ := json.Marshal(boots)
	if err := os.WriteFile(filepath.Join(dir, "boot_history.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeMTClient{}
	_, ts := newTestAppKey(t, fake, dir, key)
	base := ts.URL

	// safe mode: the stored session is NOT auto-connected
	time.Sleep(300 * time.Millisecond)
	if fake.startCalls() != 0 {
		t.Fatalf("safe mode contacted Telegram: %d", fake.startCalls())
	}

	// readyz reports safe mode
	w := mtRaw(t, "GET", base+"/readyz", "", "")
	if w.code != 503 || !strings.Contains(w.body, "safe_mode") {
		t.Fatalf("readyz in safe mode = %d %s", w.code, w.body)
	}

	// ack leaves safe mode and restores
	code, body := mtDo(t, "POST", base, "/v1/admin/safe-mode/ack", "", itestADM)
	if code != 200 || body["safe_mode"] != false || body["restored"] != float64(1) {
		t.Fatalf("ack = %d %v", code, body)
	}
	waitUntil(t, "restore after ack", 5*time.Second, func() bool { return fake.startCalls() == 1 })
}
