package botapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
)

// fakeTelegram stands in for api.telegram.org: deleteWebhook OK,
// getUpdates yields one batch then empties, sendMessage records.
type fakeTelegram struct {
	mu                sync.Mutex
	batch             []map[string]any
	fed               bool
	getCalls          int
	sent              []map[string]any
	sendChat          []map[string]any
	rawSends          [][]byte // verbatim bodies (streamed or buffered)
	lastContentLength int64
	quota429          map[string]int // chat_id → send 429 with retry_after N times
}

func newFakeTelegram(batch []map[string]any) *fakeTelegram {
	return &fakeTelegram{batch: batch, quota429: map[string]int{}}
}

func (f *fakeTelegram) Do(req *httpRequest) (*httpResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.Reader != nil {
		f.lastContentLength = req.ContentLength
		raw, _ := io.ReadAll(req.Reader)
		f.rawSends = append(f.rawSends, raw)
	} else if req.RawBody != nil {
		f.rawSends = append(f.rawSends, req.RawBody)
	}
	if req.RawBody == nil && req.Body != nil {
		raw, _ := json.Marshal(req.Body)
		req.RawBody = raw
	}
	switch {
	case strings.HasSuffix(req.URL, "/deleteWebhook"):
		return &httpResponse{StatusCode: 200, Body: []byte(`{"ok":true,"result":true}`)}, nil
	case strings.HasSuffix(req.URL, "/getUpdates"):
		f.getCalls++
		if !f.fed {
			f.fed = true
			raw, _ := json.Marshal(map[string]any{"ok": true, "result": f.batch})
			return &httpResponse{StatusCode: 200, Body: raw}, nil
		}
		time.Sleep(10 * time.Millisecond) // keep the empty-poll loop from hot-spinning
		return &httpResponse{StatusCode: 200, Body: []byte(`{"ok":true,"result":[]}`)}, nil
	case strings.HasSuffix(req.URL, "/sendMessage"):
		var body map[string]any
		_ = json.Unmarshal(req.RawBody, &body)
		if n := f.quota429[fmt.Sprint(body["chat_id"])]; n > 0 {
			f.quota429[fmt.Sprint(body["chat_id"])] = n - 1
			return &httpResponse{StatusCode: 429, Body: []byte(
				`{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7","parameters":{"retry_after":7}}`)}, nil
		}
		f.sent = append(f.sent, body)
		return &httpResponse{StatusCode: 200, Body: []byte(`{"ok":true,"result":{"message_id":42}}`)}, nil
	case strings.HasSuffix(req.URL, "/sendChatAction"):
		var body map[string]any
		_ = json.Unmarshal(req.RawBody, &body)
		f.sendChat = append(f.sendChat, body)
		return &httpResponse{StatusCode: 200, Body: []byte(`{"ok":true,"result":true}`)}, nil
	}
	return &httpResponse{StatusCode: 200, Body: []byte(`{"ok":true,"result":{}}`)}, nil
}

func testCfg(dataDir string) config.BotAPI {
	return config.BotAPI{
		Host: "127.0.0.1", Port: 0, Upstream: "http://fake.local",
		AdminSecret: "adm-secret", AppSecret: "app-secret",
		DataDir: dataDir, FernetKey: "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=",
		PollTimeout: 0, PollBackoffMax: 0.1,
		Rate:    config.DefaultRate(),
		Policy:  config.PolicyConfig{OnLimit: "queue", QueueWaitTimeout: 25, UpdateQueueMax: 5000, UpdateTTLS: 3600},
		Typing:  config.TypingConfig{Enabled: false, PrivateOnly: true, IntervalS: 0.05, TimeoutS: 0.2, TimeoutText: "timeout!"},
		Storage: config.StorageConfig{Durability: "normal", MaintenanceInterval: 3600, BackupInterval: 86400, BackupKeep: 5, AuditRetentionDays: 30},
	}
}

func newTestApp(t *testing.T, fake *fakeTelegram, dataDir string) (*App, *httptest.Server) {
	t.Helper()
	cfg := testCfg(dataDir)
	app, err := NewApp(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(appMux(app))
	t.Cleanup(func() {
		ts.Close()
		app.Close()
	})
	return app, ts
}

const token = "700000:itest-token"

func post(t *testing.T, url, path, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	req, _ := httpNewRequest("POST", url+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw := make([]byte, 1<<20)
	n, _ := resp.Body.Read(raw)
	var parsed map[string]any
	_ = json.Unmarshal(raw[:n], &parsed)
	return resp.StatusCode, parsed
}

func get(t *testing.T, url, path string, headers map[string]string) (int, string) {
	t.Helper()
	req, _ := httpNewRequest("GET", url+path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func TestGatewayLifecycle(t *testing.T) {
	batch := []map[string]any{
		{"update_id": float64(1001), "message": map[string]any{"text": "first", "chat": map[string]any{"id": float64(10)}}},
		{"update_id": float64(1002), "message": map[string]any{"text": "second", "chat": map[string]any{"id": float64(10)}}},
	}
	fake := newFakeTelegram(batch)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	// unknown token → 404 before any Telegram call
	code, _ := post(t, base, "/tgapi/bot999:unknown/getUpdates", `{}`, nil)
	if code != 404 {
		t.Fatalf("unknown token = %d, want 404", code)
	}

	// register
	code, body := post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"b1"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 || body["alias"] != "b1" {
		t.Fatalf("register = %d %v", code, body)
	}

	// poller picks the batch up asynchronously
	deadline := time.Now().Add(5 * time.Second)
	var updates []map[string]any
	for time.Now().Before(deadline) && len(updates) < 2 {
		_, b := post(t, base, "/tgapi/bot"+token+"/getUpdates", `{"timeout":0}`, nil)
		if res, ok := b["result"].([]any); ok {
			for _, u := range res {
				updates = append(updates, u.(map[string]any))
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	if len(updates) != 2 {
		t.Fatalf("poller never fed the queue: %v", updates)
	}

	// metrics expose the session and the queue
	_, metricsBody := get(t, base, "/metrics", nil)
	for _, needle := range []string{
		`tg_gateway_session_state{alias="b1"} 1`,
		"tg_gateway_update_queue_depth",
		"tg_gateway_real_429_total",
	} {
		if !strings.Contains(metricsBody, needle) {
			t.Fatalf("metrics missing %q:\n%s", needle, metricsBody)
		}
	}

	// send through the alias route → proxied verbatim
	code, body = post(t, base, "/bots/b1/sendMessage", `{"chat_id":10,"text":"hi"}`,
		map[string]string{"Authorization": "Bearer app-secret"})
	if code != 200 {
		t.Fatalf("alias send = %d %v", code, body)
	}
	if result, ok := body["result"].(map[string]any); !ok || result["message_id"] != float64(42) {
		t.Fatalf("send result = %v", body)
	}
	if len(fake.sent) != 1 || fake.sent[0]["text"] != "hi" {
		t.Fatalf("upstream send = %+v", fake.sent)
	}

	// alias route requires the app secret
	code, _ = post(t, base, "/bots/b1/sendMessage", `{"chat_id":10,"text":"x"}`, nil)
	if code != 401 {
		t.Fatalf("unauthed alias send = %d, want 401", code)
	}

	// pause: poller stops calling upstream
	_, body = post(t, base, "/admin/bots/b1/pause", ``, map[string]string{"Authorization": "Bearer adm-secret"})
	if body["ok"] != true {
		t.Fatalf("pause = %v", body)
	}
	_, statusBody := get(t, base, "/admin/status", map[string]string{"Authorization": "Bearer adm-secret"})
	if !strings.Contains(statusBody, `"state":"paused"`) {
		t.Fatalf("status after pause: %s", statusBody)
	}
	fake.mu.Lock()
	callsAtPause := fake.getCalls
	fake.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	fake.mu.Lock()
	callsAfter := fake.getCalls
	fake.mu.Unlock()
	if callsAfter != callsAtPause {
		t.Fatal("paused poller must not call upstream")
	}
	post(t, base, "/admin/bots/b1/resume", ``, map[string]string{"Authorization": "Bearer adm-secret"})

	// delete: gone from status, token no longer routes
	post(t, base, "/admin/bots/b1", ``, map[string]string{})
	code, _ = post(t, base, "/tgapi/bot"+token+"/getUpdates", `{}`, nil)
	_ = code
	req, _ := httpNewRequest("DELETE", base+"/admin/bots/b1", nil)
	req.Header.Set("Authorization", "Bearer adm-secret")
	resp, _ := httpClientDefault.Do(req)
	var delBody map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&delBody)
	resp.Body.Close()
	if delBody["ok"] != true {
		t.Fatalf("delete = %v", delBody)
	}
	code, _ = post(t, base, "/tgapi/bot"+token+"/getUpdates", `{}`, nil)
	if code != 404 {
		t.Fatalf("deleted token must 404, got %d", code)
	}
}

func TestRestartRestoresSessionAndUnackedUpdates(t *testing.T) {
	dataDir := t.TempDir()
	batch := []map[string]any{
		{"update_id": float64(1001), "message": map[string]any{"text": "first"}},
		{"update_id": float64(1002), "message": map[string]any{"text": "second"}},
	}

	// first lifetime: register, poller feeds the queue, never consume
	fake := newFakeTelegram(batch)
	app, ts := newTestApp(t, fake, dataDir)
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"keep"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	waitFor(t, 5*time.Second, "queue to fill", func() bool {
		_, sb := get(t, base, "/admin/status", map[string]string{"Authorization": "Bearer adm-secret"})
		return strings.Contains(sb, `"depth":2`)
	})
	// gateway dies WITHOUT the consumer acking
	ts.Close()
	app.Close()

	// second lifetime: same data dir
	fake2 := newFakeTelegram(nil)
	app2, ts2 := newTestApp(t, fake2, dataDir)
	_ = app2
	base2 := ts2.URL
	// session restored from the encrypted store — no re-register
	_, sb := get(t, base2, "/admin/status", map[string]string{"Authorization": "Bearer adm-secret"})
	if !strings.Contains(sb, `"alias":"keep"`) {
		t.Fatalf("session not restored: %s", sb)
	}
	// unacked updates survived the restart in queue.db
	var pulled []map[string]any
	waitFor(t, 5*time.Second, "unacked replay", func() bool {
		_, b := post(t, base2, "/tgapi/bot"+token+"/getUpdates", `{"timeout":1}`, nil)
		if res, ok := b["result"].([]any); ok {
			for _, u := range res {
				pulled = append(pulled, u.(map[string]any))
			}
		}
		return len(pulled) >= 2
	})
	texts := []string{}
	for _, u := range pulled {
		if m, ok := u["message"].(map[string]any); ok {
			texts = append(texts, m["text"].(string))
		}
	}
	if len(texts) < 2 || texts[0] != "first" || texts[1] != "second" {
		t.Fatalf("queue not restored in order: %v", texts)
	}
}

// TestTwoConcurrentPullersExactlyOnce is the rolling-deploy invariant:
// two replicas pulling concurrently — the union is every update exactly
// once, each replica's own stream duplicate-free.
func TestTwoConcurrentPullersExactlyOnce(t *testing.T) {
	batch := make([]map[string]any, 20)
	for i := range batch {
		batch[i] = map[string]any{"update_id": float64(2000 + i), "message": map[string]any{"text": fmt.Sprintf("m%d", i)}}
	}
	fake := newFakeTelegram(batch)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"r1"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	waitFor(t, 5*time.Second, "batch queued", func() bool {
		_, sb := get(t, base, "/admin/status", map[string]string{"Authorization": "Bearer adm-secret"})
		return strings.Contains(sb, `"depth":20`)
	})

	puller := func(out *[]float64) {
		idle := 0
		var offset *float64
		for idle < 3 {
			body := `{"timeout":0,"limit":5`
			if offset != nil {
				body += fmt.Sprintf(`,"offset":%d`, int64(*offset)+1)
			}
			body += "}"
			_, b := post(t, base, "/tgapi/bot"+token+"/getUpdates", body, nil)
			got := []float64{}
			if res, ok := b["result"].([]any); ok {
				for _, u := range res {
					um := u.(map[string]any)
					got = append(got, um["update_id"].(float64))
				}
			}
			*out = append(*out, got...)
			if len(got) > 0 {
				max := got[0]
				for _, g := range got {
					if g > max {
						max = g
					}
				}
				offset = &max
				idle = 0
			} else {
				idle++
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var a, b []float64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); puller(&a) }()
	go func() { defer wg.Done(); puller(&b) }()
	wg.Wait()

	// Invariant 1 — no update lost across the overlap
	got := map[float64]bool{}
	for _, id := range append(append([]float64{}, a...), b...) {
		got[id] = true
	}
	for _, u := range batch {
		if !got[u["update_id"].(float64)] {
			t.Fatalf("lost update %v", u["update_id"])
		}
	}
	// Invariant 2 — each replica's own stream is duplicate-free
	for name, ids := range map[string][]float64{"A": a, "B": b} {
		seen := map[float64]bool{}
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("replica %s saw update %v twice", name, id)
			}
			seen[id] = true
		}
	}

	// audit recorded the registration
	_, ab := get(t, base, "/admin/audit", map[string]string{"Authorization": "Bearer adm-secret"})
	if !strings.Contains(ab, `"action":"register"`) || !strings.Contains(ab, `"alias":"r1"`) {
		t.Fatalf("audit = %s", ab)
	}
}

func TestIdempotencyKeySuppressesDuplicateSends(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"i1"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	headers := map[string]string{"Authorization": "Bearer app-secret", "Idempotency-Key": "send-once"}
	code, b1 := post(t, base, "/bots/i1/sendMessage", `{"chat_id":10,"text":"once"}`, headers)
	if code != 200 || b1["ok"] != true {
		t.Fatalf("first send = %d %v", code, b1)
	}
	_, b2 := post(t, base, "/bots/i1/sendMessage", `{"chat_id":10,"text":"once"}`, headers)
	if b2["idempotent_replay"] != true {
		t.Fatalf("replay = %v", b2)
	}
	if len(fake.sent) != 1 {
		t.Fatalf("duplicate send leaked upstream: %+v", fake.sent)
	}
}

func TestReal429LearningAndBackoffLedger(t *testing.T) {
	fake := newFakeTelegram(nil)
	fake.mu.Lock()
	fake.quota429["10"] = 1
	fake.mu.Unlock()
	dir := t.TempDir()
	_, ts := newTestApp(t, fake, dir)
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"f1"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	h := map[string]string{"Authorization": "Bearer app-secret"}
	_, b := post(t, base, "/bots/f1/sendMessage", `{"chat_id":10,"text":"will 429"}`, h)
	if b["error_code"] != float64(429) {
		t.Fatalf("expected upstream 429 passthrough, got %v", b)
	}
	if params, ok := b["parameters"].(map[string]any); !ok || params["retry_after"] != float64(7) {
		t.Fatalf("429 parameters = %v", b)
	}
	_, metricsBody := get(t, base, "/metrics", nil)
	if !strings.Contains(metricsBody, `tg_gateway_real_429_total{alias="f1"} 1`) {
		t.Fatalf("real 429 not counted:\n%s", metricsBody)
	}
}

func TestTypingLoopBeatsAndTimeout(t *testing.T) {
	batch := []map[string]any{
		{"update_id": float64(1), "message": map[string]any{
			"text": "hi", "chat": map[string]any{"id": float64(10)}}},
	}
	fake := newFakeTelegram(batch)
	cfg := testCfg(t.TempDir())
	cfg.Typing.Enabled = true
	cfg.Typing.IntervalS = 0.03
	cfg.Typing.TimeoutS = 0.1
	cfg.Typing.TimeoutText = "sorry-down"
	app, err := NewApp(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	_ = app
	ts := httptest.NewServer(appMux(app))
	defer ts.Close()
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"t1"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})

	waitFor(t, 3*time.Second, "typing beat upstream", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return len(fake.sendChat) >= 1
	})
	// timeout notice lands in the private chat
	waitFor(t, 3*time.Second, "timeout notice", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		for _, s := range fake.sent {
			if s["text"] == "sorry-down" {
				return true
			}
		}
		return false
	})
}

func TestSafeModeBlocksRestoreUntilAcked(t *testing.T) {
	dir := t.TempDir()
	// plant 6 recent boots → the next boot is safe mode
	boots := make([]float64, 6)
	now := float64(time.Now().UnixNano()) / 1e9
	for i := range boots {
		boots[i] = now - 1
	}
	raw, _ := json.Marshal(boots)
	if err := writeFile(filepath.Join(dir, "boot_history.json"), raw); err != nil {
		t.Fatal(err)
	}
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, dir)
	base := ts.URL
	// register happens normally in safe mode...
	code, _ := post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"sm"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register in safe mode = %d", code)
	}
	// ...but readyz reports safe mode
	rc, rb := get(t, base, "/readyz", nil)
	if rc != 503 || !strings.Contains(rb, "safe_mode") {
		t.Fatalf("readyz in safe mode = %d %s", rc, rb)
	}
	// ack clears it
	code, _ = post(t, base, "/admin/safe_mode/ack", ``, map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatal("ack failed")
	}
	rc, rb = get(t, base, "/readyz", nil)
	if rc != 200 || !strings.Contains(rb, `"ok":true`) {
		t.Fatalf("readyz after ack = %d %s", rc, rb)
	}
}

func TestGatewayOwnedMethods(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"gw"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	h := map[string]string{"Authorization": "Bearer app-secret"}
	// deleteWebhook: no-op success (aiogram calls it on boot)
	_, b := post(t, base, "/bots/gw/deleteWebhook", `{}`, h)
	if b["ok"] != true {
		t.Fatalf("deleteWebhook = %v", b)
	}
	// setWebhook: gateway owns it — 403-shaped JSON
	code, b := post(t, base, "/bots/gw/setWebhook", `{"url":"x"}`, h)
	if code != 403 || b["error_code"] != float64(403) {
		t.Fatalf("setWebhook = %d %v", code, b)
	}
}

func TestFormURLEncodedBody(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"fm"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	req, _ := httpNewRequest("POST", base+"/bots/fm/sendMessage",
		strings.NewReader("chat_id=10&text=form+sent&disable_notification=true"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer app-secret")
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.sent) != 1 {
		t.Fatal("form send never reached upstream")
	}
	if fake.sent[0]["text"] != "form sent" {
		t.Fatalf("form decode = %v", fake.sent[0])
	}
	if fake.sent[0]["chat_id"] != int64(10) && fake.sent[0]["chat_id"] != float64(10) {
		t.Fatalf("chat_id coercion = %v (%T)", fake.sent[0]["chat_id"], fake.sent[0]["chat_id"])
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestAdminPortSplit pins GW_ADMIN_PORT: the public listener loses the
// operator surface, the admin listener gains it (the Helm chart's
// 8080/8081 layout).
func TestAdminPortSplit(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(dir)
	cfg.AdminPort = 18101
	app, err := NewApp(cfg, newFakeTelegram(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Close)

	pub := httptest.NewServer(app.srv.Handler)
	adm := httptest.NewServer(app.adminSrv.Handler)
	t.Cleanup(pub.Close)
	t.Cleanup(adm.Close)

	// public: consumer surface + probes only
	if code := httpStatus(t, pub.URL+"/healthz"); code != 200 {
		t.Fatalf("public /healthz = %d", code)
	}
	if code := httpStatus(t, pub.URL+"/metrics"); code != 404 {
		t.Fatalf("public /metrics = %d, want 404", code)
	}
	if code := httpStatusAuth(t, pub.URL+"/admin/status", "adm-secret"); code != 404 {
		t.Fatalf("public /admin/status = %d, want 404", code)
	}

	// admin: operator surface + metrics
	if code := httpStatusAuth(t, adm.URL+"/admin/status", "adm-secret"); code != 200 {
		t.Fatalf("admin /admin/status = %d, want 200", code)
	}
	if code := httpStatus(t, adm.URL+"/metrics"); code != 200 {
		t.Fatalf("admin /metrics = %d, want 200", code)
	}
	if code := httpStatus(t, adm.URL+"/tgapi/bot123:x/getUpdates"); code != 404 {
		t.Fatalf("admin /tgapi = %d, want 404 (proxy must not live here)", code)
	}
}

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := httpClientDefault.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func httpStatusAuth(t *testing.T, url, secret string) int {
	t.Helper()
	req, _ := httpNewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}
