package botapi

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSynthetic429Shape(t *testing.T) {
	r := synthetic429(7, "")
	if r.status != 429 {
		t.Fatalf("status=%d", r.status)
	}
	body, _ := r.jsonBody.(map[string]any)
	if body["ok"] != false || body["error_code"] != 429 {
		t.Fatalf("body=%v", body)
	}
	params, _ := body["parameters"].(map[string]any)
	if params["retry_after"] != 7 {
		t.Fatalf("retry_after=%v", params["retry_after"])
	}
	if r.header["Retry-After"] != "7" {
		t.Fatalf("Retry-After header=%v", r.header)
	}
	desc, _ := body["description"].(string)
	if !strings.Contains(desc, "retry after 7") {
		t.Fatalf("description=%q", desc)
	}
}

func TestRewriteGetFileAbsolutePath(t *testing.T) {
	p := &Proxy{pathPrefix: "/tgapi", localKind: true}
	in := []byte(`{"ok":true,"result":{"file_id":"X","file_path":"/var/lib/telegram-bot-api/files/doc.bin"}}`)
	out := p.rewriteGetFileBody("123:tok", in)
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatal(err)
	}
	fp := parsed["result"].(map[string]any)["file_path"].(string)
	want := "tgapi/file/bot123:tok/var/lib/telegram-bot-api/files/doc.bin"
	if fp != want {
		t.Fatalf("file_path=%q want %q", fp, want)
	}
}

func TestRewriteGetFileRelativeUnchanged(t *testing.T) {
	p := &Proxy{pathPrefix: "/tgapi", localKind: true}
	in := []byte(`{"ok":true,"result":{"file_path":"documents/file_1"}}`)
	out := p.rewriteGetFileBody("123:tok", in)
	if string(out) != string(in) {
		t.Fatalf("relative path should stay unchanged: %s", out)
	}
}

func TestGetWebhookInfoGatewayOwned(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	code, body := post(t, base, "/admin/bots", `{"token":"710000:wh","alias":"wh"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register=%d %v", code, body)
	}
	code, body = post(t, base, "/tgapi/bot710000:wh/getWebhookInfo", `{}`, nil)
	if code != 200 || body["ok"] != true {
		t.Fatalf("getWebhookInfo=%d %v", code, body)
	}
	res, _ := body["result"].(map[string]any)
	if res["url"] != "" {
		t.Fatalf("expected empty webhook url, got %v", res)
	}
	code, body = post(t, base, "/tgapi/bot710000:wh/setWebhook", `{"url":"https://x"}`, nil)
	if code != 403 {
		t.Fatalf("setWebhook=%d want 403 (%v)", code, body)
	}
}

func TestBareBotPathAndTestDC(t *testing.T) {
	fake := newFakeTelegram(nil)
	cfg := testCfg(t.TempDir())
	cfg.PathPrefix = "none"
	app, err := NewApp(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(appMux(app))
	t.Cleanup(func() { ts.Close(); app.Close() })

	code, body := post(t, ts.URL, "/admin/bots", `{"token":"720000:bare","alias":"bare"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register=%d %v", code, body)
	}
	code, body = post(t, ts.URL, "/bot720000:bare/getMe", `{}`, nil)
	if code != 200 || body["ok"] != true {
		t.Fatalf("bare getMe=%d %v", code, body)
	}
	code, body = post(t, ts.URL, "/bot720000:bare/test/getMe", `{}`, nil)
	if code != 200 || body["ok"] != true {
		t.Fatalf("test DC getMe=%d %v", code, body)
	}
}

func TestExtractMultipartChatUsername(t *testing.T) {
	prefix := []byte("--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n@mychannel\r\n")
	id, user := extractMultipartChat(prefix)
	if id != nil || user != "@mychannel" {
		t.Fatalf("id=%v user=%q", id, user)
	}
	prefix2 := []byte("--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n-100123\r\n")
	id, user = extractMultipartChat(prefix2)
	if user != "" || id == nil || *id != -100123 {
		t.Fatalf("id=%v user=%q", id, user)
	}
}

func TestRedactToken(t *testing.T) {
	got := RedactToken("123456:AAHEsecretTOKEN99")
	if strings.Contains(got, "secret") || !strings.Contains(got, "***") {
		t.Fatalf("redact=%q", got)
	}
}

func TestLearnFloodFromDescription(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, _ := newTestApp(t, fake, t.TempDir())
	s, err := app.Mgr.Attach(t.Context(), "fl", "730000:fl", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	resp := &httpResponse{
		StatusCode: 400,
		Body:       []byte(`{"ok":false,"error_code":400,"description":"FloodWait(12)"}`),
	}
	app.Proxy.learnFlood(s, nil, "sendMessage", resp)
	if s.real429Total() < 1 {
		t.Fatal("expected real 429 learning from FloodWait description")
	}
}

func TestFileRouteServesLocalDiskPath(t *testing.T) {
	fake := newFakeTelegram(nil)
	cfg := testCfg(t.TempDir())
	cfg.UpstreamKind = "local"
	app, err := NewApp(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(appMux(app))
	t.Cleanup(func() { ts.Close(); app.Close() })
	auth := map[string]string{"Authorization": "Bearer adm-secret"}

	code, body := post(t, ts.URL, "/admin/bots", `{"token":"740000:disk","alias":"disk"}`, auth)
	if code != 200 {
		t.Fatalf("register=%d %v", code, body)
	}

	// absolute path on the shared volume: served straight from disk
	tmp := filepath.Join(t.TempDir(), "doc.bin")
	if err := os.WriteFile(tmp, []byte("DISKBYTES"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, raw := get(t, ts.URL, "/tgapi/file/bot740000:disk"+tmp, nil)
	if code != 200 || raw != "DISKBYTES" {
		t.Fatalf("disk file=%d %q", code, raw)
	}

	// volume-shaped path without the leading slash: recognized as absolute,
	// stat fails on this host, falls through to the upstream fetch
	code, _ = get(t, ts.URL, "/tgapi/file/bot740000:disk/var/lib/telegram-bot-api/files/none.bin", nil)
	if code != 200 {
		t.Fatalf("abs-shaped fallthrough=%d want upstream status", code)
	}
}
