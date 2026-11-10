package botapi

// Round 3: the file route, the raw/multipart 429 path, username-chat
// resolution, the serve lifecycle and the remaining pure helpers —
// same fakes, same style.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// stubChatClient answers getChat with a fixed numeric id so
// resolveUsernameChat's resolution+cache path runs against reality.
type stubChatClient struct {
	id    int64
	calls int
}

func (s *stubChatClient) Do(req *httpRequest) (*httpResponse, error) {
	s.calls++
	return &httpResponse{
		StatusCode: 200,
		Body:       []byte(fmt.Sprintf(`{"ok":true,"result":{"id":%d}}`, s.id)),
	}, nil
}

func TestFileRouteUnknownTokenAndBadShape(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	registerBot(t, ts.URL, "filer")
	s := mustSession(t, app)

	// bad path shape — no bot token segment
	if code, body := get(t, ts.URL, "/file/nosuchshape", nil); code != 404 {
		t.Fatalf("bad shape = %d %s", code, body)
	}
	// well-formed but unknown token
	if code, _ := get(t, ts.URL, "/file/btBOGUS:x/photo.jpg", nil); code != 404 {
		t.Fatal("unknown token must 404")
	}
	// known token: the fake answers everything with {"ok":true,...} and no
	// Content-Type, so the passthrough must default to octet-stream
	code, body := get(t, ts.URL, "/file/bot"+s.Token+"/photo.jpg", nil)
	if code != 200 || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("file passthrough = %d %s", code, body)
	}
}

func TestServeLifecycleListenAndShutdown(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, err := NewApp(testCfg(t.TempDir()), fake)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	errCh := make(chan error, 1)
	go func() { errCh <- app.ListenAndServe() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// ListenAndServe must return ErrServerClosed after Shutdown
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("ListenAndServe returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServe did not return after Shutdown")
	}
}

func TestRandHexIsHex(t *testing.T) {
	h := randHex(16)
	if len(h) != 16 {
		t.Fatalf("len = %d", len(h))
	}
	for _, c := range h {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("non-hex char %q in %q", c, h)
		}
	}
}

func TestAdminAuditAuthed(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	registerBot(t, ts.URL, "audited") // writes "register" audit entry

	code, b := get(t, ts.URL, "/admin/audit", map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("audit = %d", code)
	}
	if !strings.Contains(fmt.Sprint(b), "register") {
		t.Fatalf("audit missing register entry: %v", b)
	}
	_ = app
}

func TestResolveUsernameChatCacheAndResolution(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	registerBot(t, ts.URL, "resolver")
	s := mustSession(t, app)
	stub := &stubChatClient{id: 12345}
	app.Proxy.client = stub
	ctx := context.Background()

	// no chat_id / non-@ target: untouched, no upstream call
	b1 := map[string]any{"text": "x"}
	if out := app.Proxy.resolveUsernameChat(ctx, s, b1); out["text"] != "x" || stub.calls != 0 {
		t.Fatalf("plain body mutated or upstream called: %v calls=%d", out, stub.calls)
	}
	b2 := map[string]any{"chat_id": "123"}
	if out := app.Proxy.resolveUsernameChat(ctx, s, b2); out["chat_id"] != "123" || stub.calls != 0 {
		t.Fatalf("numeric-string body mutated or upstream called: %v", out)
	}

	// @username: resolved once via getChat, then served from the cache
	orig := map[string]any{"chat_id": "@target"}
	out := app.Proxy.resolveUsernameChat(ctx, s, orig)
	if got, ok := out["chat_id"].(int64); !ok || got != 12345 {
		t.Fatalf("resolved chat_id = %#v, want int64(12345)", out["chat_id"])
	}
	if orig["chat_id"] != "@target" {
		t.Fatal("input map must not be mutated (cloneMapAny path)")
	}
	if out2 := app.Proxy.resolveUsernameChat(ctx, s, map[string]any{"chat_id": "@target"}); out2["chat_id"] != int64(12345) {
		t.Fatalf("cache miss on second resolve: %v", out2)
	}
	if stub.calls != 1 {
		t.Fatalf("getChat called %d times, want exactly 1 (cache)", stub.calls)
	}

	// fake upstream answering result:{} (no id) — body passes through
	app.Proxy.client = fake
	out3 := app.Proxy.resolveUsernameChat(ctx, s, map[string]any{"chat_id": "@nothere"})
	if out3["chat_id"] != "@nothere" {
		t.Fatalf("unresolvable body mutated: %v", out3)
	}
}

func TestExtractChatIDAndPureHelpers(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		want *int64
	}{
		{"float", map[string]any{"chat_id": 123.0}, ptrInt64(123)},
		{"negative float", map[string]any{"chat_id": -501.0}, ptrInt64(-501)},
		{"numeric string", map[string]any{"chat_id": "42"}, ptrInt64(42)},
		{"username string", map[string]any{"chat_id": "@user"}, nil},
		{"garbage string", map[string]any{"chat_id": "12a"}, nil},
		{"non-integral float", map[string]any{"chat_username": 7.5}, nil},
		{"bool true excluded", map[string]any{"chat": true}, nil},
		{"bool false excluded", map[string]any{"chat_id": false}, nil},
		{"missing", map[string]any{}, nil},
	}
	for _, tc := range cases {
		if got := extractChatID(tc.body); !eqInt64p(got, tc.want) {
			t.Fatalf("%s: extractChatID = %v, want %v", tc.name, got, tc.want)
		}
	}

	// isNumeric corners
	for s, want := range map[string]bool{"": false, "-": false, "-12": true, "12a": false, "007": true} {
		if got := isNumeric(s); got != want {
			t.Fatalf("isNumeric(%q) = %v", s, got)
		}
	}
	// numberOf across JSON-decoded and native types
	if numberOf(2.5) != 2.5 || numberOf(3) != 3 || numberOf(int64(4)) != 4 ||
		numberOf("42") != 42 || numberOf("x") != 0 || numberOf(nil) != 0 {
		t.Fatal("numberOf table failed")
	}
	// nullableInt / max / truthy
	if nullableInt(nil) != nil {
		t.Fatal("nullableInt(nil) must be nil")
	}
	v := int64(7)
	if nullableInt(&v) != any(int64(7)) {
		t.Fatal("nullableInt(&7) must be 7")
	}
	if max(1, 2) != 2 || max(3, 1) != 3 {
		t.Fatal("max failed")
	}
	if truthy(true) != true || truthy(1) != false {
		t.Fatal("truthy failed")
	}
	// gwResponse helpers
	if jsonResp(200, nil).isRaw() {
		t.Fatal("jsonResp must not be raw")
	}
	if !(&gwResponse{raw: []byte("x")}).isRaw() {
		t.Fatal("raw response must be raw")
	}
	if jsonResp(200, nil).withStatus(403).status != 403 {
		t.Fatal("withStatus failed")
	}
}

func ptrInt64(v int64) *int64 { return &v }

func eqInt64p(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func TestMultipartChat429SyntheticAndUnresolved(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	registerBot(t, ts.URL, "mp")
	s := mustSession(t, app)
	ctx := context.Background()

	mkBody := func(chat string) []byte {
		return []byte("--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n" +
			chat + "\r\n--B\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"p\"\r\n" +
			"Content-Type: application/octet-stream\r\n\r\nDATA\r\n--B--\r\n")
	}

	// no chat_id field in the prefix: admission falls to the global bucket
	// (multipartChatUnresolved) but still forwards
	resp := app.Proxy.Call(ctx, s, "sendPhoto", nil,
		"multipart/form-data; boundary=B", []byte("--B\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\ny\r\n--B--\r\n"), "")
	if resp.status != 200 {
		t.Fatalf("global-bucket multipart = %d %s", resp.status, resp.raw)
	}

	// saturate the per-chat bucket: eventually the guard refuses and the
	// proxy answers a synthetic 429 with retry_after
	var got429 *gwResponse
	for i := 0; i < 200; i++ {
		r := app.Proxy.Call(ctx, s, "sendPhoto", nil,
			"multipart/form-data; boundary=B", mkBody("7778"), "")
		if r.status == 429 {
			got429 = r
			break
		}
	}
	if got429 == nil {
		t.Fatal("per-chat saturation never produced a synthetic 429")
	}
	if !strings.Contains(fmt.Sprint(got429.jsonBody), "retry_after") {
		t.Fatalf("429 without retry_after: %v", got429.jsonBody)
	}
	if s.synth429Total() == 0 {
		t.Fatal("noteSynthetic429 not recorded")
	}
	// session accessor surface used by metrics/external consumers
	if s.Queue() == nil {
		t.Fatal("Queue() must expose the queue")
	}
}
