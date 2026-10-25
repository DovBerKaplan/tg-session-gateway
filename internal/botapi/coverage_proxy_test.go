package botapi

// Round 2: upstream client, raw/verbatim forwarding, push plumbing
// and the typing helpers — direct unit tests against the existing
// fakes and small table cases.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func registerBot(t *testing.T, base, alias string) {
	t.Helper()
	code, b := post(t, base, "/admin/bots",
		`{"token":"740000:`+alias+`","alias":"`+alias+`"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register %s: %d %v", alias, code, b)
	}
	time.Sleep(200 * time.Millisecond)
}

func TestUpstreamClientDoAnd502Path(t *testing.T) {
	// NewUpstreamClient against a live fake: Do builds the Bot API URL
	// and returns the parsed response; a DEAD upstream surfaces as an
	// error the proxy maps to 502.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer srv.Close()
	c := NewUpstreamClient(srv.URL)
	resp, err := c.Do(&httpRequest{Method: "POST", URL: "/botTOK/sendMessage", Body: map[string]any{"x": 1}})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != 200 || !strings.Contains(string(resp.Body), "message_id") {
		t.Fatalf("resp = %d %s", resp.StatusCode, resp.Body)
	}

	dead := NewUpstreamClient("http://127.0.0.1:1") // nothing listens
	if _, err := dead.Do(&httpRequest{Method: "POST", URL: "/botTOK/sendMessage", Body: map[string]any{}}); err == nil {
		t.Fatal("dead upstream must error (proxy maps to 502)")
	}
}

func TestForwardRawVerbatim(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	// one recording server plays BOTH roles: Telegram upstream (poller
	// polls it) and verbatim-forward target. getUpdates → empty result.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		mu.Lock()
		bodies = append(bodies, string(buf[:n]))
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
	}))
	defer srv.Close()
	up := NewUpstreamClient(srv.URL)
	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	app.Mgr.client = up   // poller rides this
	app.Proxy.client = up // sends ride this
	registerBot(t, ts.URL, "raw")

	body := "chat_id=7&text=verbatim"
	resp := app.Proxy.CallRawWithChat(context.Background(), mustSession(t, app), "sendMessage",
		[]byte(body), "application/x-www-form-urlencoded", nil, "")
	if resp.status != 200 {
		t.Fatalf("CallRawWithChat = %d %s", resp.status, resp.raw)
	}
	// find OUR form body among the poller's JSON bodies: it must arrive
	// VERBATIM (urlencoded, uncoerced)
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		var found string
		for _, b := range bodies {
			if strings.HasPrefix(b, "chat_id=7&") {
				found = b
			}
		}
		mu.Unlock()
		if found != "" {
			if found != body {
				t.Fatalf("body forwarded %q, want verbatim %q", found, body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("verbatim body never arrived; got %v", bodies)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTypingHelpers(t *testing.T) {
	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	registerBot(t, ts.URL, "typ")
	chat := int64(9)
	id := "cb-1"

	if got := toStr(42.0); got != "42" {
		t.Fatalf("toStr float = %q", got)
	}
	if got := toStr("x"); got != "x" {
		t.Fatalf("toStr str = %q", got)
	}
	if got := itoa64(-123); got != "-123" {
		t.Fatalf("itoa64 = %q", got)
	}

	// StopByCallbackID with an unknown id is a no-op
	app.Mgr.Typing().StopByCallbackID(mustSession(t, app), id)
	// CancelTimeout on an inactive chat is false
	if app.Mgr.Typing().CancelTimeout(mustSession(t, app), &chat) {
		t.Fatal("CancelTimeout on inactive chat must be false")
	}
	// cloneMapAny deep-copies maps
	src := map[string]any{"a": 1}
	cp := cloneMapAny(src)
	cp["a"] = 2
	if src["a"] != 1 {
		t.Fatal("cloneMapAny must not alias")
	}
}

func TestPushToConsumerPost(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		mu.Lock()
		bodies = append(bodies, string(buf[:n]))
		mu.Unlock()
	}))
	defer srv.Close()

	fake := newFakeTelegram(nil)
	app, ts := newTestApp(t, fake, t.TempDir())
	app.Mgr.client = NewUpstreamClient(srv.URL) // push rides the real client
	registerBot(t, ts.URL, "push")
	s := mustSession(t, app)
	s.pushURL = srv.URL + "/hook"
	upd := map[string]any{"update_id": 1, "message": map[string]any{"text": "x"}}
	app.Mgr.pushToConsumer(srv.URL+"/hook", upd)

	// the poller also floods this server; look for OUR update body
	var pushed string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, b := range bodies {
			if strings.Contains(b, `"update_id":1`) {
				pushed = b
			}
		}
		mu.Unlock()
		if pushed != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pushed == "" {
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("update never pushed; got %v", bodies)
	}
}

func TestSleepJitteredContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: must return immediately (false)
	if sleepJittered(ctx, 5) {
		t.Fatal("cancelled context must not sleep")
	}
}

func mustSession(t *testing.T, app *App) *BotSession {
	t.Helper()
	for _, s := range app.Mgr.Sessions() {
		return s
	}
	t.Fatal("no session registered")
	return nil
}
