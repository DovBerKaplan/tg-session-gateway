package botapi

// Coverage for the admin HTTP surface and misc routes the lifecycle
// tests don't drive: every handler's happy path, its 401 and its 404
// where one exists.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminRoutesCoverage(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	adm := map[string]string{"Authorization": "Bearer adm-secret"}
	reg := func(alias, token string) {
		code, b := post(t, base, "/admin/bots",
			fmt.Sprintf(`{"token":%q,"alias":%q}`, token, alias), adm)
		if code != 200 {
			t.Fatalf("register %s: %d %v", alias, code, b)
		}
		time.Sleep(100 * time.Millisecond) // let the poller settle
	}
	reg("cov1", "710000:cov1")
	reg("cov2", "720000:cov2")

	t.Run("401 without admin secret", func(t *testing.T) {
		for _, path := range []string{
			"/admin/bots/cov1/pause", "/admin/bots/cov1/resume",
			"/admin/bots/cov1/drain", "/admin/bots/cov1/push",
			"/admin/bots/cov1/paid", "/admin/bots/cov1/typing",
			"/admin/settings/typing", "/admin/safe_mode/ack",
		} {
			code, _ := post(t, base, path, `{}`, nil)
			if code != 401 {
				t.Errorf("POST %s without auth = %d, want 401", path, code)
			}
		}
		if code, _ := get(t, base, "/admin/settings/typing", nil); code != 401 {
			t.Errorf("GET /admin/settings/typing without auth = %d", code)
		}
	})

	t.Run("pause resume drain lifecycle", func(t *testing.T) {
		code, b := post(t, base, "/admin/bots/cov1/pause", `{}`, adm)
		if code != 200 || b["ok"] != true {
			t.Fatalf("pause: %d %v", code, b)
		}
		code, b = post(t, base, "/admin/bots/cov1/resume", `{}`, adm)
		if code != 200 || b["ok"] != true {
			t.Fatalf("resume: %d %v", code, b)
		}
		code, b = post(t, base, "/admin/bots/cov1/drain", `{}`, adm)
		if code != 200 || b["ok"] != true {
			t.Fatalf("drain: %d %v", code, b)
		}
		// drain resumes on register again
		reg("cov1", "710000:cov1")
	})

	t.Run("unknown alias contract", func(t *testing.T) {
		// pause/resume/drain/delete are IDEMPOTENT: unknown alias =
		// 200 ok:false (state-changing, safe to retry blindly).
		// setPush/setPaid/setTyping LOOK UP the alias: 404.
		for _, path := range []string{
			"/admin/bots/nope/pause", "/admin/bots/nope/resume", "/admin/bots/nope/drain",
		} {
			code, b := post(t, base, path, `{}`, adm)
			if code != 200 || b["ok"] != false {
				t.Errorf("POST %s unknown alias = %d %v, want 200 ok:false", path, code, b)
			}
		}
		for _, path := range []string{
			"/admin/bots/nope/paid", "/admin/bots/nope/typing",
		} {
			if code, _ := post(t, base, path, `{}`, adm); code != 404 {
				t.Errorf("POST %s unknown alias = %d, want 404", path, code)
			}
		}
	})

	t.Run("set push url and clear", func(t *testing.T) {
		code, b := post(t, base, "/admin/bots/cov1/push", `{"url":"http://127.0.0.1:1/hook"}`, adm)
		if code != 200 || b["ok"] != true {
			t.Fatalf("set push: %d %v", code, b)
		}
		code, b = post(t, base, "/admin/bots/cov1/push", `{}`, adm)
		if code != 200 || b["push_url"] != nil {
			t.Fatalf("clear push: %d %v", code, b)
		}
		if code, _ := post(t, base, "/admin/bots/nope/push", `{}`, adm); code != 404 {
			t.Fatalf("push unknown alias = %d", code)
		}
	})

	t.Run("paid broadcast opt-in", func(t *testing.T) {
		code, b := post(t, base, "/admin/bots/cov1/paid", `{"enabled":true}`, adm)
		if code != 200 || b["paid_broadcasts"] != true {
			t.Fatalf("paid on: %d %v", code, b)
		}
		code, b = post(t, base, "/admin/bots/cov1/paid", `{"enabled":false}`, adm)
		if code != 200 || b["paid_broadcasts"] != false {
			t.Fatalf("paid off: %d %v", code, b)
		}
	})

	t.Run("typing overrides per bot and global", func(t *testing.T) {
		code, b := post(t, base, "/admin/bots/cov1/typing",
			`{"enabled":false,"interval_s":9}`, adm)
		if code != 200 || b["ok"] != true {
			t.Fatalf("bot typing patch: %d %v", code, b)
		}
		typ := b["typing"].(map[string]any)
		if typ["enabled"] != false || typ["interval_s"] != 9.0 || typ["overridden"] == nil {
			t.Fatalf("bot typing view: %v", typ)
		}
		if code, b := get(t, base, "/admin/settings/typing", adm); code != 200 {
			t.Fatalf("global typing get: %d %s", code, b)
		}
		code, b = post(t, base, "/admin/settings/typing", `{"timeout_s":30}`, adm)
		if code != 200 || b["ok"] != true {
			t.Fatalf("global typing patch: %d %v", code, b)
		}
		if code, _ := post(t, base, "/admin/bots/nope/typing", `{}`, adm); code != 404 {
			t.Fatalf("typing unknown alias = %d", code)
		}
		if code, _ := post(t, base, "/admin/bots/cov1/typing", `{bad json`, adm); code != 400 {
			t.Fatalf("bad patch = %d", code)
		}
	})

	t.Run("safe mode ack", func(t *testing.T) {
		code, b := post(t, base, "/admin/safe_mode/ack", `{}`, adm)
		if code != 200 || b["safe_mode"] != false {
			t.Fatalf("safe mode ack: %d %v", code, b)
		}
	})

	t.Run("delete unknown alias", func(t *testing.T) {
		req, _ := httpNewRequest("DELETE", base+"/admin/bots/nope", nil)
		req.Header.Set("Authorization", "Bearer adm-secret")
		resp, err := httpClientDefault.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		// delete is idempotent: unknown alias = 200 ok:false
		if resp.StatusCode != 200 {
			t.Fatalf("delete unknown = %d", resp.StatusCode)
		}
	})

	t.Run("tgapi GET route with query params", func(t *testing.T) {
		code, b := post(t, base, "/admin/bots", `{"token":"730000:cov3","alias":"cov3"}`, adm)
		if code != 200 {
			t.Fatalf("register cov3: %d %v", code, b)
		}
		time.Sleep(100 * time.Millisecond)
		code, body := get(t, base, "/tgapi/bot730000:cov3/getMe", nil)
		if code != 401 && code != 200 {
			t.Fatalf("GET route = %d %s", code, body)
		}
		// GET /tgapi/ requires app auth; a 401 proves the route ran
		code, body = get(t, base, "/tgapi/bot730000:cov3/getChat?chat_id=5", nil)
		if code == 404 {
			t.Fatalf("GET tgapi route not registered: %d %s", code, body)
		}
		_ = strings.TrimSpace // silence potential unused import churn
	})
}

func TestParseFormBody(t *testing.T) {
	// gateway-owned urlencoded bodies parse to string values with NO
	// coercion ("007" stays "007") — the data-integrity contract
	got := parseFormBody([]byte("chat_id=007&action=typ&empty="))
	if got["chat_id"] != "007" || got["action"] != "typ" {
		t.Fatalf("parseFormBody = %v (values must stay strings, no coercion)", got)
	}
	if len(parseFormBody(nil)) != 0 {
		t.Fatal("nil body must yield empty map")
	}
}

func TestPprofGatedAndAuthed(t *testing.T) {
	fake := newFakeTelegram(nil)
	// default: OFF — no route at all
	_, ts := newTestApp(t, fake, t.TempDir())
	resp, err := http.Get(ts.URL + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("pprof must be OFF by default, got %d", resp.StatusCode)
	}

	// ON: mounted on the admin mux behind the admin secret
	cfg := testCfg(t.TempDir())
	cfg.Pprof = true
	app2, err := NewApp(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	app2.adminRoutes(mux)
	ts2 := httptest.NewServer(mux)
	defer ts2.Close()
	app2.Close()

	r, _ := http.NewRequest("GET", ts2.URL+"/debug/pprof/", nil)
	resp, err = http.DefaultClient.Do(r) // no auth
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("pprof without admin secret = %d, want 401", resp.StatusCode)
	}
	r.Header.Set("Authorization", "Bearer adm-secret")
	resp, err = http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("pprof with admin secret = %d, want 200", resp.StatusCode)
	}
}
