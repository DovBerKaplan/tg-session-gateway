package mtproto

import (
	"fmt"
	"strings"
	"testing"
)

// registerTestClient issues a client token through the admin API and
// returns (id, bearer).
func registerTestClient(t *testing.T, base, name, allowlist, sessions string) (string, string) {
	t.Helper()
	code, body := mtDo(t, "POST", base, "/v1/admin/clients", fmt.Sprintf(
		`{"name":%q,"allowlist":%s,"sessions":%s}`, name, allowlist, sessions), itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("client register=%d %v", code, body)
	}
	id, _ := body["id"].(string)
	token, _ := body["token"].(string)
	if id == "" || !strings.HasPrefix(token, clientTokenPrefix) {
		t.Fatalf("bad issue response: %v", body)
	}
	return id, "Bearer " + token
}

func TestClientTokenAllowlistAndScope(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "acc1")
	regADM(t, base, "acc2")
	waitLive(t, app, fake, "acc1")
	waitLive(t, app, fake, "acc2")

	_, bearer := registerTestClient(t, base, "scanner",
		`["get_me","send_message"]`, `["acc1"]`)

	// allowed method, allowed session
	code, body := mtDo(t, "POST", base, "/v1/sessions/acc1/get_me", `{}`, bearer)
	if code != 200 || body["ok"] != true {
		t.Fatalf("get_me=%d %v", code, body)
	}

	// method outside the allowlist
	code, body = mtDo(t, "POST", base, "/v1/sessions/acc1/get_chat", `{"chat_id":1}`, bearer)
	if code != 403 {
		t.Fatalf("disallowed method=%d %v (want 403)", code, body)
	}

	// session outside the scope
	code, body = mtDo(t, "POST", base, "/v1/sessions/acc2/get_me", `{}`, bearer)
	if code != 403 {
		t.Fatalf("out-of-scope session=%d %v (want 403)", code, body)
	}

	// updates are not in the allowlist
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/getUpdates", `{}`, bearer)
	if code != 403 {
		t.Fatalf("updates outside allowlist=%d (want 403)", code)
	}

	// the legacy app secret keeps working alongside
	code, body = mtDo(t, "POST", base, "/v1/sessions/acc2/get_me", `{}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("legacy secret=%d %v", code, body)
	}
}

func TestClientTokenRevocationAndBadToken(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "acc1")
	waitLive(t, app, fake, "acc1")

	id, bearer := registerTestClient(t, base, "tmp", `["full"]`, `["acc1"]`)

	// unknown bearer → 401
	code, _ := mtDo(t, "POST", base, "/v1/sessions/acc1/get_me", `{}`, "Bearer tggc_deadbeef")
	if code != 401 {
		t.Fatalf("unknown token=%d (want 401)", code)
	}

	// revoke → the same bearer stops working immediately
	code, body := mtDo(t, "DELETE", base, "/v1/admin/clients/"+id, ``, itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("revoke=%d %v", code, body)
	}
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/get_me", `{}`, bearer)
	if code != 401 {
		t.Fatalf("revoked token=%d (want 401)", code)
	}
}

func TestClientTokenReadonlyGroup(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "acc1")
	waitLive(t, app, fake, "acc1")

	_, bearer := registerTestClient(t, base, "shadow", `["readonly"]`, `["acc1"]`)

	code, body := mtDo(t, "POST", base, "/v1/sessions/acc1/get_me", `{}`, bearer)
	if code != 200 || body["ok"] != true {
		t.Fatalf("readonly get_me=%d %v", code, body)
	}
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/send_message",
		`{"chat_id":1,"text":"no"}`, bearer)
	if code != 403 {
		t.Fatalf("readonly send=%d (want 403)", code)
	}
	// readonly includes get_updates
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/getUpdates", `{"timeout":0}`, bearer)
	if code == 403 || code == 401 {
		t.Fatalf("readonly updates=%d (must not be authz-blocked)", code)
	}
}

func TestLegacyAppSecretCanBeDisabled(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	// flip the switch after wiring (newTestApp built it with legacy on)
	app.Cfg.LegacyAppSecret = false
	regADM(t, base, "acc1")
	waitLive(t, app, fake, "acc1")

	code, _ := mtDo(t, "POST", base, "/v1/sessions/acc1/get_me", `{}`, itestAPP)
	if code != 401 {
		t.Fatalf("legacy secret after disable=%d (want 401)", code)
	}
	// client tokens are unaffected
	_, bearer := registerTestClient(t, base, "only", `["get_me"]`, `["acc1"]`)
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/get_me", `{}`, bearer)
	if code != 200 {
		t.Fatalf("client token after legacy off=%d (want 200)", code)
	}
}

func TestAdminClientValidation(t *testing.T) {
	fake := &fakeMTClient{}
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	// name required
	code, _ := mtDo(t, "POST", base, "/v1/admin/clients", `{"allowlist":["full"],"sessions":["a"]}`, itestADM)
	if code != 400 {
		t.Fatalf("no name=%d (want 400)", code)
	}
	// explicit session scope required
	code, _ = mtDo(t, "POST", base, "/v1/admin/clients", `{"name":"x","allowlist":["full"]}`, itestADM)
	if code != 400 {
		t.Fatalf("no sessions=%d (want 400)", code)
	}
	// unknown allowlist entry
	code, _ = mtDo(t, "POST", base, "/v1/admin/clients",
		`{"name":"x","allowlist":["sendmassage"],"sessions":["a"]}`, itestADM)
	if code != 400 {
		t.Fatalf("typo allowlist=%d (want 400)", code)
	}
	// bad lane
	code, _ = mtDo(t, "POST", base, "/v1/admin/clients",
		`{"name":"x","allowlist":["full"],"sessions":["a"],"lane":"fast"}`, itestADM)
	if code != 400 {
		t.Fatalf("bad lane=%d (want 400)", code)
	}
	// list reflects nothing issued
	code, body := mtDo(t, "GET", base, "/v1/admin/clients", ``, itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("list=%d %v", code, body)
	}
}

func TestAdminClientUpdateAllowlist(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "acc1")
	waitLive(t, app, fake, "acc1")

	id, bearer := registerTestClient(t, base, "grow", `["get_me"]`, `["acc1"]`)

	// widen the allowlist in place
	code, body := mtDo(t, "POST", base, "/v1/admin/clients/"+id,
		`{"allowlist":["get_me","get_chat"]}`, itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("update=%d %v", code, body)
	}
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/get_chat", `{"chat_id":1}`, bearer)
	if code != 200 {
		t.Fatalf("get_chat after widen=%d (want 200)", code)
	}
	code, _ = mtDo(t, "POST", base, "/v1/sessions/acc1/send_message",
		`{"chat_id":1,"text":"still no"}`, bearer)
	if code != 403 {
		t.Fatalf("send after widen=%d (want 403)", code)
	}
}
