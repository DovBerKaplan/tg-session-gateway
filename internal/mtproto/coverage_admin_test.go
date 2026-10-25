package mtproto

// Coverage for the admin HTTP surface and session routes the
// lifecycle tests don't drive: happy paths, 401s and the unknown-alias
// contracts.

import (
	"testing"
)

func TestAdminRoutesCoverage(t *testing.T) {
	fake := &fakeMTClient{}
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "cov1")

	t.Run("401 without admin secret", func(t *testing.T) {
		for _, path := range []string{
			"/v1/admin/sessions/cov1/pause", "/v1/admin/sessions/cov1/resume",
			"/v1/admin/sessions/cov1/drain", "/v1/admin/sessions/cov1/pause_sending",
			"/v1/admin/sessions/cov1/resume_sending", "/v1/admin/sessions/cov1/restart",
			"/v1/admin/sessions/cov1/paid", "/v1/admin/safe-mode/ack",
		} {
			code, _ := mtDo(t, "POST", base, path, `{}`, "")
			if code != 401 {
				t.Errorf("POST %s without auth = %d, want 401", path, code)
			}
		}
		if code, _ := mtDo(t, "GET", base, "/v1/admin/status", "", ""); code != 401 {
			t.Errorf("status without auth = %d", code)
		}
	})

	t.Run("lifecycle actions on a live session", func(t *testing.T) {
		for _, action := range []string{"pause", "resume", "drain", "pause_sending", "resume_sending"} {
			code, b := mtDo(t, "POST", base, "/v1/admin/sessions/cov1/"+action, `{}`, itestADM)
			if code != 200 || b["ok"] != true {
				t.Fatalf("%s: %d %v", action, code, b)
			}
		}
	})

	t.Run("lifecycle on unknown alias is 200 ok:false", func(t *testing.T) {
		// idempotent: blind retries against a gone alias are safe
		for _, action := range []string{"pause", "resume", "drain"} {
			code, b := mtDo(t, "POST", base, "/v1/admin/sessions/nope/"+action, `{}`, itestADM)
			if code != 200 || b["ok"] != false {
				t.Fatalf("%s unknown: %d %v, want 200 ok:false", action, code, b)
			}
		}
	})

	t.Run("restart", func(t *testing.T) {
		if code, _ := mtDo(t, "POST", base, "/v1/admin/sessions/nope/restart", `{}`, itestADM); code != 404 {
			t.Fatalf("restart unknown = %d, want 404", code)
		}
		code, b := mtDo(t, "POST", base, "/v1/admin/sessions/cov1/restart", `{}`, itestADM)
		if code != 200 || b["ok"] != true {
			t.Fatalf("restart live: %d %v", code, b)
		}
	})

	t.Run("dead letters list and clear", func(t *testing.T) {
		if code, _ := mtDo(t, "GET", base, "/v1/admin/sessions/nope/deadletters", "", itestADM); code != 404 {
			t.Fatalf("deadletters unknown = %d, want 404", code)
		}
		code, b := mtDo(t, "GET", base, "/v1/admin/sessions/cov1/deadletters", "", itestADM)
		// an empty list serializes as null (nil slice) — ok is the contract
		if code != 200 || b["ok"] != true {
			t.Fatalf("deadletters list: %d %v", code, b)
		}
		code, b = mtDo(t, "DELETE", base, "/v1/admin/sessions/cov1/deadletters", "", itestADM)
		if code != 200 || b["ok"] != true {
			t.Fatalf("deadletters clear: %d %v", code, b)
		}
	})

	t.Run("paid opt-in", func(t *testing.T) {
		if code, _ := mtDo(t, "POST", base, "/v1/admin/sessions/nope/paid", `{}`, itestADM); code != 404 {
			t.Fatalf("paid unknown = %d, want 404", code)
		}
		code, b := mtDo(t, "POST", base, "/v1/admin/sessions/cov1/paid", `{"enabled":true}`, itestADM)
		if code != 200 || b["paid_broadcasts"] != true {
			t.Fatalf("paid on: %d %v", code, b)
		}
	})

	t.Run("safe mode ack", func(t *testing.T) {
		code, b := mtDo(t, "POST", base, "/v1/admin/safe-mode/ack", `{}`, itestADM)
		if code != 200 || b["safe_mode"] != false {
			t.Fatalf("ack: %d %v", code, b)
		}
	})

	t.Run("session health", func(t *testing.T) {
		if code, _ := mtDo(t, "GET", base, "/v1/sessions/nope/health", "", itestAPP); code != 404 {
			t.Fatalf("health unknown = %d, want 404", code)
		}
		if code, _ := mtDo(t, "GET", base, "/v1/sessions/cov1/health", "", ""); code != 401 {
			t.Fatalf("health without auth = %d, want 401", code)
		}
		code, b := mtDo(t, "GET", base, "/v1/sessions/cov1/health", "", itestAPP)
		if code != 200 || b["ok"] != true || b["rate"] == nil {
			t.Fatalf("health: %d %v", code, b)
		}
	})

	t.Run("delete unknown alias is 200 ok:false", func(t *testing.T) {
		code, b := mtDo(t, "DELETE", base, "/v1/admin/sessions/nope", `{}`, itestADM)
		if code != 200 || b["ok"] != false {
			t.Fatalf("delete unknown: %d %v, want 200 ok:false", code, b)
		}
	})
}

func TestIsAllDigits(t *testing.T) {
	// vacuously true for "" — callers guard emptiness before calling
	cases := map[string]bool{
		"12345": true, "": true, "12a45": false, "-5": false, "12 4": false,
	}
	for in, want := range cases {
		if got := isAllDigits(in); got != want {
			t.Errorf("isAllDigits(%q) = %v, want %v", in, got, want)
		}
	}
}
