package mtproto

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	tgdl "github.com/gotd/td/telegram"
)

// fakeLoginDriver scripts the auth flow without Telegram.
type fakeLoginDriver struct {
	mu              sync.Mutex
	phone, codeHash string
	twoFA           bool
	failPassword    bool
	sentCodeCalls   int
}

func (f *fakeLoginDriver) SendCode(_ context.Context, phone string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentCodeCalls++
	f.phone = phone
	f.codeHash = "hash-1"
	return f.codeHash, nil
}

func (f *fakeLoginDriver) SignIn(_ context.Context, phone, code, codeHash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if phone != f.phone || codeHash != f.codeHash {
		return false, errors.New("phone/hash mismatch")
	}
	if code != "12345" {
		return false, errors.New("PHONE_CODE_INVALID")
	}
	return f.twoFA, nil
}

func (f *fakeLoginDriver) Password(_ context.Context, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPassword {
		return errors.New("PASSWORD_HASH_INVALID")
	}
	if password != "correct horse" {
		return errors.New("PASSWORD_HASH_INVALID")
	}
	return nil
}

func TestLoginCtlHappyPath(t *testing.T) {
	ctl := newLoginCtl(&fakeLoginDriver{})
	if stage, _, _ := ctl.snapshot(); stage != loginStageIdle {
		t.Fatalf("initial stage=%s", stage)
	}
	if err := ctl.Start(context.Background(), "+972500000000"); err != nil {
		t.Fatal(err)
	}
	if stage, _, _ := ctl.snapshot(); stage != loginStageCodeSent {
		t.Fatalf("after start=%s", stage)
	}
	needed, err := ctl.Code(context.Background(), "12345")
	if err != nil || needed {
		t.Fatalf("code=%v err=%v", needed, err)
	}
	if stage, _, _ := ctl.snapshot(); stage != loginStageAuthed {
		t.Fatalf("after code=%s", stage)
	}
}

func TestLoginCtlTwoFA(t *testing.T) {
	ctl := newLoginCtl(&fakeLoginDriver{twoFA: true})
	_ = ctl.Start(context.Background(), "+972500000001")

	needed, err := ctl.Code(context.Background(), "12345")
	if err != nil || !needed {
		t.Fatalf("code=%v err=%v (want password needed)", needed, err)
	}
	if stage, _, _ := ctl.snapshot(); stage != loginStagePassword {
		t.Fatalf("stage=%s", stage)
	}
	// wrong password: stage goes failed, message has no password material
	if err := ctl.Password(context.Background(), "wrong"); err == nil {
		t.Fatal("wrong password must fail")
	}
	if stage, _, lastErr := ctl.snapshot(); stage != loginStageFailed || strings.Contains(lastErr, "wrong") {
		t.Fatalf("stage=%s lastErr=%q", stage, lastErr)
	}
	// a failed login can restart
	if err := ctl.Start(context.Background(), "+972500000001"); err != nil {
		t.Fatal(err)
	}
	_, _ = ctl.Code(context.Background(), "12345")
	if err := ctl.Password(context.Background(), "correct horse"); err != nil {
		t.Fatal(err)
	}
	if stage, _, _ := ctl.snapshot(); stage != loginStageAuthed {
		t.Fatalf("final stage=%s", stage)
	}
}

func TestLoginCtlStageGuards(t *testing.T) {
	ctl := newLoginCtl(&fakeLoginDriver{})
	// code before start
	if _, err := ctl.Code(context.Background(), "12345"); err == nil {
		t.Fatal("code before start must fail")
	}
	// password before password stage
	if err := ctl.Password(context.Background(), "x"); err == nil {
		t.Fatal("password before code must fail")
	}
	_ = ctl.Start(context.Background(), "+972500000002")
	// double start refused while in progress
	if err := ctl.Start(context.Background(), "+972500000002"); err == nil {
		t.Fatal("double start must fail")
	}
	// bad code fails and marks failed
	if _, err := ctl.Code(context.Background(), "00000"); err == nil {
		t.Fatal("bad code must fail")
	}
}

// telethonFixture builds a valid Telethon StringSession: version '1' +
// base64url(pack('>B4sH256s')).
func telethonFixture(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 0, 263)
	buf = append(buf, 2)                     // dc id
	buf = append(buf, 10, 0, 0, 1)           // ip 10.0.0.1
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], 443)
	buf = append(buf, port[:]...)
	key := make([]byte, 256)
	for i := range key {
		key[i] = byte(i)
	}
	buf = append(buf, key...)
	return "1" + base64.URLEncoding.EncodeToString(buf)
}

func TestSessionTelethonDecode(t *testing.T) {
	blob, err := sessionTelethonDecode(telethonFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	// canonical loader envelope: {"Version":1,"Data":{...}}
	var envelope struct {
		Version int `json:"Version"`
		Data    struct {
			DC   int    `json:"DC"`
			Addr string `json:"Addr"`
		} `json:"Data"`
	}
	if err := json.Unmarshal(blob, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != 1 || envelope.Data.DC != 2 ||
		envelope.Data.Addr != net.JoinHostPort("10.0.0.1", "443") {
		t.Fatalf("envelope=%+v", envelope)
	}
	if _, err := sessionTelethonDecode("1!!!not-base64"); err == nil {
		t.Fatal("garbage must fail")
	}
	if _, err := sessionTelethonDecode(""); err == nil {
		t.Fatal("empty must fail")
	}
}

func TestConnOptsParsing(t *testing.T) {
	if parseConnOpts("") != nil || parseConnOpts("   ") != nil {
		t.Fatal("empty opts = nil")
	}
	if parseConnOpts("{bad json") != nil {
		t.Fatal("bad JSON = nil (default dial)")
	}
	co := parseConnOpts(`{"ipv6":"2001:db8::2","device":{"model":"Pixel 8 Pro","system_version":"Android 15","app_version":"10.14.2"}}`)
	if co == nil || co.IPv6 != "2001:db8::2" || co.Device.Model != "Pixel 8 Pro" {
		t.Fatalf("opts=%+v", co)
	}
	// resolver binds tcp6 to the source address
	r, err := co.resolver()
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("resolver must be built for a valid ipv6")
	}
	// a v4 address is rejected
	bad := parseConnOpts(`{"ipv6":"192.0.2.1"}`)
	if _, err := bad.resolver(); err == nil {
		t.Fatal("v4 in ipv6 field must fail")
	}
	// device defaults
	dev := co.device()
	if dev.AppVersion != "10.14.2" || dev.LangCode != "en" {
		t.Fatalf("device=%+v", dev)
	}
	if d := (*connOpts)(nil).device(); d != (tgdl.DeviceConfig{}) {
		t.Fatal("nil opts = zero device")
	}
}

func TestUserSessionHTTPFlow(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	// script: twoFA off; a second client for the 2FA variant
	app.Mgr.LoginDriverFactory = func(alias string) loginDriver {
		if alias == "u2fa" {
			return &fakeLoginDriver{twoFA: true}
		}
		return &fakeLoginDriver{}
	}

	// register a user session (no token)
	code, body := mtDo(t, "POST", base, "/v1/admin/sessions",
		`{"alias":"u1","kind":"user","phone":"+972500000011"}`, itestADM)
	if code != 200 || body["ok"] != true || body["kind"] != "user" {
		t.Fatalf("register user=%d %v", code, body)
	}
	// the fake client never reports awaiting-login, so the session goes
	// live like any other — the login endpoints still drive the machine.

	// login/start without phone → 400
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/u1/login/start", `{}`, itestADM)
	if code != 400 {
		t.Fatalf("start no phone=%d", code)
	}
	// unknown alias → 404
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/nope/login/start", `{"phone":"+1"}`, itestADM)
	if code != 404 {
		t.Fatalf("start unknown=%d", code)
	}

	// happy path
	code, body = mtDo(t, "POST", base, "/v1/admin/sessions/u1/login/start",
		`{"phone":"+972500000011"}`, itestADM)
	if code != 200 || body["stage"] != loginStageCodeSent {
		t.Fatalf("start=%d %v", code, body)
	}
	code, body = mtDo(t, "POST", base, "/v1/admin/sessions/u1/login/code",
		`{"code":"12345"}`, itestADM)
	if code != 200 || body["stage"] != loginStageAuthed {
		t.Fatalf("code=%d %v", code, body)
	}

	// 2FA path on a second session
	mtDo(t, "POST", base, "/v1/admin/sessions", `{"alias":"u2fa","kind":"user"}`, itestADM)
	mtDo(t, "POST", base, "/v1/admin/sessions/u2fa/login/start", `{"phone":"+972500000012"}`, itestADM)
	code, body = mtDo(t, "POST", base, "/v1/admin/sessions/u2fa/login/code", `{"code":"12345"}`, itestADM)
	if code != 409 || body["password_required"] != true {
		t.Fatalf("2fa code=%d %v", code, body)
	}
	code, body = mtDo(t, "POST", base, "/v1/admin/sessions/u2fa/login/password",
		`{"password":"correct horse"}`, itestADM)
	if code != 200 || body["stage"] != loginStageAuthed {
		t.Fatalf("password=%d %v", code, body)
	}

	// bot sessions refuse the login flow
	mtDo(t, "POST", base, "/v1/admin/sessions", `{"alias":"b1","token":"720000:bb"}`, itestADM)
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/b1/login/start", `{"phone":"+1"}`, itestADM)
	if code != 400 {
		t.Fatalf("bot login start=%d (want 400)", code)
	}

	// status exposes kind + login stage (mask, never the phone)
	code, body = mtDo(t, "GET", base, "/v1/admin/status", ``, itestADM)
	if code != 200 {
		t.Fatalf("status=%d", code)
	}
	sessions := body["sessions"].([]any)
	found := false
	for _, raw := range sessions {
		s := raw.(map[string]any)
		if s["alias"] == "u1" {
			found = true
			if s["kind"] != "user" || s["login_stage"] != loginStageAuthed {
				t.Fatalf("user status=%v", s)
			}
			if ph, ok := s["login_phone"].(string); ok && strings.Contains(ph, "97250") {
				t.Fatalf("phone leaked in status: %q", ph)
			}
		}
	}
	if !found {
		t.Fatal("u1 missing from status")
	}
}

func TestImportSessionEndpoint(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	// both sources or neither → 400
	code, _ := mtDo(t, "POST", base, "/v1/admin/sessions/imp1/import", `{}`, itestADM)
	if code != 400 {
		t.Fatalf("no source=%d", code)
	}
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/imp1/import",
		fmt.Sprintf(`{"telethon_string":%q,"gotd_session_b64":"e30="}`, telethonFixture(t)), itestADM)
	if code != 400 {
		t.Fatalf("two sources=%d", code)
	}

	// telethon import: seals and registers the user session
	code, body := mtDo(t, "POST", base, "/v1/admin/sessions/imp1/import",
		fmt.Sprintf(`{"telethon_string":%q,"phone":"+972500000021"}`, telethonFixture(t)), itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("import=%d %v", code, body)
	}
	if !app.Store.HasGotdSession("imp1") {
		t.Fatal("blob not stored")
	}
	if meta, ok := app.Store.GetSessionMeta("imp1"); !ok || meta.Kind != "user" {
		t.Fatalf("meta=%+v ok=%v", meta, ok)
	}

	// second import without force → 409
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/imp1/import",
		fmt.Sprintf(`{"telethon_string":%q}`, telethonFixture(t)), itestADM)
	if code != 409 {
		t.Fatalf("overwrite without force=%d (want 409)", code)
	}

	// force replaces
	gotdBlob := base64.StdEncoding.EncodeToString([]byte(`{"version":2,"dc":2}`))
	code, body = mtDo(t, "POST", base, "/v1/admin/sessions/imp1/import",
		`{"gotd_session_b64":"`+gotdBlob+`","force":true}`, itestADM)
	if code != 200 || body["ok"] != true {
		t.Fatalf("force import=%d %v", code, body)
	}

	// invalid sources
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/imp2/import", `{"telethon_string":"junk"}`, itestADM)
	if code != 400 {
		t.Fatalf("junk telethon=%d", code)
	}
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/imp2/import", `{"gotd_session_b64":"!!!"}`, itestADM)
	if code != 400 {
		t.Fatalf("bad base64=%d", code)
	}
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions/imp2/import", `{"gotd_session_b64":"aW52YWxpZA=="}`, itestADM)
	if code != 400 {
		t.Fatalf("non-json blob=%d", code)
	}

	// the audit trail records the import without the material
	entries := app.Store.GetAudit(50)
	sawImport := false
	for _, e := range entries {
		if e.Action == "import" && e.Alias == "imp1" {
			sawImport = true
			if strings.Contains(e.Detail, "telethon1") || strings.Contains(e.Detail, "AAAA") {
				t.Fatalf("audit leaked session material: %q", e.Detail)
			}
		}
	}
	if !sawImport {
		t.Fatal("import not audited")
	}
}

func TestUserSessionRegisterValidation(t *testing.T) {
	fake := &fakeMTClient{}
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	// bad kind
	code, _ := mtDo(t, "POST", base, "/v1/admin/sessions", `{"alias":"x","kind":"agent"}`, itestADM)
	if code != 400 {
		t.Fatalf("bad kind=%d", code)
	}
	// bot without token
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions", `{"alias":"x","kind":"bot"}`, itestADM)
	if code != 400 {
		t.Fatalf("bot no token=%d", code)
	}
	// bad conn_opts
	code, _ = mtDo(t, "POST", base, "/v1/admin/sessions",
		`{"alias":"x","kind":"user","conn_opts":"not-json"}`, itestADM)
	if code != 400 {
		t.Fatalf("bad conn_opts=%d", code)
	}
}
