package botapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
)

func TestSupervisorForceFailoverAndFailback(t *testing.T) {
	cfg := config.BotAPI{
		Upstream:           "https://api.telegram.org",
		UpstreamFallback:   "http://telegram-bot-api:8081",
		UpstreamFailover:   "auto",
		UpstreamKind:       "cloud",
		UpstreamTimeoutS:   5,
		MaxMultipartMB:     64,
		EnvDetect:          "off",
	}
	sup, err := NewUpstreamSupervisor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sup.Stop)
	if err := sup.ForceFailover("test"); err != nil {
		t.Fatal(err)
	}
	st := sup.Status()
	if st["failover_active"] != true || st["active"] != "http://telegram-bot-api:8081" {
		t.Fatalf("status=%v", st)
	}
	if cfg.EffectiveKind() == "local" || true {
		// ApplyLocalPreset mutates supervisor cfg copy
	}
	_ = sup.Failback()
	st = sup.Status()
	if st["failover_active"] != false {
		t.Fatalf("after failback: %v", st)
	}
}

func TestSupervisorNoFallback(t *testing.T) {
	cfg := config.BotAPI{Upstream: "https://api.telegram.org", UpstreamTimeoutS: 5}
	sup, err := NewUpstreamSupervisor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sup.ForceFailover("x"); err == nil {
		t.Fatal("expected error without fallback")
	}
}

func TestIsTLSOrTransportErr(t *testing.T) {
	if !isTLSOrTransportErr(errors.New("x509: certificate has expired")) {
		t.Fatal("expected tls err")
	}
	if isTLSOrTransportErr(errors.New("429 too many")) {
		t.Fatal("429 is not transport")
	}
}

func TestAdminUpstreamEndpointsWithSupervisor(t *testing.T) {
	okJSON := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	primary := httptest.NewServer(okJSON)
	fallback := httptest.NewServer(okJSON)
	t.Cleanup(primary.Close)
	t.Cleanup(fallback.Close)

	cfg := testCfg(t.TempDir())
	cfg.Upstream = primary.URL
	cfg.UpstreamFallback = fallback.URL
	cfg.UpstreamKind = "cloud"
	cfg.UpstreamFailover = "off"
	cfg.EnvDetect = "off"
	sup, err := NewUpstreamSupervisor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sup.Stop)
	app, err := NewApp(cfg, sup)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(appMux(app))
	t.Cleanup(func() { ts.Close(); app.Close() })
	auth := map[string]string{"Authorization": "Bearer adm-secret"}

	if code, _ := get(t, ts.URL, "/admin/upstream", nil); code != 401 {
		t.Fatalf("upstream without secret=%d want 401", code)
	}

	code, raw := get(t, ts.URL, "/admin/upstream", auth)
	if code != 200 {
		t.Fatalf("upstream=%d", code)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if st["active"] != primary.URL || st["failover_active"] != false {
		t.Fatalf("initial status=%v", st)
	}

	code, raw = get(t, ts.URL, "/admin/env", auth)
	if code != 200 {
		t.Fatalf("env=%d", code)
	}
	st = map[string]any{}
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if st["env_detect"] != "off" {
		t.Fatalf("env=%v", st)
	}

	code, body := post(t, ts.URL, "/admin/upstream/failover", `{}`, auth)
	if code != 200 || body["ok"] != true {
		t.Fatalf("failover=%d %v", code, body)
	}
	code, raw = get(t, ts.URL, "/admin/upstream", auth)
	st = map[string]any{}
	_ = json.Unmarshal([]byte(raw), &st)
	if st["active"] != fallback.URL || st["failover_active"] != true {
		t.Fatalf("after failover=%v", st)
	}

	code, body = post(t, ts.URL, "/admin/upstream/failback", `{}`, auth)
	if code != 200 || body["ok"] != true {
		t.Fatalf("failback=%d %v", code, body)
	}
	code, raw = get(t, ts.URL, "/admin/upstream", auth)
	st = map[string]any{}
	_ = json.Unmarshal([]byte(raw), &st)
	if st["active"] != primary.URL || st["failover_active"] != false {
		t.Fatalf("after failback=%v", st)
	}
}

func TestAdminUpstreamEndpointsWithoutSupervisor(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	auth := map[string]string{"Authorization": "Bearer adm-secret"}

	code, raw := get(t, ts.URL, "/admin/upstream", auth)
	if code != 200 {
		t.Fatalf("upstream=%d", code)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if st["active"] != "http://fake.local" || st["failover_active"] != false {
		t.Fatalf("status=%v", st)
	}

	code, body := post(t, ts.URL, "/admin/upstream/failover", `{}`, auth)
	if code != 400 || body["ok"] != false {
		t.Fatalf("failover without supervisor=%d %v", code, body)
	}
	code, body = post(t, ts.URL, "/admin/upstream/failback", `{}`, auth)
	if code != 400 || body["ok"] != false {
		t.Fatalf("failback without supervisor=%d %v", code, body)
	}
	code, raw = get(t, ts.URL, "/admin/env", auth)
	if code != 200 {
		t.Fatalf("env=%d", code)
	}
	st = map[string]any{}
	if err := json.Unmarshal([]byte(raw), &st); err != nil || st["env_detect"] != "" {
		t.Fatalf("env=%v err=%v", st, err)
	}
}
