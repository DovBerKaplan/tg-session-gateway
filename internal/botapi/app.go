package botapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/health"
	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
	"github.com/DovBerKaplan/tg-session-gateway/internal/version"
)

// App is the whole Bot API gateway: store, sessions, proxy, admin,
// health and metrics wired together with graceful shutdown.
type App struct {
	Cfg       config.BotAPI
	Store     *store.Gateway
	Mgr       *Manager
	Proxy     *Proxy
	Health    *health.Storage
	Beat      *health.Beat
	Boot      *health.BootTracker
	SafeMode  atomic.Bool
	maintStop chan struct{}
	closeOnce sync.Once
	srv       *http.Server
	// adminSrv is non-nil when GW_ADMIN_PORT splits the operator surface
	// onto its own (unexposed) listener.
	adminSrv *http.Server
}

// NewApp wires everything. The HTTPClient is injectable so tests fake
// api.telegram.org; production passes NewUpstreamClient(cfg.Upstream).
func NewApp(cfg config.BotAPI, client HTTPClient) (*App, error) {
	h := health.NewStorage()
	st, err := store.OpenGateway(cfg.DataDir, cfg.FernetKey, cfg.Storage.Durability, cfg.BackupsDir(), h)
	if err != nil {
		return nil, err
	}
	mgr := NewManager(cfg, st, client, h)
	app := &App{
		Cfg: cfg, Store: st, Mgr: mgr, Health: h,
		Proxy:     NewProxy(mgr, client, cfg.Policy.QueueWaitTimeout),
		Boot:      &health.BootTracker{Path: cfg.DataDir + "/boot_history.json"},
		maintStop: make(chan struct{}),
	}
	app.Beat = health.NewBeat()
	app.buildLifecycle()

	mux := http.NewServeMux()
	if cfg.AdminPort > 0 {
		app.publicRoutes(mux)
		adminMux := http.NewServeMux()
		app.adminRoutes(adminMux)
		app.adminSrv = &http.Server{
			Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.AdminPort),
			Handler:           adminMux,
			ReadHeaderTimeout: 10 * time.Second, // slowloris guard
			IdleTimeout:       120 * time.Second,
			// no WriteTimeout on purpose: getUpdates long-polls up to 50s
			// and the WS connections stream indefinitely
		}
	} else {
		app.routes(mux)
	}
	app.srv = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // slowloris guard
		IdleTimeout:       120 * time.Second,
		// no WriteTimeout on purpose: getUpdates long-polls up to 50s
		// and the WS connections stream indefinitely
	}
	return app, nil
}

// buildLifecycle runs the boot sequence (safe-mode check, restore,
// maintenance loop) — split from NewApp so tests can inspect state.
func (a *App) buildLifecycle() {
	go a.maintenanceLoop()

	// crash-loop guard: too many boots in the window → SAFE MODE
	safe, boots := a.Boot.RecordBoot()
	a.SafeMode.Store(safe)
	restored := 0
	if safe {
		slog.Error("SAFE MODE: boots in the last window — sessions stay on disk, nothing auto-connects. Ack via POST /admin/safe_mode/ack",
			"boots", boots, "window_s", 600)
	} else {
		restored = a.Mgr.RestoreAll(context.Background())
	}
	if bad := a.Store.VerifyTokens(); len(bad) > 0 {
		slog.Error("GW_FERNET_KEY cannot decrypt tokens (wrong key? those bots will not start)",
			"aliases", strings.Join(bad, ", "))
	}
	slog.Info("gateway up", "restored", restored, "version", version.Version)
}

// maintenanceLoop runs the hourly integrity/backup/retention sweep.
func (a *App) maintenanceLoop() {
	interval := a.Cfg.Storage.MaintenanceInterval
	if interval <= 0 {
		return
	}
	t := time.NewTicker(time.Duration(interval * float64(time.Second)))
	defer t.Stop()
	for {
		select {
		case <-a.maintStop:
			return
		case <-t.C:
			store.MaintenancePass(
				[]string{a.Store.Path, a.Cfg.DataDir + "/queue.db"},
				a.Cfg.BackupsDir(), a.Health,
				a.Cfg.Storage.BackupKeep, a.Cfg.Storage.BackupInterval,
				"DELETE FROM admin_audit WHERE CAST(strftime('%s', ts) AS REAL) < ?",
				a.Cfg.Storage.AuditRetentionDays)
		}
	}
}

// Close drains everything (graceful shutdown path); idempotent.
func (a *App) Close() {
	a.closeOnce.Do(func() {
		close(a.maintStop)
		a.Beat.Stop()
		a.Mgr.Shutdown()
		a.Store.Close()
	})
}

// ListenAndServe blocks serving HTTP (both listeners when split).
func (a *App) ListenAndServe() error {
	if a.adminSrv != nil {
		go func() { _ = a.adminSrv.ListenAndServe() }()
	}
	return a.srv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests.
func (a *App) Shutdown(ctx context.Context) error {
	if a.adminSrv != nil {
		_ = a.adminSrv.Shutdown(ctx)
	}
	return a.srv.Shutdown(ctx)
}

// RunServe boots and blocks until SIGTERM/SIGINT.
func (a *App) RunServe() error {
	errCh := make(chan error, 1)
	go func() { errCh <- a.ListenAndServe() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-sig:
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)
	a.Close()
	return nil
}

// ── routing ────────────────────────────────────────────────────────

func (a *App) routes(mux *http.ServeMux) {
	a.publicRoutes(mux)
	a.adminRoutes(mux)
}

// publicRoutes is the consumer surface: proxy, alias routes, files and
// the kubelet probes. When GW_ADMIN_PORT splits the listeners, this is
// everything the API Service exposes.
func (a *App) publicRoutes(mux *http.ServeMux) {
	// token-in-path routes need manual parsing: ServeMux wildcards must
	// occupy a whole segment, and "/tgapi/bot<token>/<method>" embeds the
	// token mid-segment.
	mux.HandleFunc("POST /tgapi/", a.handleTGAPIPost)
	mux.HandleFunc("GET /tgapi/", a.handleTGAPIGetRoute)
	mux.HandleFunc("POST /bots/{alias}/{method}", a.handleByAlias)
	mux.HandleFunc("GET /file/", a.handleFileRoute)
	mux.HandleFunc("GET /tgapi/file/", a.handleFileRoute)

	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /readyz", a.handleReadyz)
}

// adminRoutes is the operator surface: /admin/* and /metrics.
func (a *App) adminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/bots", a.adminRegister)
	mux.HandleFunc("GET /admin/status", a.adminStatus)
	mux.HandleFunc("POST /admin/bots/{alias}/pause", a.adminPause)
	mux.HandleFunc("POST /admin/bots/{alias}/resume", a.adminResume)
	mux.HandleFunc("POST /admin/bots/{alias}/drain", a.adminDrain)
	mux.HandleFunc("DELETE /admin/bots/{alias}", a.adminDelete)
	mux.HandleFunc("POST /admin/bots/{alias}/push", a.adminSetPush)
	mux.HandleFunc("POST /admin/bots/{alias}/paid", a.adminSetPaid)
	mux.HandleFunc("POST /admin/bots/{alias}/typing", a.adminSetBotTyping)
	mux.HandleFunc("GET /admin/settings/typing", a.adminGetTypingSettings)
	mux.HandleFunc("POST /admin/settings/typing", a.adminSetTypingSettings)
	mux.HandleFunc("POST /admin/safe_mode/ack", a.adminSafeModeAck)
	mux.HandleFunc("GET /admin/audit", a.adminAudit)

	mux.HandleFunc("GET /metrics", a.handleMetrics)
}

// splitBotTokenPath parses "<prefix>/bot<token>/<rest>" paths.
func splitBotTokenPath(path, prefix string) (token, rest string, ok bool) {
	p := strings.TrimPrefix(path, prefix)
	p = strings.TrimPrefix(p, "/")
	if !strings.HasPrefix(p, "bot") {
		return "", "", false
	}
	remainder := p[len("bot"):]
	idx := strings.Index(remainder, "/")
	if idx < 0 {
		return "", "", false
	}
	return remainder[:idx], remainder[idx+1:], true
}

func (a *App) handleTGAPIPost(w http.ResponseWriter, r *http.Request) {
	token, method, ok := splitBotTokenPath(r.URL.Path, "/tgapi")
	if !ok {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s := a.Mgr.FindByToken(token)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "unknown bot token"})
		return
	}
	a.callRoute(w, r, s, method)
}

func (a *App) handleTGAPIGetRoute(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/tgapi/file/") {
		a.handleFileRoute(w, r)
		return
	}
	token, method, ok := splitBotTokenPath(r.URL.Path, "/tgapi")
	if !ok {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s := a.Mgr.FindByToken(token)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	q := map[string]any{}
	for k, vals := range r.URL.Query() {
		if len(vals) > 0 {
			q[k] = vals[0]
		}
	}
	a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, q, "", nil, ""))
}

func (a *App) handleFileRoute(w http.ResponseWriter, r *http.Request) {
	token, path, ok := splitBotTokenPath(r.URL.Path, "/file")
	if !ok {
		token, path, ok = splitBotTokenPath(r.URL.Path, "/tgapi/file")
	}
	if !ok {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	if a.Mgr.FindByToken(token) == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	resp, err := a.Mgr.client.Do(&httpRequest{Method: "GET", URL: "/file/bot" + token + "/" + path})
	if err != nil {
		writeJSON(w, 502, map[string]any{"ok": false, "error_code": 502, "description": "upstream: " + err.Error()})
		return
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(resp.Body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeRawJSON(w http.ResponseWriter, status int, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (a *App) writeGwResponse(w http.ResponseWriter, r *gwResponse) {
	if r.isRaw() {
		writeRawJSON(w, r.status, r.raw)
		return
	}
	writeJSON(w, r.status, r.jsonBody)
}

func (a *App) adminAuthed(r *http.Request) bool {
	return bearerOK(r.Header.Get("Authorization"), a.Cfg.AdminSecret)
}

func (a *App) appAuthed(r *http.Request) bool {
	return bearerOK(r.Header.Get("Authorization"), a.Cfg.AppSecret)
}

// upload limits: over the cap the gateway answers 413 instead of
// silently truncating (a truncated upload would send a CORRUPT file).
const (
	maxMultipartBody = 64 << 20
	maxFormBody      = 1 << 20
	maxJSONBody      = 8 << 20
)

func tooLarge(w http.ResponseWriter, limit int64) bool {
	const msg = `{"ok":false,"error_code":413,"description":"request body exceeds the gateway limit"}`
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusRequestEntityTooLarge)
	_, _ = w.Write([]byte(msg))
	return true
}

// callRoute parses the request body the way the Python gateway does:
// JSON dict, urlencoded form (ints/bools coerced), or raw multipart.
func (a *App) callRoute(w http.ResponseWriter, r *http.Request, s *BotSession, method string) {
	ctype := r.Header.Get("Content-Type")
	if strings.HasPrefix(ctype, "multipart/") {
		r.Body = http.MaxBytesReader(w, r.Body, maxMultipartBody)
		if r.ContentLength > 0 && r.ContentLength <= maxMultipartBody {
			// STREAM: read a prefix (the chat_id form field lives in the
			// leading parts), forward the rest verbatim without ever
			// holding the whole upload in memory
			prefix := make([]byte, 64<<10)
			n, _ := io.ReadFull(r.Body, prefix)
			a.writeGwResponse(w, a.Proxy.ForwardStream(
				r.Context(), s, method, prefix[:n], r.Body, r.ContentLength, ctype))
			return
		}
		// chunked/unknown length: buffer (bounded) — a ContentLength is
		// mandatory for streaming (api.telegram.org rejects chunked)
		raw, err := io.ReadAll(r.Body)
		if err != nil && isTooLarge(err) {
			tooLarge(w, maxMultipartBody)
			return
		}
		a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, nil, ctype, raw, ""))
		return
	}
	if strings.HasPrefix(ctype, "application/x-www-form-urlencoded") {
		r.Body = http.MaxBytesReader(w, r.Body, maxFormBody)
		raw, err := io.ReadAll(r.Body)
		if err != nil && isTooLarge(err) {
			tooLarge(w, maxFormBody)
			return
		}
		parsed := map[string]any{}
		for _, kv := range strings.Split(string(raw), "&") {
			if kv == "" {
				continue
			}
			parts := strings.SplitN(kv, "=", 2)
			k := urlUnescape(parts[0])
			v := ""
			if len(parts) == 2 {
				v = urlUnescape(parts[1])
			}
			if isNumeric(v) {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					parsed[k] = n
					continue
				}
			}
			if v == "true" || v == "false" {
				parsed[k] = v == "true"
				continue
			}
			parsed[k] = v
		}
		a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, parsed, "", nil, r.Header.Get("Idempotency-Key")))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil && isTooLarge(err) {
		tooLarge(w, maxJSONBody)
		return
	}
	body := map[string]any{}
	if len(raw) > 0 {
		// invalid JSON stays TOLERANT (body = {}): the Python gateway's
		// _json_body does exactly that — parity over taste
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err == nil && parsed != nil {
			body = parsed
		}
	}
	a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, body, "", nil, r.Header.Get("Idempotency-Key")))
}

func isTooLarge(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

func urlUnescape(s string) string {
	if !strings.Contains(s, "%") && !strings.Contains(s, "+") {
		return s
	}
	replacer := strings.NewReplacer("+", " ")
	s = replacer.Replace(s)
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			hi, ok1 := unhex(s[i+1])
			lo, ok2 := unhex(s[i+2])
			if ok1 && ok2 {
				out = append(out, hi<<4|lo)
				i += 2
				continue
			}
		}
		out = append(out, s[i])
	}
	return string(out)
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func (a *App) handleByAlias(w http.ResponseWriter, r *http.Request) {
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "unknown alias"})
		return
	}
	a.callRoute(w, r, s, r.PathValue("method"))
}

// ── admin ──────────────────────────────────────────────────────────

func (a *App) adminRegister(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	var body struct {
		Alias          *string `json:"alias"`
		Token          string  `json:"token"`
		OnLimit        *string `json:"on_limit"`
		PaidBroadcasts bool    `json:"paid_broadcasts"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil || body.Token == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "token required"})
		return
	}
	if body.OnLimit != nil && *body.OnLimit != "queue" && *body.OnLimit != "reject" {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "on_limit must be queue|reject"})
		return
	}
	alias := ""
	if body.Alias != nil {
		alias = *body.Alias
	}
	if alias == "" {
		alias = "bot-" + randHex(4)
	}
	s, err := a.Mgr.Attach(r.Context(), alias, body.Token, body.OnLimit, body.PaidBroadcasts)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error_code": 500, "description": err.Error()})
		return
	}
	a.Store.Audit("register", alias, "token="+store.TokenSuffix(body.Token)+" on_limit="+s.EffectiveOnLimit())
	writeJSON(w, 200, map[string]any{
		"ok": true, "alias": alias, "state": string(s.CurrentState()),
		"on_limit": s.EffectiveOnLimit(),
	})
}

func randHex(n int) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, n)
	for i := range out {
		out[i] = hexdigits[int(time.Now().UnixNano()>>(uint(i)*4))&0xf]
	}
	return string(out)
}

func (a *App) adminStatus(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	bots := []map[string]any{}
	for _, alias := range sortedAliases(a.Mgr.Sessions()) {
		s := a.Mgr.Get(alias)
		if s == nil {
			continue
		}
		st := s.PublicStatus()
		st["typing"] = a.Mgr.Typing().StatusFor(s)
		bots = append(bots, st)
	}
	writeJSON(w, 200, map[string]any{"ok": true, "bots": bots})
}

func (a *App) adminPause(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Pause(alias) {
		a.Store.Audit("pause", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminResume(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Resume(r.Context(), alias) {
		a.Store.Audit("resume", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminDrain(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Drain(alias) {
		a.Store.Audit("drain", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminDelete(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Delete(r.Context(), alias) {
		a.Store.Audit("delete", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminSetPush(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	var body struct {
		URL *string `json:"url"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	url := ""
	if body.URL != nil {
		url = *body.URL
	}
	if !a.Mgr.SetPush(r.Context(), alias, url) {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	a.Store.Audit("set_push", alias, "url="+url)
	writeJSON(w, 200, map[string]any{"ok": true, "push_url": nilIfEmpty(a.Mgr.Get(alias).PushURL())})
}

func (a *App) adminSetPaid(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	var body struct {
		Enabled bool `json:"enabled"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	s := a.Mgr.Get(alias)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s.PaidBroadcasts = body.Enabled
	s.guard.PaidEnabled = body.Enabled
	a.Store.Audit("set_paid", alias, fmt.Sprintf("enabled=%v", body.Enabled))
	writeJSON(w, 200, map[string]any{"ok": true, "paid_broadcasts": s.PaidBroadcasts})
}

func (a *App) adminSetBotTyping(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	var patch TypingPatch
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "invalid JSON"})
		return
	}
	s := a.Mgr.Get(alias)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s.typMu.Lock()
	s.typingEnabled = patch.Enabled
	s.typingPrivateOnly = patch.PrivateOnly
	s.typingIntervalS = patch.IntervalS
	s.typingTimeoutS = patch.TimeoutS
	s.typingTimeoutText = patch.TimeoutText
	s.typMu.Unlock()
	a.Store.Audit("set_typing", alias, describePatch(&patch))
	writeJSON(w, 200, map[string]any{"ok": true, "typing": a.Mgr.Typing().StatusFor(s)})
}

func describePatch(p *TypingPatch) string {
	var parts []string
	if p.Enabled != nil {
		parts = append(parts, fmt.Sprintf("enabled=%v", *p.Enabled))
	}
	if p.PrivateOnly != nil {
		parts = append(parts, fmt.Sprintf("private_only=%v", *p.PrivateOnly))
	}
	if p.IntervalS != nil {
		parts = append(parts, fmt.Sprintf("interval_s=%v", *p.IntervalS))
	}
	if p.TimeoutS != nil {
		parts = append(parts, fmt.Sprintf("timeout_s=%v", *p.TimeoutS))
	}
	if p.TimeoutText != nil {
		parts = append(parts, fmt.Sprintf("timeout_text=%v", *p.TimeoutText))
	}
	if len(parts) == 0 {
		return "reset-all"
	}
	return strings.Join(parts, ", ")
}

func (a *App) adminGetTypingSettings(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "settings": a.Mgr.Typing().Settings().Snapshot()})
}

func (a *App) adminSetTypingSettings(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	var patch TypingPatch
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "invalid JSON"})
		return
	}
	settings := a.Mgr.Typing().Settings().Apply(patch)
	a.Store.Audit("set_typing_global", "global", describePatch(&patch))
	writeJSON(w, 200, map[string]any{"ok": true, "settings": settings})
}

func (a *App) adminSafeModeAck(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	a.Boot.Clear()
	a.SafeMode.Store(false)
	restored := a.Mgr.RestoreAll(r.Context())
	a.Store.Audit("safe_mode_ack", "global", fmt.Sprintf("restored=%d", restored))
	writeJSON(w, 200, map[string]any{"ok": true, "safe_mode": false, "restored": restored})
}

func (a *App) adminAudit(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "entries": a.Store.GetAudit(100)})
}

// ── health & metrics ───────────────────────────────────────────────

// bearerOK compares secrets in constant time (timing side channels).
func bearerOK(header, secret string) bool {
	got := strings.TrimPrefix(header, "Bearer ")
	if len(got) != len(secret) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(secret)) == 1
}

func (a *App) handleHealthz(w http.ResponseWriter, r *http.Request) {
	lag := round2(a.Beat.Lag())
	writeJSON(w, 200, map[string]any{"ok": a.Beat.Alive(), "version": version.Version, "loop_lag_s": lag})
}

func (a *App) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// write probe: catches full/read-only/locked storage
	if _, err := a.Store.DB.Exec("BEGIN IMMEDIATE"); err != nil {
		writeJSON(w, 503, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if _, err := a.Store.DB.Exec("ROLLBACK"); err != nil {
		writeJSON(w, 503, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if reason := a.Health.Degraded(); reason != "" {
		writeJSON(w, 503, map[string]any{"ok": false, "storage": a.Health.Snapshot()})
		return
	}
	if a.SafeMode.Load() {
		writeJSON(w, 503, map[string]any{
			"ok": false, "safe_mode": true,
			"description": "crash-loop safe mode — ack via POST /admin/safe_mode/ack",
		})
		return
	}
	states := map[string]string{}
	live := 0
	for alias, s := range a.Mgr.Sessions() {
		st := string(s.CurrentState())
		states[alias] = st
		if st == "live" {
			live++
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true, "sessions": states, "live": live})
}

func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }

func (a *App) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var lines []string
	for _, alias := range sortedAliases(a.Mgr.Sessions()) {
		s := a.Mgr.Get(alias)
		if s == nil {
			continue
		}
		labels := fmt.Sprintf("alias=%q", s.Alias)
		stateVal := 0
		s.mu.Lock()
		connectedSince := s.connectedSince
		s.mu.Unlock()
		if s.CurrentState() == StateLive {
			stateVal = 1
		}
		age := int64(0)
		if connectedSince > 0 {
			age = int64(wallNow() - connectedSince)
		}
		q := s.queue.Stats()
		lines = append(lines,
			fmt.Sprintf("tg_gateway_session_state{%s} %d", labels, stateVal),
			fmt.Sprintf("tg_gateway_session_connection_age_seconds{%s} %d", labels, age),
			fmt.Sprintf("tg_gateway_update_queue_depth{%s} %d", labels, q["depth"]),
			fmt.Sprintf("tg_gateway_updates_dropped_oldest_total{%s} %d", labels, q["dropped_oldest"]),
			fmt.Sprintf("tg_gateway_updates_expired_by_ttl_total{%s} %d", labels, q["expired_by_ttl"]),
			fmt.Sprintf("tg_gateway_queue_persistent{%s} %d", labels, b2i(s.queue.Persistent())),
			fmt.Sprintf("tg_gateway_real_429_total{%s} %d", labels, s.real429Total()),
			fmt.Sprintf("tg_gateway_synthetic_429_total{%s} %d", labels, s.synth429Total()),
			fmt.Sprintf("tg_gateway_typing_active{%s} %d", labels, a.Mgr.Typing().ActiveCount(s.Alias)),
			fmt.Sprintf("tg_gateway_persist_failures_total{%s} %d", labels, a.Health.PersistFailures),
			fmt.Sprintf("tg_gateway_corrupt_rows_total{%s} %d", labels, a.Health.CorruptRows),
			fmt.Sprintf("tg_gateway_queue_max_age_seconds{%s} %v", labels, q["max_age_s"]),
		)
		if bl := s.queue.UnloadedBacklog(); bl > 0 {
			lines = append(lines, fmt.Sprintf("tg_gateway_backlog_unloaded{%s} %d", labels, bl))
		}
	}
	degraded := 0
	if a.Health.Degraded() != "" {
		degraded = 1
	}
	if n := multipartChatUnresolved.Load(); n > 0 {
		lines = append(lines, fmt.Sprintf("tg_gateway_multipart_chat_unresolved_total %d", n))
	}
	lines = append(lines,
		fmt.Sprintf("tg_gateway_storage_degraded{plane=\"bot\"} %d", degraded),
		fmt.Sprintf("tg_gateway_loop_lag_seconds{plane=\"bot\"} %v", round2(a.Beat.Lag())),
	)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *BotSession) real429Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.real429
}

func (s *BotSession) synth429Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.synth429
}
