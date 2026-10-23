// Package mtproto: HTTP surface of the MTProto sidecar — the port of
// mtgateway/app.py. Same routes, same JSON shapes, same metric names;
// the only contract change is the adapter allowlist: unknown methods
// answer 501 with the supported list instead of Pyrogram's getattr.
package mtproto

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/health"
	"github.com/DovBerKaplan/tg-session-gateway/internal/lock"
	"github.com/DovBerKaplan/tg-session-gateway/internal/rateguard"
	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
	"github.com/DovBerKaplan/tg-session-gateway/internal/version"
)

// App is the whole MTProto sidecar: store, session manager, admin,
// health and metrics wired together with graceful shutdown.
type App struct {
	Cfg    config.MTProto
	Store  *store.MT
	Mgr    *Manager
	Health *health.Storage
	Beat   *health.Beat
	Boot   *health.BootTracker

	SafeMode atomic.Bool

	maintStop chan struct{}
	houseStop chan struct{}
	closeOnce sync.Once
	srv       *http.Server
	// adminSrv is non-nil when GW_ADMIN_PORT splits the operator surface
	// onto its own (unexposed) listener.
	adminSrv *http.Server
}

// NewApp wires everything (production client factory: gotd).
func NewApp(cfg config.MTProto) (*App, error) {
	return newApp(cfg, false)
}

// newApp builds the app; skipRestore defers RestoreAll to the caller —
// in-package tests use it to inject a fake ClientFactory BEFORE any
// supervisor starts (boot restore would otherwise dial real Telegram).
func newApp(cfg config.MTProto, skipRestore bool) (*App, error) {
	h := health.NewStorage()
	st, err := store.OpenMT(cfg.DataDir, cfg.FernetKey, cfg.Storage.Durability, cfg.DataDir+"/backups", h)
	if err != nil {
		return nil, err
	}
	app := &App{
		Cfg:       cfg,
		Store:     st,
		Mgr:       NewManager(cfg, st, h),
		Health:    h,
		Boot:      &health.BootTracker{Path: cfg.DataDir + "/boot_history.json"},
		maintStop: make(chan struct{}),
		houseStop: make(chan struct{}),
	}
	app.Beat = health.NewBeat()
	app.buildLifecycle(skipRestore)

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
// housekeeping and maintenance loops).
func (a *App) buildLifecycle(skipRestore bool) {
	go a.housekeepingLoop()
	go a.maintenanceLoop()

	// crash-loop guard: too many boots in the window → SAFE MODE
	safe, boots := a.Boot.RecordBoot()
	a.SafeMode.Store(safe)
	restored := 0
	if safe {
		slog.Error("SAFE MODE: boots in the last window — sessions stay on disk, nothing auto-connects. Ack via POST /v1/admin/safe-mode/ack",
			"boots", boots, "window_s", 600)
	} else if skipRestore {
		// caller restores after wiring the client factory
	} else {
		restored = a.Mgr.RestoreAll(context.Background())
	}
	if bad := a.Store.VerifyTokens(); len(bad) > 0 {
		slog.Error("GW_FERNET_KEY cannot decrypt tokens (wrong key? those sessions will not start)",
			"aliases", strings.Join(bad, ", "))
	}
	slog.Info("MTProto gateway up", "restored", restored, "version", version.Version)
}

// housekeepingLoop runs the 60s pass: state snapshots, idempotency
// cleanup, queue TTL enforcement (GW_UPDATE_TTL_S).
func (a *App) housekeepingLoop() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.houseStop:
			return
		case <-t.C:
			a.Mgr.PersistStates()
			a.Store.IdempotencyCleanup()
			ttl := a.Cfg.Policy.UpdateTTLS
			cutoff := wallNow() - ttl
			for _, s := range a.Mgr.Sessions() {
				droppedMem := s.PruneExpiredUpdates(ttl)
				droppedDB := a.Store.QueueTTLCleanup(s.Alias, cutoff)
				if droppedMem > 0 || droppedDB > 0 {
					slog.Info("TTL pruned updates", "alias", s.Alias,
						"in_memory", droppedMem, "persisted", droppedDB)
				}
			}
		}
	}
}

// maintenanceLoop runs the periodic integrity/backup/retention sweep.
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
				[]string{a.Store.Path},
				a.Cfg.DataDir+"/backups", a.Health,
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
		close(a.houseStop)
		a.Beat.Stop()
		a.Mgr.PersistStates() // final snapshot before exit
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

// RunServe blocks until SIGTERM/SIGINT.
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

// publicRoutes is the consumer surface: method proxy, getUpdates, WS
// push, files and the kubelet probes.
func (a *App) publicRoutes(mux *http.ServeMux) {
	// the dedicated getUpdates route is strictly more specific than
	// {method}, so ServeMux picks it — no delegation needed.
	mux.HandleFunc("POST /v1/sessions/{alias}/getUpdates", a.handleGetUpdates)
	mux.HandleFunc("POST /v1/sessions/{alias}/{method}", a.handleProxyMethod)
	// token is embedded mid-segment: manual parsing, like /tgapi/
	mux.HandleFunc("POST /v1/token/", a.handleProxyByToken)

	mux.HandleFunc("GET /v1/sessions/{alias}/updates", a.handleWSUpdates)
	mux.HandleFunc("GET /v1/sessions/{alias}/health", a.handleSessionHealth)
	mux.HandleFunc("GET /v1/sessions/{alias}/files/{file_id}", a.handleDownloadFile)

	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /readyz", a.handleReadyz)
}

// adminRoutes is the operator surface: /v1/admin/* and /metrics.
func (a *App) adminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/admin/sessions", a.adminRegister)
	mux.HandleFunc("GET /v1/admin/status", a.adminStatus)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/pause", a.adminPause)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/resume", a.adminResume)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/drain", a.adminDrain)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/restart", a.adminRestart)
	mux.HandleFunc("POST /v1/admin/safe-mode/ack", a.adminSafeModeAck)
	mux.HandleFunc("GET /v1/admin/sessions/{alias}/deadletters", a.adminListDeadLetters)
	mux.HandleFunc("DELETE /v1/admin/sessions/{alias}/deadletters", a.adminClearDeadLetters)
	mux.HandleFunc("DELETE /v1/admin/sessions/{alias}", a.adminDelete)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/pause_sending", a.adminPauseSending)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/resume_sending", a.adminResumeSending)
	mux.HandleFunc("POST /v1/admin/sessions/{alias}/paid", a.adminSetPaid)
	mux.HandleFunc("GET /v1/admin/audit", a.adminAudit)

	mux.HandleFunc("GET /metrics", a.handleMetrics)
}

func (a *App) appAuthed(r *http.Request) bool {
	return bearerOK(r.Header.Get("Authorization"), a.Cfg.AppSecret)
}

func (a *App) adminAuthed(r *http.Request) bool {
	return bearerOK(r.Header.Get("Authorization"), a.Cfg.AdminSecret)
}

// ── method proxy ───────────────────────────────────────────────────

// handleProxyMethod is POST /v1/sessions/{alias}/{method}.
func (a *App) handleProxyMethod(w http.ResponseWriter, r *http.Request) {
	alias := r.PathValue("alias")
	method := r.PathValue("method")
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(alias)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "unknown session: " + alias})
		return
	}
	a.dispatchProxy(w, r, s, method)
}

// handleProxyByToken is POST /v1/token/bot<token>/<method> — the same
// contract for consumers that only hold the bot token.
func (a *App) handleProxyByToken(w http.ResponseWriter, r *http.Request) {
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	token, method, ok := splitBotTokenPath(r.URL.Path, "/v1/token")
	if !ok {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s := a.Mgr.FindByToken(token)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "unknown bot token"})
		return
	}
	a.dispatchProxy(w, r, s, method)
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

// extractChatRef resolves the rate-guard chat scope: chat_id, else
// chat_username, else chat; numeric strings coerce, anything else (a
// @username) has no numeric scope.
func extractChatRef(body map[string]any) *int64 {
	var v any
	for _, k := range []string{"chat_id", "chat_username", "chat"} {
		if val, ok := body[k]; ok && val != nil && val != "" {
			v = val
			break
		}
	}
	if v == nil {
		return nil
	}
	switch n := v.(type) {
	case float64:
		c := int64(n)
		return &c
	case int64:
		return &n
	case int:
		c := int64(n)
		return &c
	case string:
		numeric := strings.TrimLeft(n, "-")
		if numeric != "" && isAllDigits(numeric) {
			if i, err := strconv.ParseInt(n, 10, 64); err == nil {
				return &i
			}
		}
	}
	return nil
}

func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// dispatchProxy is the shared alias/token proxy body — the port of
// proxy_method: idempotency, E4 pause, rate guard, adapter allowlist,
// flood-wait reporting.
func (a *App) dispatchProxy(w http.ResponseWriter, r *http.Request, s *Session, method string) {
	if s.LiveClient() == nil {
		writeJSON(w, 503, map[string]any{
			"ok": false, "error_code": 503,
			"description": "session state: " + string(s.CurrentState()),
			"retry_in":    3,
		})
		return
	}

	body := map[string]any{}
	raw, readErr := readAll(r)
	if readErr != nil && isTooLarge(readErr) {
		writeJSON(w, 413, map[string]any{
			"ok": false, "error_code": 413,
			"description": "request body exceeds the gateway limit",
		})
		return
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "invalid JSON body"})
			return
		}
	}

	chatRef := extractChatRef(body)

	// E2: idempotency — atomic claim; a FAILED send releases the key,
	// only a completed send suppresses replays.
	idemKey := ""
	if k, ok := body["_idempotency_key"].(string); ok {
		idemKey = k
		delete(body, "_idempotency_key")
	}
	if idemKey != "" {
		if s.CheckIdempotency(idemKey) == "replay" {
			writeJSON(w, 200, map[string]any{
				"ok": true, "result": nil, "idempotent_replay": true,
				"description": "duplicate send suppressed (idempotency key already seen)",
			})
			return
		}
	}

	// E4: send-paused sessions accept reads, reject writes
	if s.SendPausedNow() && !isReadMethod(method) {
		writeJSON(w, 423, map[string]any{
			"ok": false, "error_code": 423,
			"description": "sending is paused for this session (E4)",
		})
		return
	}

	// rate guard admission
	paidRequested := false
	if v, ok := body["allow_paid_broadcast"].(bool); ok {
		paidRequested = v
	}
	s.Guard.EnterPaidLane(paidRequested)
	defer s.Guard.ExitPaidLane()
	if paidRequested && !s.Guard.PaidEnabled {
		writeJSON(w, 403, map[string]any{
			"ok": false, "error_code": 403,
			"description": "paid broadcast requires gateway opt-in",
		})
		return
	}

	waited := 0.0
	for !s.Guard.TryAcquire(method, chatRef) {
		if a.Cfg.Policy.OnLimit == "reject" || waited > a.Cfg.Policy.QueueWaitTimeout {
			s.noteSynthetic429()
			retry := s.Guard.WaitTime(method, chatRef)
			if retry < 1.0 {
				retry = 1.0
			} else if retry > 60.0 {
				retry = 60.0
			}
			writeJSON(w, 429, map[string]any{
				"ok": false, "error_code": 429,
				"description": fmt.Sprintf("Too Many Requests: retry after %d", int(retry)),
				"parameters":  map[string]any{"retry_after": int(retry)},
			})
			return
		}
		wait := s.Guard.WaitTime(method, chatRef)
		if wait > 2.0 {
			wait = 2.0
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(wait * float64(time.Second))):
		}
		waited += wait
	}

	client := s.LiveClient()
	result, err := Dispatch(r.Context(), client, method, body)

	if err == ErrUnsupported {
		writeJSON(w, 501, map[string]any{
			"ok": false, "error_code": 501,
			"description": "unsupported method (not in the adapter allowlist): " + method +
				"; supported: " + strings.Join(SupportedMethods(), ", "),
		})
		return
	}

	if err != nil {
		// the send FAILED — release the claim so a retry is not suppressed
		if idemKey != "" {
			a.Store.IdempotencyFail(s.Alias, idemKey)
		}
		if info := rateguard.ParseFloodWait(err.Error()); info != nil {
			s.noteRealFloodWait()
			adaptive := s.Guard.ReportFloodWait(err.Error(), chatRef)
			retry := info.Wait
			if adaptive != nil {
				if aw := s.Guard.AdaptiveWait(adaptive, chatRef); aw > 0 {
					retry = aw
				}
			}
			s.noteLastFloodWait(info, chatRef)
			writeJSON(w, 429, map[string]any{
				"ok": false, "error_code": 429,
				"description": fmt.Sprintf("FLOOD_WAIT: retry after %d", retry),
				"parameters":  map[string]any{"retry_after": retry},
			})
			return
		}
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		writeJSON(w, 500, map[string]any{"ok": false, "error_code": 500, "description": msg})
		return
	}

	// the send completed — the key now suppresses replays
	if idemKey != "" {
		a.Store.IdempotencyFinish(s.Alias, idemKey)
		s.NoteIdempotencyDone(idemKey)
	}
	// clear backoff on success (positive signal)
	if chatRef != nil {
		s.Guard.ClearPeerBackoff(*chatRef)
	}
	// E7: deploy-to-first-message SLO
	s.NoteFirstSend()

	writeJSON(w, 200, map[string]any{"ok": true, "result": result})
}

// isReadMethod lists calls exempt from the E4 send pause.
func isReadMethod(method string) bool {
	switch strings.ToLower(method) {
	case "answer_callback_query", "answer_inline_query", "get_updates", "getupdates", "get_me", "get_chat", "resolve_username":
		return true
	}
	return false
}

func (s *Session) noteSynthetic429() {
	s.mu.Lock()
	s.Synthetic429++
	s.mu.Unlock()
}

func (s *Session) noteRealFloodWait() {
	s.mu.Lock()
	s.RealFloodWait++
	s.mu.Unlock()
}

func (s *Session) noteLastFloodWait(info *rateguard.FloodInfo, chatRef *int64) {
	m := map[string]any{"type": info.Type, "wait": info.Wait, "at": wallNow()}
	if chatRef != nil {
		m["chat_id"] = *chatRef
	}
	s.mu.Lock()
	s.LastFloodWait = m
	s.mu.Unlock()
}

// ── pull updates (Bot API compat) ──────────────────────────────────

// handleGetUpdates is POST /v1/sessions/{alias}/getUpdates.
// Offset mode: offset = FIRST update wanted; everything strictly below
// is acked (delivery does NOT ack). No-offset mode keeps the legacy
// delivery=ack behavior.
func (a *App) handleGetUpdates(w http.ResponseWriter, r *http.Request) {
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}

	body := map[string]any{}
	raw, _ := readAll(r)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body) // tolerant: bad JSON pulls defaults
	}

	timeout := 0.0
	if v, ok := anyToFloat(body["timeout"]); ok {
		timeout = v
	}
	if timeout > 50.0 {
		timeout = 50.0
	}
	limit := 100
	if v, ok := anyToInt64(body["limit"]); ok && v > 0 {
		limit = int(v)
	}

	if off, ok := anyToInt64(body["offset"]); ok {
		// Bot API offset semantics: ack everything strictly below
		_ = a.Store.QueueAck(s.Alias, off-1)
		deadline := monoNow() + timeout
		for {
			snapshot := a.Store.QueuePull(s.Alias, off-1, limit)
			if len(snapshot) > 0 {
				writeJSON(w, 200, map[string]any{"ok": true, "result": snapshot})
				return
			}
			remaining := deadline - monoNow()
			if remaining <= 0 {
				writeJSON(w, 200, map[string]any{"ok": true, "result": []any{}})
				return
			}
			if remaining > 0.2 {
				remaining = 0.2
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Duration(remaining * float64(time.Second))):
			}
		}
	}

	// legacy no-offset mode: delivery = ack
	if timeout == 0 {
		updates := drainUpdateCh(s, limit)
		_ = a.Store.QueueAck(s.Alias, maxSeq(updates))
		writeJSON(w, 200, map[string]any{"ok": true, "result": updates})
		return
	}

	// long-poll
	deadline := monoNow() + timeout
	for {
		updates := drainUpdateCh(s, limit)
		if len(updates) > 0 {
			_ = a.Store.QueueAck(s.Alias, maxSeq(updates))
			writeJSON(w, 200, map[string]any{"ok": true, "result": updates})
			return
		}
		remaining := deadline - monoNow()
		if remaining <= 0 {
			writeJSON(w, 200, map[string]any{"ok": true, "result": []any{}})
			return
		}
		if remaining > 0.5 {
			remaining = 0.5
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(remaining * float64(time.Second))):
		}
	}
}

func drainUpdateCh(s *Session, limit int) []map[string]any {
	out := []map[string]any{}
	for len(out) < limit {
		select {
		case u := <-s.updateCh:
			out = append(out, u)
		default:
			return out
		}
	}
	return out
}

func maxSeq(updates []map[string]any) int64 {
	max := int64(0)
	for _, u := range updates {
		if v, ok := anyToInt64(u["_gw_seq"]); ok && v > max {
			max = v
		}
	}
	return max
}

// ── WebSocket push updates ─────────────────────────────────────────

// handleWSUpdates is GET /v1/sessions/{alias}/updates.
func (a *App) handleWSUpdates(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()

	alias := r.PathValue("alias")
	s := a.Mgr.Get(alias)
	if s == nil {
		_ = c.Write(r.Context(), websocket.MessageText,
			mustJSON(map[string]any{"ok": false, "error": "unknown session"}))
		_ = c.Close(websocket.StatusCode(4404), "unknown session")
		return
	}
	if !a.appAuthed(r) {
		_ = c.Write(r.Context(), websocket.MessageText,
			mustJSON(map[string]any{"ok": false, "error": "unauthorized"}))
		_ = c.Close(websocket.StatusCode(4401), "unauthorized")
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	consumerID := randSuffix(8)
	writeCh := make(chan []byte, 64)
	go func() {
		for msg := range writeCh {
			wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.Write(wctx, websocket.MessageText, msg)
			wcancel()
			if err != nil {
				cancel()
				return
			}
		}
	}()

	s.AddConsumer(&Consumer{
		ID:          consumerID,
		Send:        func(data string) error { return trySend(writeCh, []byte(data)) },
		ConnectedAt: time.Now(),
		LastSeen:    time.Now(),
	})
	defer func() {
		s.RemoveConsumer(consumerID)
		close(writeCh)
	}()

	_ = c.Write(ctx, websocket.MessageText, mustJSON(map[string]any{
		"ok": true, "consumer_id": consumerID, "session": alias,
		"state": string(s.CurrentState()),
	}))

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var msg map[string]any
		if err := json.Unmarshal(data, &msg); err == nil {
			if t, _ := msg["type"].(string); t == "ping" {
				select {
				case writeCh <- mustJSON(map[string]any{"type": "pong"}):
				default:
				}
			}
		}
	}
}

// trySend enqueues without blocking the supervisor goroutine; a full
// buffer means the consumer is slow — drop rather than stall Telegram.
func trySend(ch chan []byte, data []byte) error {
	select {
	case ch <- data:
		return nil
	default:
		return fmt.Errorf("consumer write buffer full")
	}
}

// ── session health ─────────────────────────────────────────────────

func (a *App) handleSessionHealth(w http.ResponseWriter, r *http.Request) {
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	out := map[string]any{"ok": true}
	for k, v := range s.Stats() {
		out[k] = v
	}
	out["rate"] = s.Guard.Snapshot()
	writeJSON(w, 200, out)
}

// ── media file proxy ───────────────────────────────────────────────

func (a *App) handleDownloadFile(w http.ResponseWriter, r *http.Request) {
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	client := s.LiveClient()
	if client == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	data, err := client.DownloadMedia(r.Context(), r.PathValue("file_id"))
	if err != nil {
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		writeJSON(w, 500, map[string]any{"ok": false, "error_code": 500, "description": msg})
		return
	}
	if data == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "media not found"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)
	_, _ = w.Write(data)
}

// ── admin endpoints ────────────────────────────────────────────────

func (a *App) adminRegister(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	var body struct {
		Alias string `json:"alias"`
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Alias == "" || body.Token == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "alias and token required"})
		return
	}
	s, err := a.Mgr.Register(r.Context(), body.Alias, body.Token)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error_code": 500, "description": err.Error()})
		return
	}
	a.Store.Audit("register", body.Alias, "token=…"+last4(body.Token))
	writeJSON(w, 200, map[string]any{"ok": true, "alias": s.Alias, "state": string(s.CurrentState())})
}

func (a *App) adminStatus(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	sessions := a.Mgr.Sessions()
	aliases := make([]string, 0, len(sessions))
	for alias := range sessions {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	out := make([]map[string]any, 0, len(aliases))
	for _, alias := range aliases {
		out = append(out, sessions[alias].Stats())
	}
	writeJSON(w, 200, map[string]any{"ok": true, "sessions": out})
}

func (a *App) adminLifecycle(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.adminAuthed(r) {
			writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
			return
		}
		alias := r.PathValue("alias")
		var ok bool
		switch action {
		case "pause":
			ok = a.Mgr.Pause(alias)
		case "resume":
			ok = a.Mgr.Resume(alias)
		case "drain":
			ok = a.Mgr.Drain(alias)
		case "pause_sending":
			ok = a.Mgr.PauseSending(alias)
		case "resume_sending":
			ok = a.Mgr.ResumeSending(alias)
		}
		if ok {
			a.Store.Audit(action, alias, "")
		}
		writeJSON(w, 200, map[string]any{"ok": ok})
	}
}

func (a *App) adminPause(w http.ResponseWriter, r *http.Request)  { a.adminLifecycle("pause")(w, r) }
func (a *App) adminResume(w http.ResponseWriter, r *http.Request) { a.adminLifecycle("resume")(w, r) }
func (a *App) adminDrain(w http.ResponseWriter, r *http.Request)  { a.adminLifecycle("drain")(w, r) }
func (a *App) adminPauseSending(w http.ResponseWriter, r *http.Request) {
	a.adminLifecycle("pause_sending")(w, r)
}
func (a *App) adminResumeSending(w http.ResponseWriter, r *http.Request) {
	a.adminLifecycle("resume_sending")(w, r)
}

// adminRestart is escalation rung 1: reconnect ONE session (never the
// process). Terminal (quarantined) sessions are refused — a revoked
// key needs unregister + re-auth instead.
func (a *App) adminRestart(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	if s.IsTerminal() {
		writeJSON(w, 409, map[string]any{
			"ok": false, "error_code": 409,
			"description": "session quarantined (terminal error) — unregister and re-register instead",
		})
		return
	}
	ok := a.Mgr.Restart(r.PathValue("alias"))
	if ok {
		a.Store.Audit("restart", r.PathValue("alias"), "")
	}
	writeJSON(w, 200, map[string]any{"ok": ok})
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

func (a *App) adminListDeadLetters(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Get(alias) == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "dead_letters": a.Store.ListDeadLetters(alias, 200)})
}

func (a *App) adminClearDeadLetters(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	n := a.Store.ClearDeadLetters(alias)
	a.Store.Audit("deadletters_clear", alias, fmt.Sprintf("count=%d", n))
	writeJSON(w, 200, map[string]any{"ok": true, "cleared": n})
}

func (a *App) adminDelete(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Unregister(r.Context(), alias) {
		a.Store.Audit("delete", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminSetPaid(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	var body struct {
		Enabled bool `json:"enabled"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.Guard.PaidEnabled = body.Enabled
	a.Store.Audit("set_paid", s.Alias, fmt.Sprintf("enabled=%v", body.Enabled))
	writeJSON(w, 200, map[string]any{"ok": true, "paid_broadcasts": s.Guard.PaidEnabled})
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
	writeJSON(w, 200, map[string]any{"ok": a.Beat.Alive(), "version": version.Version, "loop_lag_s": round2(a.Beat.Lag())})
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
			"description": "crash-loop safe mode — ack via POST /v1/admin/safe-mode/ack",
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

func (a *App) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !a.Cfg.MetricsEnabled {
		w.WriteHeader(404)
		return
	}
	var lines []string
	sessions := a.Mgr.Sessions()
	aliases := make([]string, 0, len(sessions))
	for alias := range sessions {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		s := sessions[alias]
		labels := fmt.Sprintf("alias=%q", alias)
		stateVal := 0
		if s.CurrentState() == StateLive {
			stateVal = 1
		}
		s.mu.Lock()
		connectedSince := s.ConnectedSince
		realFlood := s.RealFloodWait
		synth := s.Synthetic429
		sendPaused := s.SendPaused
		reconnects := s.reconnects
		startAttempts := s.startAttempts
		terminal := s.terminalError
		idemClaims := s.IdempotencyClaims
		persistFails := s.PersistFailures
		queueDepth := len(s.updateCh)
		consumers := len(s.consumers)
		lastUpdateAt := s.LastUpdateAt
		lastPingOKAt := s.LastPingOKAt
		s.mu.Unlock()

		age := 0
		if connectedSince > 0 {
			age = int(wallNow() - connectedSince)
		}
		lines = append(lines,
			fmt.Sprintf("tg_gateway_session_state{%s} %d", labels, stateVal),
			fmt.Sprintf("tg_gateway_session_connection_age_seconds{%s} %d", labels, age),
			fmt.Sprintf("tg_gateway_update_queue_depth{%s} %d", labels, queueDepth),
			fmt.Sprintf("tg_gateway_real_flood_wait_total{%s} %d", labels, realFlood),
			fmt.Sprintf("tg_gateway_synthetic_429_total{%s} %d", labels, synth),
			fmt.Sprintf("tg_gateway_consumers{%s} %d", labels, consumers),
			fmt.Sprintf("tg_gateway_send_paused{%s} %d", labels, b2i(sendPaused)),
			fmt.Sprintf("tg_gateway_session_reconnects_total{%s} %d", labels, reconnects),
			fmt.Sprintf("tg_gateway_session_start_attempts_total{%s} %d", labels, startAttempts),
			fmt.Sprintf("tg_gateway_session_terminal{%s} %d", labels, b2i(terminal)),
			fmt.Sprintf("tg_gateway_idempotency_claims_total{%s} %d", labels, idemClaims),
			fmt.Sprintf("tg_gateway_persist_failures_total{%s} %d", labels, persistFails),
		)
		if slo := s.DeployToFirstMessageS(); slo > 0 {
			lines = append(lines, fmt.Sprintf("tg_gateway_deploy_to_first_message_seconds{%s} %.2f", labels, slo))
		}
		if lastUpdateAt > 0 {
			lines = append(lines, fmt.Sprintf("tg_gateway_seconds_since_last_update{%s} %.1f", labels, wallNow()-lastUpdateAt))
		}
		if lastPingOKAt > 0 {
			lines = append(lines, fmt.Sprintf("tg_gateway_seconds_since_ping_ok{%s} %.1f", labels, monoNow()-lastPingOKAt))
		}
	}
	degraded := 0
	if a.Health.Degraded() != "" {
		degraded = 1
	}
	lines = append(lines,
		fmt.Sprintf("tg_gateway_persist_failures_total{plane=\"mt\"} %d", a.Health.PersistFailures),
		fmt.Sprintf("tg_gateway_corrupt_rows_total{plane=\"mt\"} %d", a.Health.CorruptRows),
		fmt.Sprintf("tg_gateway_quarantined_files_total{plane=\"mt\"} %d", a.Health.QuarantinedFiles),
		fmt.Sprintf("tg_gateway_storage_degraded{plane=\"mt\"} %d", degraded),
		fmt.Sprintf("tg_gateway_lock_reclaims_total{plane=\"mt\"} %d", lock.ReclaimsTotal()),
		fmt.Sprintf("tg_gateway_loop_lag_seconds{plane=\"mt\"} %v", round2(a.Beat.Lag())),
	)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}

// ── helpers ────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// maxMethodBody bounds method-call bodies: the allowlist is text
// (messages, markups, base64 media handles) — 10MB is generous and
// keeps a hostile client from ballooning memory.
const maxMethodBody = 10 << 20

func readAll(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	return io.ReadAll(http.MaxBytesReader(nil, r.Body, maxMethodBody))
}

func isTooLarge(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

// randSuffix returns n random lowercase-hex chars (consumer ids).
func randSuffix(n int) string {
	b := make([]byte, n)
	for i := range b {
		v, _ := rand.Int(rand.Reader, big.NewInt(16))
		b[i] = "0123456789abcdef"[v.Int64()]
	}
	return hex.EncodeToString(b)[:n]
}

func last4(s string) string {
	if len(s) <= 4 {
		return s
	}
	return s[len(s)-4:]
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }
