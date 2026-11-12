package mtproto

// User-account login: a stepwise state machine driven by the admin API
// (start → code → password?), executed one Telegram round-trip per step.
// The code and the 2FA password never reach a log line, an error
// message or the audit trail (same rules the scanner's auth enforced).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	tdsession "github.com/gotd/td/session"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

// ErrAwaitingLogin marks a user session that holds no authorization
// yet: not an error, the session waits for the admin login flow.
var ErrAwaitingLogin = errors.New("user session awaiting login")

// Login stages reported to the admin surface.
const (
	loginStageIdle     = "idle"            // no login started
	loginStageCodeSent = "code_sent"       // Telegram sent the code
	loginStagePassword = "password_needed" // 2FA required
	loginStageAuthed   = "authorized"      // done — supervisor connects
	loginStageFailed   = "failed"
)

// loginDriver performs the Telegram auth round-trips. Production runs
// each step on a fresh short-lived gotd connection (same persisted
// auth_key); tests drive a fake.
type loginDriver interface {
	// SendCode asks Telegram to send the login code; returns codeHash.
	SendCode(ctx context.Context, phone string) (codeHash string, err error)
	// SignIn submits the code. passwordNeeded=true means 2FA is on and
	// Password() must be called next.
	SignIn(ctx context.Context, phone, code, codeHash string) (passwordNeeded bool, err error)
	// Password submits the 2FA password (SRP).
	Password(ctx context.Context, password string) error
}

// loginCtl is one in-flight login: the stage machine plus the stored
// code hash. Steps are serialized — one Telegram auth call at a time.
type loginCtl struct {
	mu       sync.Mutex
	driver   loginDriver
	phone    string
	codeHash string
	stage    string
	lastErr  string
}

func newLoginCtl(driver loginDriver) *loginCtl {
	return &loginCtl{driver: driver, stage: loginStageIdle}
}

func (l *loginCtl) snapshot() (stage, phone, lastErr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stage, l.phone, l.lastErr
}

// Start sends the code for phone. Restarting a failed login is allowed;
// a login already past code_sent must be finished or failed first.
func (l *loginCtl) Start(ctx context.Context, phone string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.stage {
	case loginStageCodeSent, loginStagePassword, loginStageAuthed:
		return errors.New("login already in progress (stage " + l.stage + "); finish it or restart the session")
	}
	hash, err := l.driver.SendCode(ctx, phone)
	if err != nil {
		l.stage, l.lastErr = loginStageFailed, redactAuthErr(err)
		return err
	}
	l.phone, l.codeHash, l.stage, l.lastErr = phone, hash, loginStageCodeSent, ""
	return nil
}

// Code submits the login code. passwordRequired=true answers 409 with
// the next step (Password); false means the session is authorized.
func (l *loginCtl) Code(ctx context.Context, code string) (passwordRequired bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stage != loginStageCodeSent {
		return false, errors.New("no code expected at stage " + l.stage + "; call login/start first")
	}
	needed, err := l.driver.SignIn(ctx, l.phone, code, l.codeHash)
	if err != nil {
		l.stage, l.lastErr = loginStageFailed, redactAuthErr(err)
		return false, err
	}
	if needed {
		l.stage = loginStagePassword
		return true, nil
	}
	l.stage = loginStageAuthed
	return false, nil
}

// Password submits the 2FA password after a password_needed code step.
func (l *loginCtl) Password(ctx context.Context, password string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stage != loginStagePassword {
		return errors.New("no password expected at stage " + l.stage + "; submit the code first")
	}
	if err := l.driver.Password(ctx, password); err != nil {
		l.stage, l.lastErr = loginStageFailed, redactAuthErr(err)
		return err
	}
	l.stage, l.lastErr = loginStageAuthed, ""
	return nil
}

// redactAuthErr strips anything that could echo the code/password:
// gotd's auth errors never contain them, but a driver might wrap them —
// keep the class only.
func redactAuthErr(err error) string {
	msg := err.Error()
	if len(msg) > 160 {
		msg = msg[:160] + "…"
	}
	return msg
}

// ── Manager surface (admin endpoints call these) ───────────────────

// loginCtlFor returns (creating if needed) the login controller for a
// user session.
func (m *Manager) loginCtlFor(alias string) (*loginCtl, error) {
	s := m.Get(alias)
	if s == nil {
		return nil, errors.New("unknown session: " + alias)
	}
	if s.Kind != "user" {
		return nil, errors.New("session is a bot; user login applies to kind=user sessions")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.logins == nil {
		m.logins = map[string]*loginCtl{}
	}
	ctl, ok := m.logins[alias]
	if !ok {
		ctl = newLoginCtl(m.LoginDriverFactory(alias))
		m.logins[alias] = ctl
	}
	return ctl, nil
}

// LoginStart sends the login code for a user session.
func (m *Manager) LoginStart(ctx context.Context, alias, phone string) (string, error) {
	ctl, err := m.loginCtlFor(alias)
	if err != nil {
		return "", err
	}
	if err := ctl.Start(ctx, phone); err != nil {
		return "", err
	}
	stage, _, _ := ctl.snapshot()
	return stage, nil
}

// LoginCode submits the code. When the account has 2FA the caller gets
// passwordRequired=true and calls LoginPassword next.
func (m *Manager) LoginCode(ctx context.Context, alias, code string) (passwordRequired bool, stage string, err error) {
	ctl, err := m.loginCtlFor(alias)
	if err != nil {
		return false, "", err
	}
	needed, err := ctl.Code(ctx, code)
	if err != nil {
		return false, "", err
	}
	if !needed {
		m.loginAuthorized(alias)
	}
	stage, _, _ = ctl.snapshot()
	return needed, stage, nil
}

// LoginPassword submits the 2FA password.
func (m *Manager) LoginPassword(ctx context.Context, alias, password string) (stage string, err error) {
	ctl, err := m.loginCtlFor(alias)
	if err != nil {
		return "", err
	}
	if err := ctl.Password(ctx, password); err != nil {
		return "", err
	}
	m.loginAuthorized(alias)
	stage, _, _ = ctl.snapshot()
	return stage, nil
}

// LoginStage reports the admin-visible login state.
func (m *Manager) LoginStage(alias string) (stage, phone, lastErr string) {
	m.mu.Lock()
	ctl := m.logins[alias]
	m.mu.Unlock()
	if ctl == nil {
		return loginStageIdle, "", ""
	}
	return ctl.snapshot()
}

// loginAuthorized wakes the supervisor: the persisted auth_key is now
// authorized, so the next connect goes live.
func (m *Manager) loginAuthorized(alias string) {
	s := m.Get(alias)
	if s == nil {
		return
	}
	m.wakeAfterRelogin(s)
	slog.Info("user session authorized — supervisor will connect", "alias", alias)
}

// sessionTelethonDecode turns a Telethon StringSession into the gotd
// session JSON blob — the exact bytes session.Loader.Save produces, so
// the sealed storage opens it without translation.
func sessionTelethonDecode(hx string) ([]byte, error) {
	data, err := tdsession.TelethonSession(strings.TrimSpace(hx))
	if err != nil {
		// gotd's error does not echo the input; keep it that way
		return nil, fmt.Errorf("decode telethon string session: %w", err)
	}
	var mem memSessionStorage
	loader := tdsession.Loader{Storage: &mem}
	if err := loader.Save(context.Background(), data); err != nil {
		return nil, fmt.Errorf("encode imported session: %w", err)
	}
	return mem.buf, nil
}

// memSessionStorage captures what Loader.Save writes.
type memSessionStorage struct{ buf []byte }

func (m *memSessionStorage) LoadSession(context.Context) ([]byte, error) { return m.buf, nil }
func (m *memSessionStorage) StoreSession(_ context.Context, data []byte) error {
	m.buf = data
	return nil
}

// ── production driver: one short-lived gotd run per step ───────────

// gotdLoginDriver executes each auth step on its own telegram.Client
// over the same persisted session (sequential connections; the process
// holds the session lock throughout).
type gotdLoginDriver struct {
	cfg   config.MTProto
	alias string
	st    *store.MT
}

// newGotdLoginDriver builds the production driver.
func newGotdLoginDriver(cfg config.MTProto, alias string, st *store.MT) loginDriver {
	return &gotdLoginDriver{cfg: cfg, alias: alias, st: st}
}

// runAuthStep dials, runs fn inside the client run context, then hangs up.
func (d *gotdLoginDriver) runAuthStep(fn func(ctx context.Context, a *auth.Client) error) error {
	client := newTelegramClient(d.cfg, d.alias, d.st, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() {
		done <- client.Run(runCtx, func(rctx context.Context) error {
			err := fn(rctx, client.Auth())
			runCancel() // step done — hang up
			return err
		})
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *gotdLoginDriver) SendCode(ctx context.Context, phone string) (string, error) {
	var hash string
	err := d.runAuthStep(func(rctx context.Context, a *auth.Client) error {
		sent, err := a.SendCode(rctx, phone, auth.SendCodeOptions{})
		if err != nil {
			return err
		}
		if sc, ok := sent.(*tg.AuthSentCode); ok {
			hash = sc.PhoneCodeHash
		}
		if hash == "" {
			return errors.New("telegram returned no phone_code_hash")
		}
		return nil
	})
	return hash, err
}

func (d *gotdLoginDriver) SignIn(ctx context.Context, phone, code, codeHash string) (bool, error) {
	var passwordNeeded bool
	err := d.runAuthStep(func(rctx context.Context, a *auth.Client) error {
		_, err := a.SignIn(rctx, phone, code, codeHash)
		if errors.Is(err, auth.ErrPasswordAuthNeeded) {
			passwordNeeded = true
			return nil
		}
		return err
	})
	return passwordNeeded, err
}

func (d *gotdLoginDriver) Password(ctx context.Context, password string) error {
	return d.runAuthStep(func(rctx context.Context, a *auth.Client) error {
		_, err := a.Password(rctx, password)
		return err
	})
}
