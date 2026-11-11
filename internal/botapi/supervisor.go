package botapi

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/envdetect"
)

// UpstreamSupervisor owns the active upstream base URL, failover, and
// optional env-detect re-probing. It wraps UpstreamClient so Proxy keeps
// using HTTPClient.Do unchanged.
type UpstreamSupervisor struct {
	mu sync.RWMutex

	cfg      config.BotAPI
	client   *UpstreamClient
	primary  string
	fallback string

	active   atomic.Value // string
	failover atomic.Bool  // true when on fallback
	lastErr  atomic.Value // string
	stopCh   chan struct{}
	once     sync.Once

	// env-detect snapshot for admin/metrics
	lastDecision atomic.Value // envdetect.Decision
	lastFacts    atomic.Value // envdetect.DetectedFacts
	tlsState     atomic.Value // string
	clockSkewS   atomic.Int64
	envRuntime   atomic.Value // string
}

// NewUpstreamSupervisor builds the live client and optional failover loop.
func NewUpstreamSupervisor(cfg config.BotAPI) (*UpstreamSupervisor, error) {
	timeout := time.Duration(cfg.UpstreamTimeoutS) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	client := NewUpstreamClientOpts(cfg.Upstream, UpstreamClientOptions{
		Timeout:  timeout,
		CAFile:   cfg.UpstreamCAFile,
		EnableH2: true,
	})
	s := &UpstreamSupervisor{
		cfg:      cfg,
		client:   client,
		primary:  strings.TrimRight(cfg.Upstream, "/"),
		fallback: strings.TrimRight(cfg.UpstreamFallback, "/"),
		stopCh:   make(chan struct{}),
	}
	s.active.Store(s.primary)
	s.lastErr.Store("")
	s.tlsState.Store(string(envdetect.TLSUnreachable))
	s.envRuntime.Store(string(envdetect.RuntimeBare))
	s.lastDecision.Store(envdetect.Decision{})
	s.lastFacts.Store(envdetect.DetectedFacts{})

	if cfg.EnvDetect == "auto" || cfg.UpstreamFailover == "auto" || cfg.UpstreamFailover == "prefer_local" {
		s.applyEnvDetect(context.Background())
	}
	return s, nil
}

// Client returns the underlying UpstreamClient (mutable base).
func (s *UpstreamSupervisor) Client() *UpstreamClient { return s.client }

// Do implements HTTPClient.
func (s *UpstreamSupervisor) Do(req *httpRequest) (*httpResponse, error) {
	resp, err := s.client.Do(req)
	if err != nil && isTLSOrTransportErr(err) && s.maybeFailover(err) {
		return s.client.Do(req)
	}
	return resp, err
}

// DoStream implements StreamHTTPClient.
func (s *UpstreamSupervisor) DoStream(req *httpRequest) (*httpResponse, error) {
	resp, err := s.client.DoStream(req)
	if err != nil && isTLSOrTransportErr(err) && s.maybeFailover(err) {
		return s.client.DoStream(req)
	}
	return resp, err
}

func isTLSOrTransportErr(err error) bool {
	if err == nil {
		return false
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"x509:", "certificate", "tls:", "handshake", "connection refused",
		"no such host", "i/o timeout", "network is unreachable",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func (s *UpstreamSupervisor) maybeFailover(err error) bool {
	if s.fallback == "" {
		s.lastErr.Store(err.Error())
		return false
	}
	mode := s.cfg.UpstreamFailover
	if mode == "off" && s.cfg.EnvDetect != "auto" {
		s.lastErr.Store(err.Error())
		return false
	}
	if s.failover.Load() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failover.Load() {
		return false
	}
	s.client.SetBase(s.fallback)
	s.active.Store(s.fallback)
	s.failover.Store(true)
	s.lastErr.Store(err.Error())
	s.cfg.ApplyLocalPreset()
	slog.Warn("upstream failover to local companion",
		"reason", err.Error(),
		"fallback", s.fallback)
	return true
}

// ForceFailover switches to the preconfigured fallback (admin).
func (s *UpstreamSupervisor) ForceFailover(reason string) error {
	if s.fallback == "" {
		return errors.New("GW_UPSTREAM_FALLBACK is not configured")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client.SetBase(s.fallback)
	s.active.Store(s.fallback)
	s.failover.Store(true)
	s.lastErr.Store(reason)
	s.cfg.ApplyLocalPreset()
	slog.Warn("admin forced upstream failover", "fallback", s.fallback, "reason", reason)
	return nil
}

// Failback switches back to the cloud primary after a probe succeeds.
func (s *UpstreamSupervisor) Failback() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client.SetBase(s.primary)
	s.active.Store(s.primary)
	s.failover.Store(false)
	s.lastErr.Store("")
	slog.Info("upstream failback to primary", "primary", s.primary)
	return nil
}

// Status is the admin/metrics snapshot.
func (s *UpstreamSupervisor) Status() map[string]any {
	active, _ := s.active.Load().(string)
	lastErr, _ := s.lastErr.Load().(string)
	tlsSt, _ := s.tlsState.Load().(string)
	rt, _ := s.envRuntime.Load().(string)
	dec, _ := s.lastDecision.Load().(envdetect.Decision)
	facts, _ := s.lastFacts.Load().(envdetect.DetectedFacts)
	return map[string]any{
		"primary":           s.primary,
		"fallback":          s.fallback,
		"active":            active,
		"failover_active":   s.failover.Load(),
		"last_error":        lastErr,
		"kind":              s.cfg.EffectiveKind(),
		"tls_state":         tlsSt,
		"clock_skew_seconds": s.clockSkewS.Load(),
		"env_runtime":       rt,
		"env_detect":        s.cfg.EnvDetect,
		"decision": map[string]any{
			"settings":            dec.Settings,
			"reasons":             dec.Reasons,
			"actionable_message":  dec.ActionableMessage,
		},
		"facts": facts,
	}
}

// StartLoop begins low-frequency re-probing when env-detect or auto failover is on.
func (s *UpstreamSupervisor) StartLoop() {
	s.once.Do(func() {
		if s.cfg.EnvDetect != "auto" && s.cfg.UpstreamFailover != "auto" {
			return
		}
		go func() {
			t := time.NewTicker(60 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-s.stopCh:
					return
				case <-t.C:
					s.applyEnvDetect(context.Background())
					s.probeFailback()
				}
			}
		}()
	})
}

// Stop ends the probe loop.
func (s *UpstreamSupervisor) Stop() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
}

func (s *UpstreamSupervisor) applyEnvDetect(ctx context.Context) {
	ecfg := envdetect.ExplicitConfig{
		EnvDetect:        s.cfg.EnvDetect,
		Upstream:         s.primary,
		UpstreamFallback: s.fallback,
		UpstreamKind:     s.cfg.UpstreamKind,
		UpstreamFailover: s.cfg.UpstreamFailover,
		MaxMultipartMB:   s.cfg.MaxMultipartMB,
		UpstreamTimeoutS: s.cfg.UpstreamTimeoutS,
		PathPrefix:       s.cfg.PathPrefix,
		UpstreamCAFile:   s.cfg.UpstreamCAFile,
		Explicit: envdetect.ExplicitFlags{
			EnvDetect:          true,
			Upstream:           true,
			UpstreamFallback:   s.fallback != "",
			UpstreamKind:       true,
			UpstreamFailover:   true,
			MaxMultipartMB:     true,
			UpstreamTimeoutS:   true,
			PathPrefix:         true,
			UpstreamCAFile:     s.cfg.UpstreamCAFile != "",
		},
	}
	_ = ctx
	report := envdetect.RunDoctor(ecfg)
	s.lastFacts.Store(report.Facts)
	s.lastDecision.Store(report.Decision)
	s.tlsState.Store(string(report.Facts.TLS))
	s.clockSkewS.Store(int64(report.Facts.ClockSkewS))
	s.envRuntime.Store(string(report.Facts.Runtime))

	if s.cfg.EnvDetect != "auto" {
		return
	}
	order := report.Decision.Settings.UpstreamOrder
	if len(order) == 0 {
		return
	}
	want := strings.TrimRight(order[0], "/")
	cur, _ := s.active.Load().(string)
	if want != cur {
		s.mu.Lock()
		s.client.SetBase(want)
		s.active.Store(want)
		onFallback := s.fallback != "" && want == s.fallback
		s.failover.Store(onFallback)
		if onFallback {
			s.cfg.ApplyLocalPreset()
		}
		s.mu.Unlock()
		slog.Warn("envdetect switched active upstream",
			"active", want,
			"message", report.Decision.ActionableMessage,
			"reasons", report.Decision.Reasons)
	}
}

func (s *UpstreamSupervisor) probeFailback() {
	if !s.failover.Load() || s.cfg.UpstreamFailover == "prefer_local" {
		return
	}
	// cheap HEAD/GET against primary getMe-shaped path without a token — just TLS dial
	u := s.primary
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
	_ = s.Failback()
}

// RedactToken masks a bot token for logs/metrics labels.
func RedactToken(token string) string {
	if token == "" {
		return ""
	}
	if i := strings.IndexByte(token, ':'); i > 0 && i+1 < len(token) {
		rest := token[i+1:]
		if len(rest) <= 4 {
			return token[:i] + ":***"
		}
		return token[:i] + ":" + rest[:2] + "***" + rest[len(rest)-2:]
	}
	if len(token) <= 8 {
		return "***"
	}
	return token[:4] + "***" + token[len(token)-2:]
}
