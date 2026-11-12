package envdetect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// TLSProber classifies TLS to an HTTPS upstream.
type TLSProber interface {
	ProbeTLS(ctx context.Context, upstream, caFile string) (TLSState, error)
}

// ClockProber measures clock skew via the upstream Date header.
type ClockProber interface {
	ProbeClockSkew(ctx context.Context, upstream string) (float64, error)
}

// DNSProber checks name resolution for the upstream host.
type DNSProber interface {
	ProbeDNS(ctx context.Context, host string) (ipv4, ipv6 bool, err error)
}

// RuntimeProber detects docker / kubernetes / bare metal.
type RuntimeProber interface {
	ProbeRuntime() RuntimeKind
}

// CompanionProber checks the local companion HTTP health endpoint.
type CompanionProber interface {
	ProbeCompanion(ctx context.Context, companionURL string) (CompanionFacts, error)
}

// FileExists checks for runtime marker files (e.g. /.dockerenv).
type FileExists func(path string) bool

// Prober bundles injectable probe implementations for tests.
type Prober struct {
	TLS       TLSProber
	Clock     ClockProber
	DNS       DNSProber
	Runtime   RuntimeProber
	Companion CompanionProber
}

// DefaultProber returns production probes using the stdlib network stack.
func DefaultProber() *Prober {
	return &Prober{
		TLS:       defaultTLSProber{},
		Clock:     defaultClockProber{hc: &http.Client{Timeout: 15 * time.Second}},
		DNS:       defaultDNSProber{},
		Runtime:   defaultRuntimeProber{exists: osStatExists},
		Companion: defaultCompanionProber{hc: &http.Client{Timeout: 5 * time.Second}},
	}
}

// Collect runs all probes and assembles DetectedFacts for cfg's upstream target.
func (p *Prober) Collect(ctx context.Context, cfg ExplicitConfig) DetectedFacts {
	upstream := cfg.Upstream
	if upstream == "" {
		upstream = DefaultCloudUpstream
	}
	host := hostFromURL(upstream)

	var facts DetectedFacts
	facts.UpstreamHost = host

	if p.TLS != nil {
		st, _ := p.TLS.ProbeTLS(ctx, upstream, cfg.UpstreamCAFile)
		facts.TLS = st
	}
	if p.Clock != nil {
		skew, _ := p.Clock.ProbeClockSkew(ctx, upstream)
		facts.ClockSkewS = skew
	}
	if p.DNS != nil {
		v4, v6, _ := p.DNS.ProbeDNS(ctx, host)
		facts.Network.IPv4OK = v4
		facts.Network.IPv6OK = v6
		facts.Network.DNSOK = v4 || v6
	}
	facts.Network.HTTPProxy = os.Getenv("HTTP_PROXY")
	if facts.Network.HTTPProxy == "" {
		facts.Network.HTTPProxy = os.Getenv("http_proxy")
	}
	facts.Network.HTTPSProxy = os.Getenv("HTTPS_PROXY")
	if facts.Network.HTTPSProxy == "" {
		facts.Network.HTTPSProxy = os.Getenv("https_proxy")
	}
	facts.Network.NoProxy = os.Getenv("NO_PROXY")
	if facts.Network.NoProxy == "" {
		facts.Network.NoProxy = os.Getenv("no_proxy")
	}
	facts.Network.ProxySet = facts.Network.HTTPProxy != "" || facts.Network.HTTPSProxy != ""

	if p.Runtime != nil {
		facts.Runtime = p.Runtime.ProbeRuntime()
	}
	if p.Companion != nil && cfg.UpstreamFallback != "" {
		cf, _ := p.Companion.ProbeCompanion(ctx, cfg.UpstreamFallback)
		facts.Companion = cf
	}
	return facts
}

func hostFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
	}
	host, _, _ := net.SplitHostPort(u.Host)
	if host == "" {
		host = u.Host
	}
	return host
}

func osStatExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type defaultTLSProber struct{}

func (defaultTLSProber) ProbeTLS(ctx context.Context, upstream, caFile string) (TLSState, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return TLSUnreachable, err
	}
	if u.Scheme != "https" {
		return TLSValid, nil
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(host, port)

	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return TLSUnknownCA, err
		}
		if ok := roots.AppendCertsFromPEM(pem); !ok {
			return TLSUnknownCA, fmt.Errorf("no certificates in %s", caFile)
		}
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
		ServerName: host,
		RootCAs:    roots,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		return classifyTLSError(err), err
	}
	defer conn.Close()

	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return TLSHandshakeFail, errors.New("no peer certificates")
	}
	leaf := certs[0]
	if err := leaf.VerifyHostname(host); err != nil {
		if isExpired(leaf) {
			return TLSExpired, err
		}
		return TLSHandshakeFail, err
	}
	opts := x509.VerifyOptions{DNSName: host, Roots: roots}
	if _, err := leaf.Verify(opts); err != nil {
		return classifyTLSError(err), err
	}
	if unexpectedTelegramIssuer(leaf) {
		return TLSUnexpectedIssuer, nil
	}
	return TLSValid, nil
}

func classifyTLSError(err error) TLSState {
	var ua x509.UnknownAuthorityError
	var certInvalid x509.CertificateInvalidError
	if errors.As(err, &ua) {
		return TLSUnknownCA
	}
	if errors.As(err, &certInvalid) {
		if certInvalid.Reason == x509.Expired {
			return TLSExpired
		}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return TLSUnreachable
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "certificate has expired"), strings.Contains(msg, "expired"):
		return TLSExpired
	case strings.Contains(msg, "unknown authority"), strings.Contains(msg, "untrusted"):
		return TLSUnknownCA
	case strings.Contains(msg, "handshake"):
		return TLSHandshakeFail
	default:
		return TLSUnreachable
	}
}

func isExpired(c *x509.Certificate) bool {
	now := time.Now()
	return now.Before(c.NotBefore) || now.After(c.NotAfter)
}

// unexpectedTelegramIssuer flags likely MITM when chain verifies but issuer
// is not from the usual public CAs serving api.telegram.org.
func unexpectedTelegramIssuer(c *x509.Certificate) bool {
	o := strings.ToLower(strings.Join(c.Issuer.Organization, " "))
	known := []string{"let's encrypt", "google trust", "digicert", "globalsign", "sectigo", "comodo", "amazon", "cloudflare"}
	for _, k := range known {
		if strings.Contains(o, k) {
			return false
		}
	}
	return o != ""
}

type defaultClockProber struct {
	hc *http.Client
}

func (p defaultClockProber) ProbeClockSkew(ctx context.Context, upstream string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, upstream, nil)
	if err != nil {
		return 0, err
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	d := resp.Header.Get("Date")
	if d == "" {
		return 0, errors.New("missing Date header")
	}
	ts, err := http.ParseTime(d)
	if err != nil {
		return 0, err
	}
	return ts.Sub(time.Now()).Seconds(), nil
}

type defaultDNSProber struct{}

func (defaultDNSProber) ProbeDNS(ctx context.Context, host string) (bool, bool, error) {
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return false, false, err
	}
	var v4, v6 bool
	for _, ip := range ips {
		if ip.To4() != nil {
			v4 = true
		} else {
			v6 = true
		}
	}
	return v4, v6, nil
}

type defaultRuntimeProber struct {
	exists FileExists
}

func (r defaultRuntimeProber) ProbeRuntime() RuntimeKind {
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return RuntimeK8s
	}
	if r.exists != nil && r.exists("/.dockerenv") {
		return RuntimeDocker
	}
	return RuntimeBare
}

type defaultCompanionProber struct {
	hc *http.Client
}

func (p defaultCompanionProber) ProbeCompanion(ctx context.Context, companionURL string) (CompanionFacts, error) {
	f := CompanionFacts{URL: companionURL}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(companionURL, "/")+"/healthz", nil)
	if err != nil {
		return f, err
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return f, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	f.StatusCode = resp.StatusCode
	f.Reachable = resp.StatusCode >= 200 && resp.StatusCode < 300
	f.DiskFreeMB, f.MemAvailMB = readProcMemDiskHints()
	return f, nil
}

// readProcMemDiskHints best-effort from /proc when present (common in containers).
func readProcMemDiskHints() (diskMB, memMB int64) {
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "MemAvailable:") {
				var kb int64
				if _, err := fmt.Sscanf(line, "MemAvailable: %d kB", &kb); err == nil {
					memMB = kb / 1024
				}
				break
			}
		}
	}
	var st os.FileInfo
	if fi, err := os.Stat("/data"); err == nil {
		st = fi
	} else if fi, err := os.Stat("/"); err == nil {
		st = fi
	}
	_ = st
	return diskMB, memMB
}
