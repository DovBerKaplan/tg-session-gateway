package envdetect

import "strings"

// ChosenSettings is the effective upstream configuration after detection.
type ChosenSettings struct {
	UpstreamOrder           []string
	Kind                    string // cloud | local
	MaxMultipartMB          int
	UpstreamTimeoutS        int
	RecommendComposeProfile string
	RecommendHelmMode       string // disabled | standby | always
}

// Decision is the output of the pure decision engine.
type Decision struct {
	Settings          ChosenSettings
	Reasons           []string
	ActionableMessage string
}

func cloudTLSDegraded(t TLSState) bool {
	switch t {
	case TLSValid:
		return false
	default:
		return true
	}
}

// Decide maps detected facts and explicit config to chosen settings.
// Explicit operator fields always win; TLS verification is never disabled;
// automatic fallback uses only a preconfigured UpstreamFallback URL.
func Decide(facts DetectedFacts, cfg ExplicitConfig) Decision {
	base := baselineSettings(cfg)
	var reasons []string

	detectOn := cfg.EnvDetect == "auto"
	if cfg.Explicit.EnvDetect {
		detectOn = cfg.EnvDetect == "auto"
	}

	if !detectOn {
		enforceExplicitUpstream(&base, cfg, facts)
		applyExplicitOverrides(&base, cfg)
		reasons = append(reasons, "GW_ENV_DETECT=off: using explicit/default settings only")
		return Decision{
			Settings:          base,
			Reasons:           reasons,
			ActionableMessage: BuildActionableMessage(facts, base, reasons),
		}
	}

	reasons = append(reasons, runtimeReason(facts.Runtime)...)

	if !facts.Network.DNSOK {
		reasons = append(reasons, "DNS lookup failed for "+facts.UpstreamHost)
	}
	if facts.Network.ProxySet {
		reasons = append(reasons, "HTTP(S)_PROXY is set; TLS still fully verified")
	}

	reasons = append(reasons, tlsReasons(facts)...)

	failover := cfg.UpstreamFailover
	if !cfg.Explicit.UpstreamFailover {
		if facts.Runtime == RuntimeK8s {
			failover = "auto"
		}
	}

	fallback := strings.TrimSpace(cfg.UpstreamFallback)
	hasFallback := cfg.Explicit.UpstreamFallback && fallback != ""

	primary := cfg.Upstream
	if primary == "" {
		primary = DefaultCloudUpstream
	}

	order := []string{primary}
	kind := resolveKind(cfg, facts, hasFallback)

	switch failover {
	case "prefer_local":
		if hasFallback && facts.Companion.Reachable {
			order = []string{fallback, primary}
			kind = "local"
			reasons = append(reasons, "prefer_local: companion healthy, trying local first")
		} else if hasFallback {
			reasons = append(reasons, "prefer_local: companion unreachable, cloud primary retained")
		}
	case "auto":
		if hasFallback && shouldFailover(facts) {
			order = []string{fallback, primary}
			kind = "local"
			reasons = append(reasons, "auto failover: cloud path degraded, using preconfigured fallback")
		}
	case "off":
		if hasFallback && shouldFailover(facts) {
			reasons = append(reasons, "cloud degraded but GW_UPSTREAM_FAILOVER=off: no automatic switch (fallback URL is configured)")
		}
	}

	base.UpstreamOrder = order
	base.Kind = kind
	base.RecommendComposeProfile = recommendCompose(facts, hasFallback)
	base.RecommendHelmMode = recommendHelm(facts, hasFallback)

	enforceExplicitUpstream(&base, cfg, facts)
	applyExplicitOverrides(&base, cfg)
	reasons = dedupeStrings(reasons)

	return Decision{
		Settings:          base,
		Reasons:           reasons,
		ActionableMessage: BuildActionableMessage(facts, base, reasons),
	}
}

func baselineSettings(cfg ExplicitConfig) ChosenSettings {
	up := cfg.Upstream
	if up == "" {
		up = DefaultCloudUpstream
	}
	kind := cfg.UpstreamKind
	if kind == "" {
		kind = "auto"
	}
	if kind == "auto" {
		kind = "cloud"
	}
	mb := cfg.MaxMultipartMB
	if mb <= 0 {
		mb = DefaultMaxMultipartMB
	}
	to := cfg.UpstreamTimeoutS
	if to <= 0 {
		to = DefaultUpstreamTimeoutS
	}
	return ChosenSettings{
		UpstreamOrder:     []string{up},
		Kind:              kind,
		MaxMultipartMB:    mb,
		UpstreamTimeoutS:  to,
		RecommendHelmMode: "disabled",
	}
}

func resolveKind(cfg ExplicitConfig, facts DetectedFacts, hasFallback bool) string {
	if cfg.Explicit.UpstreamKind && cfg.UpstreamKind != "auto" {
		return cfg.UpstreamKind
	}
	if hasFallback && facts.Companion.Reachable && cloudTLSDegraded(facts.TLS) {
		return "local"
	}
	if cfg.UpstreamKind == "local" {
		return "local"
	}
	return "cloud"
}

func shouldFailover(facts DetectedFacts) bool {
	if !cloudTLSDegraded(facts.TLS) {
		return false
	}
	if facts.TLS == TLSExpired && abs(facts.ClockSkewS) >= clockSkewThresholdS {
		return false
	}
	return true
}

func tlsReasons(facts DetectedFacts) []string {
	var out []string
	switch facts.TLS {
	case TLSValid:
		out = append(out, "cloud TLS: valid")
	case TLSExpired:
		if abs(facts.ClockSkewS) >= clockSkewThresholdS {
			out = append(out, formatClockSkewCert(facts))
		} else {
			out = append(out, "cloud TLS: certificate expired (clock skew "+formatSkew(facts.ClockSkewS)+")")
		}
	case TLSUnknownCA:
		out = append(out, "cloud TLS: unknown CA — add GW_UPSTREAM_CA_FILE or fix trust store (never skip verify)")
	case TLSUnexpectedIssuer:
		out = append(out, "cloud TLS: unexpected issuer — possible MITM or SSL inspection; report only, no auto-trust")
	case TLSHandshakeFail:
		out = append(out, "cloud TLS: handshake failure")
	case TLSUnreachable:
		out = append(out, "cloud TLS: upstream unreachable")
	}
	return out
}

func runtimeReason(r RuntimeKind) []string {
	switch r {
	case RuntimeK8s:
		return []string{"runtime: kubernetes"}
	case RuntimeDocker:
		return []string{"runtime: docker"}
	default:
		return []string{"runtime: bare metal"}
	}
}

func recommendCompose(facts DetectedFacts, hasFallback bool) string {
	if facts.Runtime == RuntimeDocker && hasFallback {
		return "local-botapi"
	}
	return ""
}

func recommendHelm(facts DetectedFacts, hasFallback bool) string {
	if !hasFallback {
		return "disabled"
	}
	switch facts.Runtime {
	case RuntimeK8s:
		return "standby"
	case RuntimeDocker:
		return "disabled"
	default:
		return "disabled"
	}
}

func enforceExplicitUpstream(s *ChosenSettings, cfg ExplicitConfig, facts DetectedFacts) {
	primary := cfg.Upstream
	if primary == "" {
		primary = DefaultCloudUpstream
	}
	if cfg.Explicit.UpstreamFailover && cfg.UpstreamFailover == "off" {
		s.UpstreamOrder = []string{primary}
		return
	}
	if !cfg.Explicit.Upstream {
		return
	}
	if len(s.UpstreamOrder) == 0 {
		s.UpstreamOrder = []string{primary}
		return
	}
	if s.UpstreamOrder[0] == primary {
		return
	}
	// Explicit primary always wins over automatic prefer_local / failover ordering.
	rest := make([]string, 0, len(s.UpstreamOrder))
	for _, u := range s.UpstreamOrder {
		if u == primary {
			continue
		}
		rest = append(rest, u)
	}
	s.UpstreamOrder = append([]string{primary}, rest...)
}

func applyExplicitOverrides(s *ChosenSettings, cfg ExplicitConfig) {
	if cfg.Explicit.UpstreamKind && cfg.UpstreamKind != "" && cfg.UpstreamKind != "auto" {
		s.Kind = cfg.UpstreamKind
	}
	if cfg.Explicit.MaxMultipartMB && cfg.MaxMultipartMB > 0 {
		s.MaxMultipartMB = cfg.MaxMultipartMB
	}
	if cfg.Explicit.UpstreamTimeoutS && cfg.UpstreamTimeoutS > 0 {
		s.UpstreamTimeoutS = cfg.UpstreamTimeoutS
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
