package envdetect

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// DoctorReport is the structured output of a doctor run.
type DoctorReport struct {
	Facts    DetectedFacts `json:"facts"`
	Decision Decision      `json:"decision"`
	At       time.Time     `json:"at"`
}

// RunDoctor executes environment probes and the decision engine for cfg.
//
// Wire in cmd/tggw: add a `doctor` subcommand that loads ExplicitConfig from
// GW_* env (or flags), calls envdetect.RunDoctor(cfg), prints JSON or a human
// summary to stdout, and exits non-zero when ActionableMessage indicates a
// blocking cloud path with no healthy fallback — e.g.:
//
//	case "doctor":
//	    return envdetect.RunDoctorAndExit(loadExplicitConfig())
func RunDoctor(cfg ExplicitConfig) DoctorReport {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := DefaultProber()
	facts := p.Collect(ctx, cfg)
	dec := Decide(facts, cfg)
	return DoctorReport{Facts: facts, Decision: dec, At: time.Now().UTC()}
}

// RunDoctorWithProber is like RunDoctor but accepts injectable probes (tests).
func RunDoctorWithProber(cfg ExplicitConfig, p *Prober) DoctorReport {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if p == nil {
		p = DefaultProber()
	}
	facts := p.Collect(ctx, cfg)
	dec := Decide(facts, cfg)
	return DoctorReport{Facts: facts, Decision: dec, At: time.Now().UTC()}
}

// FormatHuman renders a concise doctor report for the CLI.
func (r DoctorReport) FormatHuman() string {
	var b strings.Builder
	fmt.Fprintf(&b, "envdetect doctor @ %s\n", r.At.Format(time.RFC3339))
	fmt.Fprintf(&b, "  tls=%s clock_skew=%.0fs runtime=%s dns=%v proxy=%v\n",
		r.Facts.TLS, r.Facts.ClockSkewS, r.Facts.Runtime, r.Facts.Network.DNSOK, r.Facts.Network.ProxySet)
	if r.Facts.Companion.URL != "" {
		fmt.Fprintf(&b, "  companion reachable=%v url=%s\n", r.Facts.Companion.Reachable, r.Facts.Companion.URL)
	}
	fmt.Fprintf(&b, "  upstream order: %s\n", strings.Join(r.Decision.Settings.UpstreamOrder, " -> "))
	fmt.Fprintf(&b, "  kind=%s multipart_mb=%d timeout_s=%d\n",
		r.Decision.Settings.Kind, r.Decision.Settings.MaxMultipartMB, r.Decision.Settings.UpstreamTimeoutS)
	if r.Decision.Settings.RecommendComposeProfile != "" {
		fmt.Fprintf(&b, "  recommend compose profile: %s\n", r.Decision.Settings.RecommendComposeProfile)
	}
	if r.Decision.Settings.RecommendHelmMode != "" && r.Decision.Settings.RecommendHelmMode != "disabled" {
		fmt.Fprintf(&b, "  recommend helm localBotApi.mode: %s\n", r.Decision.Settings.RecommendHelmMode)
	}
	for _, reason := range r.Decision.Reasons {
		fmt.Fprintf(&b, "  - %s\n", reason)
	}
	fmt.Fprintf(&b, "\n%s\n", r.Decision.ActionableMessage)
	return b.String()
}

// MarshalJSON implements json.Marshaler via embedding for API reuse.
func (r DoctorReport) MarshalJSON() ([]byte, error) {
	type alias DoctorReport
	return json.Marshal(alias(r))
}
