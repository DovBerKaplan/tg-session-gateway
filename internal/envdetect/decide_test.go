package envdetect

import (
	"strings"
	"testing"
)

func TestDecide(t *testing.T) {
	cloud := DefaultCloudUpstream
	local := "http://telegram-bot-api:8081"

	tests := []struct {
		name       string
		facts      DetectedFacts
		cfg        ExplicitConfig
		wantFirst  string
		wantKind   string
		wantSubstr []string
		noFailover bool
	}{
		{
			name: "clock_skew_vs_expired_cert",
			facts: DetectedFacts{
				UpstreamHost: "api.telegram.org",
				TLS:          TLSExpired,
				ClockSkewS:   4000,
				Network:      NetworkFacts{DNSOK: true},
				Runtime:      RuntimeDocker,
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         cloud,
				UpstreamFallback: local,
				UpstreamFailover: "auto",
				Explicit:         ExplicitFlags{UpstreamFallback: true, UpstreamFailover: true},
			},
			wantFirst:  cloud,
			wantKind:   "cloud",
			wantSubstr: []string{"clock skew", "likely host clock"},
			noFailover: true,
		},
		{
			name: "real_expired_cert_failover",
			facts: DetectedFacts{
				UpstreamHost: "api.telegram.org",
				TLS:          TLSExpired,
				ClockSkewS:   2,
				Network:      NetworkFacts{DNSOK: true},
				Runtime:      RuntimeDocker,
				Companion:    CompanionFacts{URL: local, Reachable: true},
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         cloud,
				UpstreamFallback: local,
				UpstreamFailover: "auto",
				Explicit:         ExplicitFlags{UpstreamFallback: true, UpstreamFailover: true},
			},
			wantFirst:  local,
			wantKind:   "local",
			wantSubstr: []string{"auto failover", "certificate expired"},
		},
		{
			name: "mitm_unexpected_issuer",
			facts: DetectedFacts{
				TLS:     TLSUnexpectedIssuer,
				Network: NetworkFacts{DNSOK: true},
				Runtime: RuntimeBare,
			},
			cfg: ExplicitConfig{
				EnvDetect: "auto",
				Upstream:  cloud,
			},
			wantFirst:  cloud,
			wantKind:   "cloud",
			wantSubstr: []string{"unexpected issuer", "MITM"},
		},
		{
			name: "no_dns",
			facts: DetectedFacts{
				UpstreamHost: "api.telegram.org",
				TLS:          TLSUnreachable,
				Network:      NetworkFacts{DNSOK: false},
				Runtime:      RuntimeBare,
			},
			cfg: ExplicitConfig{
				EnvDetect: "auto",
				Upstream:  cloud,
			},
			wantFirst:  cloud,
			wantSubstr: []string{"DNS lookup failed", "no DNS"},
		},
		{
			name: "proxy_set",
			facts: DetectedFacts{
				TLS:     TLSValid,
				Network: NetworkFacts{DNSOK: true, ProxySet: true, HTTPSProxy: "http://proxy.corp:8080"},
				Runtime: RuntimeDocker,
			},
			cfg: ExplicitConfig{
				EnvDetect: "auto",
				Upstream:  cloud,
			},
			wantFirst:  cloud,
			wantSubstr: []string{"PROXY", "fully verified"},
		},
		{
			name: "k8s_runtime_default_auto_failover",
			facts: DetectedFacts{
				TLS:       TLSHandshakeFail,
				Network:   NetworkFacts{DNSOK: true},
				Runtime:   RuntimeK8s,
				Companion: CompanionFacts{URL: local, Reachable: true},
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         cloud,
				UpstreamFallback: local,
				Explicit:         ExplicitFlags{UpstreamFallback: true},
			},
			wantFirst:  local,
			wantKind:   "local",
			wantSubstr: []string{"kubernetes", "auto failover"},
		},
		{
			name: "docker_recommends_compose_profile",
			facts: DetectedFacts{
				TLS:     TLSValid,
				Network: NetworkFacts{DNSOK: true},
				Runtime: RuntimeDocker,
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         cloud,
				UpstreamFallback: local,
				Explicit:         ExplicitFlags{UpstreamFallback: true},
			},
			wantFirst: cloud,
			wantKind:  "cloud",
		},
		{
			name: "companion_down_prefer_local",
			facts: DetectedFacts{
				TLS:       TLSValid,
				Network:   NetworkFacts{DNSOK: true},
				Runtime:   RuntimeDocker,
				Companion: CompanionFacts{URL: local, Reachable: false},
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         cloud,
				UpstreamFallback: local,
				UpstreamFailover: "prefer_local",
				Explicit:         ExplicitFlags{UpstreamFallback: true, UpstreamFailover: true},
			},
			wantFirst:  cloud,
			wantSubstr: []string{"companion unreachable"},
		},
		{
			name: "companion_up_prefer_local",
			facts: DetectedFacts{
				TLS:       TLSValid,
				Network:   NetworkFacts{DNSOK: true},
				Runtime:   RuntimeDocker,
				Companion: CompanionFacts{URL: local, Reachable: true},
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         cloud,
				UpstreamFallback: local,
				UpstreamFailover: "prefer_local",
				Explicit:         ExplicitFlags{UpstreamFallback: true, UpstreamFailover: true},
			},
			wantFirst:  local,
			wantKind:   "local",
			wantSubstr: []string{"prefer_local", "companion healthy"},
		},
		{
			name: "explicit_upstream_wins",
			facts: DetectedFacts{
				TLS:       TLSUnreachable,
				Network:   NetworkFacts{DNSOK: true},
				Runtime:   RuntimeK8s,
				Companion: CompanionFacts{URL: local, Reachable: true},
			},
			cfg: ExplicitConfig{
				EnvDetect:        "auto",
				Upstream:         "https://custom.example/",
				UpstreamFallback: local,
				UpstreamFailover: "auto",
				Explicit: ExplicitFlags{
					Upstream:         true,
					UpstreamFallback: true,
					UpstreamFailover: true,
					MaxMultipartMB:   true,
				},
				MaxMultipartMB: 200,
			},
			wantFirst: "https://custom.example/",
			wantKind:  "local",
		},
		{
			name: "env_detect_off",
			facts: DetectedFacts{
				TLS:     TLSUnreachable,
				Runtime: RuntimeK8s,
			},
			cfg: ExplicitConfig{
				EnvDetect: "off",
				Upstream:  cloud,
			},
			wantFirst:  cloud,
			wantSubstr: []string{"GW_ENV_DETECT=off"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := Decide(tt.facts, tt.cfg)
			if len(dec.Settings.UpstreamOrder) == 0 {
				t.Fatal("empty upstream order")
			}
			if dec.Settings.UpstreamOrder[0] != tt.wantFirst {
				t.Errorf("first upstream: got %q want %q (order=%v)", dec.Settings.UpstreamOrder[0], tt.wantFirst, dec.Settings.UpstreamOrder)
			}
			if tt.wantKind != "" && dec.Settings.Kind != tt.wantKind {
				t.Errorf("kind: got %q want %q", dec.Settings.Kind, tt.wantKind)
			}
			if tt.noFailover && len(dec.Settings.UpstreamOrder) > 1 {
				t.Errorf("expected no failover, order=%v", dec.Settings.UpstreamOrder)
			}
			blob := strings.Join(append(dec.Reasons, dec.ActionableMessage), " ")
			for _, sub := range tt.wantSubstr {
				if !strings.Contains(strings.ToLower(blob), strings.ToLower(sub)) {
					t.Errorf("missing substring %q in %q", sub, blob)
				}
			}
			if tt.name == "docker_recommends_compose_profile" {
				if dec.Settings.RecommendComposeProfile != "local-botapi" {
					t.Errorf("compose profile: got %q", dec.Settings.RecommendComposeProfile)
				}
			}
			if tt.cfg.Explicit.MaxMultipartMB && dec.Settings.MaxMultipartMB != tt.cfg.MaxMultipartMB {
				t.Errorf("max multipart: got %d want %d", dec.Settings.MaxMultipartMB, tt.cfg.MaxMultipartMB)
			}
			if strings.Contains(blob, "skip verify") || strings.Contains(blob, "InsecureSkipVerify") {
				t.Error("must never suggest disabling TLS verification")
			}
		})
	}
}

func TestDecideDeterministic(t *testing.T) {
	facts := DetectedFacts{
		TLS:     TLSValid,
		Network: NetworkFacts{DNSOK: true},
		Runtime: RuntimeDocker,
	}
	cfg := DefaultExplicitConfig()
	cfg.EnvDetect = "auto"
	a := Decide(facts, cfg)
	b := Decide(facts, cfg)
	if a.Settings.UpstreamOrder[0] != b.Settings.UpstreamOrder[0] || a.Settings.Kind != b.Settings.Kind {
		t.Fatalf("non-deterministic: %+v vs %+v", a, b)
	}
}
