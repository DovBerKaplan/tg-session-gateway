package envdetect

import (
	"context"
	"testing"
)

type fakeProbes struct {
	tls       TLSState
	skew      float64
	v4, v6    bool
	runtime   RuntimeKind
	companion CompanionFacts
}

func (f fakeProbes) ProbeTLS(context.Context, string, string) (TLSState, error) {
	return f.tls, nil
}
func (f fakeProbes) ProbeClockSkew(context.Context, string) (float64, error) { return f.skew, nil }
func (f fakeProbes) ProbeDNS(context.Context, string) (bool, bool, error)    { return f.v4, f.v6, nil }
func (f fakeProbes) ProbeRuntime() RuntimeKind                              { return f.runtime }
func (f fakeProbes) ProbeCompanion(context.Context, string) (CompanionFacts, error) {
	return f.companion, nil
}

func TestProberCollectNoNetwork(t *testing.T) {
	p := &Prober{
		TLS:       fakeProbes{tls: TLSValid},
		Clock:     fakeProbes{skew: -1.5},
		DNS:       fakeProbes{v4: true},
		Runtime:   fakeProbes{runtime: RuntimeK8s},
		Companion: fakeProbes{companion: CompanionFacts{URL: "http://companion:8081", Reachable: true}},
	}
	cfg := ExplicitConfig{
		EnvDetect:        "auto",
		Upstream:         DefaultCloudUpstream,
		UpstreamFallback: "http://companion:8081",
		Explicit:         ExplicitFlags{UpstreamFallback: true},
	}
	facts := p.Collect(context.Background(), cfg)
	if facts.TLS != TLSValid || facts.Runtime != RuntimeK8s {
		t.Fatalf("facts: %+v", facts)
	}
	if !facts.Network.DNSOK || !facts.Companion.Reachable {
		t.Fatalf("network/companion: %+v", facts)
	}
}

func TestDefaultRuntimeProber(t *testing.T) {
	r := defaultRuntimeProber{exists: func(path string) bool { return path == "/.dockerenv" }}
	if r.ProbeRuntime() != RuntimeDocker {
		t.Fatalf("expected docker")
	}
	r2 := defaultRuntimeProber{exists: func(string) bool { return false }}
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	if r2.ProbeRuntime() != RuntimeK8s {
		t.Fatalf("expected k8s")
	}
}
