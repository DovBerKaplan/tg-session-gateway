// Package envdetect probes the host environment and chooses safe upstream
// settings without weakening TLS verification.
package envdetect

// TLSState classifies the last TLS probe to the cloud upstream.
type TLSState string

const (
	TLSValid             TLSState = "valid"
	TLSExpired           TLSState = "expired"
	TLSUnknownCA         TLSState = "unknown_ca"
	TLSHandshakeFail     TLSState = "handshake_fail"
	TLSUnexpectedIssuer  TLSState = "unexpected_issuer"
	TLSUnreachable       TLSState = "unreachable"
)

// RuntimeKind is where the gateway process is running.
type RuntimeKind string

const (
	RuntimeDocker RuntimeKind = "docker"
	RuntimeK8s    RuntimeKind = "k8s"
	RuntimeBare   RuntimeKind = "bare"
)

// NetworkFacts holds outbound connectivity signals.
type NetworkFacts struct {
	DNSOK      bool
	IPv4OK     bool
	IPv6OK     bool
	ProxySet   bool
	HTTPProxy  string
	HTTPSProxy string
	NoProxy    string
}

// CompanionFacts is health of the optional local Bot API companion.
type CompanionFacts struct {
	URL        string
	Reachable  bool
	DiskFreeMB int64
	MemAvailMB int64
	StatusCode int
}

// DetectedFacts aggregates probe results used by the decision engine.
type DetectedFacts struct {
	UpstreamHost string
	TLS          TLSState
	ClockSkewS   float64
	Network      NetworkFacts
	Runtime      RuntimeKind
	Companion    CompanionFacts
}
