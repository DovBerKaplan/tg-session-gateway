package envdetect

// ExplicitFlags records which config fields the operator set explicitly.
// Explicit values always override environment detection.
type ExplicitFlags struct {
	EnvDetect          bool
	Upstream           bool
	UpstreamFallback   bool
	UpstreamKind       bool
	UpstreamFailover   bool
	MaxMultipartMB     bool
	UpstreamTimeoutS   bool
	PathPrefix         bool
	UpstreamCAFile     bool
}

// ExplicitConfig is the operator-facing upstream / env-detect configuration.
type ExplicitConfig struct {
	EnvDetect        string // off | auto
	Upstream         string
	UpstreamFallback string
	UpstreamKind     string // cloud | local | auto
	UpstreamFailover string // off | auto | prefer_local
	MaxMultipartMB   int
	UpstreamTimeoutS int
	PathPrefix       string
	UpstreamCAFile   string

	Explicit ExplicitFlags
}

// DefaultExplicitConfig matches GW_* defaults before env-detect runs.
func DefaultExplicitConfig() ExplicitConfig {
	return ExplicitConfig{
		EnvDetect:        "off",
		Upstream:         DefaultCloudUpstream,
		UpstreamKind:     "auto",
		UpstreamFailover: "off",
		MaxMultipartMB:   DefaultMaxMultipartMB,
		UpstreamTimeoutS: DefaultUpstreamTimeoutS,
	}
}

const (
	DefaultCloudUpstream    = "https://api.telegram.org"
	DefaultMaxMultipartMB   = 50
	DefaultUpstreamTimeoutS = 60

	clockSkewThresholdS = 300.0
)
