package envdetect

import (
	"fmt"
	"strings"
)

func formatSkew(skew float64) string {
	return fmt.Sprintf("%.0fs", skew)
}

func formatClockSkewCert(facts DetectedFacts) string {
	return fmt.Sprintf(
		"cloud TLS failed: certificate validity error on upstream; clock skew is %s — likely host clock, not a real cert problem",
		formatSkew(facts.ClockSkewS),
	)
}

// BuildActionableMessage summarizes failures and the chosen mitigation for operators.
func BuildActionableMessage(facts DetectedFacts, settings ChosenSettings, reasons []string) string {
	var parts []string

	switch facts.TLS {
	case TLSExpired:
		if abs(facts.ClockSkewS) >= clockSkewThresholdS {
			parts = append(parts, formatClockSkewCert(facts)+"; sync NTP then retry")
		} else {
			parts = append(parts, "cloud TLS failed: certificate expired on the upstream; clock skew is "+formatSkew(facts.ClockSkewS)+" -> real cert problem")
		}
	case TLSUnknownCA:
		parts = append(parts, "cloud TLS failed: unknown CA; set GW_UPSTREAM_CA_FILE for your corporate CA (certificate verification stays enabled)")
	case TLSUnexpectedIssuer:
		parts = append(parts, "cloud TLS: unexpected certificate issuer — possible SSL inspection; verify proxy trust, do not disable verification")
	case TLSUnreachable:
		if !facts.Network.DNSOK {
			parts = append(parts, "no DNS for "+facts.UpstreamHost+"; check resolver and outbound policy")
		} else {
			parts = append(parts, "cloud upstream unreachable; check firewall and proxy settings")
		}
	case TLSHandshakeFail:
		parts = append(parts, "cloud TLS handshake failed; check TLS version and intermediary proxies")
	}

	if len(settings.UpstreamOrder) > 1 {
		parts = append(parts, "using upstream order "+strings.Join(settings.UpstreamOrder, " -> "))
	} else if len(settings.UpstreamOrder) == 1 {
		parts = append(parts, "active upstream "+settings.UpstreamOrder[0])
	}

	if settings.Kind == "local" && facts.Companion.Reachable {
		parts = append(parts, "local companion healthy")
	} else if settings.Kind == "local" && !facts.Companion.Reachable && facts.Companion.URL != "" {
		parts = append(parts, "local companion not reachable at "+facts.Companion.URL)
	}

	if facts.Network.ProxySet {
		parts = append(parts, "proxy env detected; ensure NO_PROXY includes companion addresses if needed")
	}

	if len(parts) == 0 && len(reasons) > 0 {
		return reasons[0]
	}
	if len(parts) == 0 {
		return "environment OK"
	}
	return strings.Join(parts, "; ")
}
