// Package version carries the build version. The default is for
// local builds; release builds inject the real value:
//
//	go build -ldflags "-X github.com/DovBerKaplan/tg-session-gateway/go/internal/version.Version=v0.1.0"
package version

// Version is reported by `tggw version`, /healthz and the startup log.
var Version = "0.1.0-dev"
