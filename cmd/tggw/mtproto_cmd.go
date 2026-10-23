package main

import (
	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/mtproto"
)

// runMTProto boots the MTProto sidecar.
func runMTProto() error {
	cfg := config.MTProtoFromEnv()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := preflight(cfg.FernetKey, cfg.DataDir); err != nil {
		return err
	}
	app, err := mtproto.NewApp(cfg)
	if err != nil {
		return err
	}
	return app.RunServe()
}
