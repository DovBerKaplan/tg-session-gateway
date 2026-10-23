// Package config holds the environment-driven configuration for both
// gateway planes. Every knob keeps its GW_* name from the Python
// implementation so existing deployments transfer unchanged.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func envStr(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v, ok := os.LookupEnv(name); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envFloat(name string, def float64) float64 {
	if v, ok := os.LookupEnv(name); ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(name string, def bool) bool {
	if v, ok := os.LookupEnv(name); ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes":
			return true
		}
		return false
	}
	return def
}

// RateConfig carries the token buckets per the official Bots FAQ.
type RateConfig struct {
	PrivateRate  float64 // private chat: 1 msg/s, short burst allowed
	PrivateBurst int
	GroupRate    float64 // group: 20 msgs / 60 s
	GroupBurst   int
	GlobalRate   float64 // ~30 msgs/s for the whole bot
	GlobalBurst  int
	ReadRate     float64 // soft global bucket for reads
	PaidRate     float64 // paid broadcasts (double opt-in)
}

func DefaultRate() RateConfig {
	return RateConfig{
		PrivateRate:  envFloat("GW_PRIVATE_RATE", 1.0),
		PrivateBurst: envInt("GW_PRIVATE_BURST", 3),
		GroupRate:    envFloat("GW_GROUP_RATE", 20.0/60.0),
		GroupBurst:   envInt("GW_GROUP_BURST", 2),
		GlobalRate:   envFloat("GW_GLOBAL_RATE", 30.0),
		GlobalBurst:  envInt("GW_GLOBAL_BURST", 30),
		ReadRate:     envFloat("GW_READ_RATE", 20.0),
		PaidRate:     envFloat("GW_PAID_RATE", 1000.0),
	}
}

// PolicyConfig is the per-bot behavior when a bucket is empty.
type PolicyConfig struct {
	OnLimit               string // queue | reject
	QueueWaitTimeout      float64
	UpdateQueueMax        int
	UpdateTTLS            float64
	PaidBroadcastsEnabled bool
}

func DefaultPolicy() PolicyConfig {
	return PolicyConfig{
		OnLimit:               envStr("GW_ON_LIMIT", "queue"),
		QueueWaitTimeout:      envFloat("GW_QUEUE_WAIT_TIMEOUT", 25.0),
		UpdateQueueMax:        envInt("GW_UPDATE_QUEUE_MAX", 5000),
		UpdateTTLS:            envFloat("GW_UPDATE_TTL_S", 3600.0),
		PaidBroadcastsEnabled: envBool("GW_PAID_BROADCASTS", false),
	}
}

// StorageConfig holds DB durability + maintenance knobs.
type StorageConfig struct {
	Durability          string // normal | full
	MaintenanceInterval float64
	BackupInterval      float64
	BackupKeep          int
	AuditRetentionDays  int
}

func DefaultStorage() StorageConfig {
	return StorageConfig{
		Durability:          envStr("GW_DURABILITY", "normal"),
		MaintenanceInterval: envFloat("GW_MAINTENANCE_INTERVAL_S", 3600.0),
		BackupInterval:      envFloat("GW_BACKUP_INTERVAL_S", 86400.0),
		BackupKeep:          envInt("GW_BACKUP_KEEP", 5),
		AuditRetentionDays:  envInt("GW_AUDIT_RETENTION_DAYS", 30),
	}
}

// TypingConfig is the auto-typing boot defaults (spec §10.4).
type TypingConfig struct {
	Enabled     bool
	PrivateOnly bool
	IntervalS   float64
	TimeoutS    float64
	TimeoutText string
}

// DefaultTypingTimeoutText mirrors the Python default byte-for-byte.
const DefaultTypingTimeoutText = "⚠️ Sorry, this bot is temporarily unavailable due to a problem on the provider side. Please try again later."

func DefaultTyping() TypingConfig {
	return TypingConfig{
		Enabled:     envBool("GW_TYPING_ENABLED", true),
		PrivateOnly: envBool("GW_TYPING_PRIVATE_ONLY", true),
		IntervalS:   envFloat("GW_TYPING_INTERVAL", 4.5),
		TimeoutS:    envFloat("GW_TYPING_TIMEOUT", 30.0),
		TimeoutText: envStr("GW_TYPING_TIMEOUT_TEXT", DefaultTypingTimeoutText),
	}
}

// MTProtoRate is MTProto-specific rate handling beyond the FAQ buckets.
type MTProtoRate struct {
	FloodBackoffBase  float64 // exponential multiplier per repeated offence
	SlowmodeRespect   bool
	PeerFloodCooldown float64
	SilentRate        float64
}

func DefaultMTProtoRate() MTProtoRate {
	return MTProtoRate{
		FloodBackoffBase:  envFloat("GW_FLOOD_BACKOFF_BASE", 1.5),
		SlowmodeRespect:   envBool("GW_SLOWMODE_RESPECT", true),
		PeerFloodCooldown: envFloat("GW_PEER_FLOOD_COOLDOWN", 300.0),
		SilentRate:        envFloat("GW_SILENT_RATE", 5.0),
	}
}

// BotAPI is the Bot API gateway (proxy/poller) configuration.
type BotAPI struct {
	Host     string
	Port     int
	Upstream string
	// AdminPort > 0 splits the admin surface (/admin/*, /metrics) onto
	// a second listener; 0 keeps the classic single-port layout.
	AdminPort int

	AdminSecret string
	AppSecret   string

	DataDir   string
	FernetKey string // empty → load from data dir / generate (with warning)

	PollTimeout        int
	PollBackoffMax     float64
	PushHealthInterval float64

	Rate    RateConfig
	Policy  PolicyConfig
	Typing  TypingConfig
	Storage StorageConfig
}

func BotAPIFromEnv() BotAPI {
	return BotAPI{
		Host:               envStr("GW_HOST", "0.0.0.0"),
		Port:               envInt("GW_PORT", 8080),
		AdminPort:          envInt("GW_ADMIN_PORT", 0),
		Upstream:           envStr("GW_UPSTREAM", "https://api.telegram.org"),
		AdminSecret:        envStr("GW_ADMIN_SECRET", ""),
		AppSecret:          envStr("GW_APP_SECRET", ""),
		DataDir:            envStr("GW_DATA_DIR", "/data"),
		FernetKey:          envStr("GW_FERNET_KEY", ""),
		PollTimeout:        envInt("GW_POLL_TIMEOUT", 25),
		PollBackoffMax:     envFloat("GW_POLL_BACKOFF_MAX", 30.0),
		PushHealthInterval: envFloat("GW_PUSH_HEALTH_INTERVAL", 60.0),
		Rate:               DefaultRate(),
		Policy:             DefaultPolicy(),
		Typing:             DefaultTyping(),
		Storage:            DefaultStorage(),
	}
}

// MTProto is the MTProto sidecar configuration.
type MTProto struct {
	Host   string
	Port   int
	WSAddr string // informational: GW_WS_PORT kept for compatibility
	// AdminPort > 0 splits the admin surface (/v1/admin/*, /metrics)
	// onto a second listener; 0 keeps the classic single-port layout.
	AdminPort int

	AdminSecret string
	AppSecret   string

	DataDir   string
	FernetKey string

	APIID   int
	APIHash string

	SessionDir string
	BackupsDir string

	LockBackend string // "file" (redis removed: K8s StatefulSet + RWO PVC own single ownership)

	MaxSessions     int
	ConsumerTimeout float64

	MetricsEnabled bool

	Rate    RateConfig
	MTRate  MTProtoRate
	Policy  PolicyConfig
	Storage StorageConfig
}

func MTProtoFromEnv() MTProto {
	dataDir := envStr("GW_DATA_DIR", "/data")
	cfg := MTProto{
		Host:            envStr("GW_HOST", "0.0.0.0"),
		Port:            envInt("GW_PORT", 8080),
		AdminPort:       envInt("GW_ADMIN_PORT", 0),
		AdminSecret:     envStr("GW_ADMIN_SECRET", ""),
		AppSecret:       envStr("GW_APP_SECRET", ""),
		DataDir:         dataDir,
		FernetKey:       envStr("GW_FERNET_KEY", ""),
		APIID:           envInt("GW_API_ID", 0),
		APIHash:         envStr("GW_API_HASH", ""),
		LockBackend:     envStr("GW_LOCK_BACKEND", "file"),
		MaxSessions:     envInt("GW_MAX_SESSIONS", 50),
		ConsumerTimeout: envFloat("GW_CONSUMER_TIMEOUT", 120.0),
		MetricsEnabled:  envBool("GW_METRICS", true),
		Rate:            DefaultRate(),
		MTRate:          DefaultMTProtoRate(),
		Policy:          DefaultPolicy(),
		Storage:         DefaultStorage(),
	}
	cfg.SessionDir = dataDir + "/sessions"
	cfg.BackupsDir = dataDir + "/backups"
	return cfg
}

func (c *BotAPI) BackupsDir() string { return c.DataDir + "/backups" }

func (c *BotAPI) Validate() error {
	var missing []string
	if c.AdminSecret == "" {
		missing = append(missing, "GW_ADMIN_SECRET")
	}
	if c.AppSecret == "" {
		missing = append(missing, "GW_APP_SECRET")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	return validatePolicyStorage(c.Policy, c.Storage)
}

func (c *MTProto) Validate() error {
	var missing []string
	if c.AdminSecret == "" {
		missing = append(missing, "GW_ADMIN_SECRET")
	}
	if c.AppSecret == "" {
		missing = append(missing, "GW_APP_SECRET")
	}
	if c.APIID == 0 {
		missing = append(missing, "GW_API_ID")
	}
	if c.APIHash == "" {
		missing = append(missing, "GW_API_HASH")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	return validatePolicyStorage(c.Policy, c.Storage)
}

func validatePolicyStorage(p PolicyConfig, s StorageConfig) error {
	if p.OnLimit != "queue" && p.OnLimit != "reject" {
		return fmt.Errorf("GW_ON_LIMIT must be 'queue' or 'reject'")
	}
	if s.Durability != "normal" && s.Durability != "full" {
		return fmt.Errorf("GW_DURABILITY must be 'normal' or 'full'")
	}
	return nil
}
