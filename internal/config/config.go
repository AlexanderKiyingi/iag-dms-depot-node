// Package config loads the depot-node runtime configuration from the
// environment. The depot node is an edge service deployed at a distributor
// depot: it accepts platform Bearer tokens (aud=iag.dms-depot-node) on inbound
// requests and mints its own service-account token (aud=iag.dms) for outbound
// document sync to the central DMS through the gateway.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alvor-technologies/iag-platform-go/corsenv"
)

type Config struct {
	ServiceName    string
	Addr           string
	Environment    string
	DepotID        string // logical identifier of the physical depot this node serves
	DatabaseURL    string
	UseMemoryStore bool

	// Inbound auth (this service verifies these on requests it receives).
	JWTIssuer string
	JWKSURL   string
	Audience  string

	// Outbound service-account auth (used to sync documents upstream).
	ServiceClientID     string
	ServiceClientSecret string
	AuthTokenURL        string

	CORSOrigin  string
	AutoMigrate bool

	// Upstream sync target (the central DMS, reached via the gateway).
	UpstreamBaseURL  string // e.g. https://iag-api-gateway-production.up.railway.app/api/v1/dms
	UpstreamAudience string // aud requested on the service token (e.g. iag.dms)
	IngestPath       string // path appended to UpstreamBaseURL to POST a buffered document
	SyncEnabled      bool
	SyncInterval     time.Duration
	SyncBatchSize    int
	MaxSyncAttempts  int
	HeartbeatEnabled bool
	HeartbeatEvery   time.Duration

	// Kafka event bus (emits depot.* events on iag.operations).
	EventBusEnabled bool
	KafkaBrokers    []string

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func Load() (Config, error) {
	env := strings.ToLower(strings.TrimSpace(envOr("ENVIRONMENT", envOr("APP_ENV", "development"))))
	issuer := envOr("JWT_ISSUER", "http://localhost:3001")
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	useMemory := strings.EqualFold(envOr("STORE_MODE", ""), "memory")
	upstream := strings.TrimRight(strings.TrimSpace(os.Getenv("DMS_UPSTREAM_URL")), "/")

	cfg := Config{
		ServiceName:    envOr("SERVICE_NAME", "dms-depot-node"),
		Addr:           ListenAddr(),
		Environment:    env,
		DepotID:        strings.TrimSpace(envOr("DEPOT_ID", "depot-local")),
		DatabaseURL:    dbURL,
		UseMemoryStore: useMemory,

		JWTIssuer: issuer,
		JWKSURL:   envOr("JWKS_URL", strings.TrimRight(issuer, "/")+"/.well-known/jwks.json"),
		Audience:  envOr("AUDIENCE", "iag.dms-depot-node"),

		ServiceClientID:     envOr("SERVICE_CLIENT_ID", "iag-dms-depot-node"),
		ServiceClientSecret: strings.TrimSpace(os.Getenv("SERVICE_CLIENT_SECRET")),
		AuthTokenURL:        envOr("AUTH_TOKEN_URL", strings.TrimRight(issuer, "/")+"/oauth/token"),

		CORSOrigin:  corsenv.Allowlist(corsenv.DefaultDevOrigins),
		AutoMigrate: envOr("AUTO_MIGRATE", "true") != "false",

		UpstreamBaseURL:  upstream,
		UpstreamAudience: envOr("DMS_UPSTREAM_AUDIENCE", "iag.dms"),
		IngestPath:       "/" + strings.TrimLeft(envOr("DMS_INGEST_PATH", "/v1/depot/documents"), "/"),
		SyncEnabled:      strings.EqualFold(envOr("SYNC_ENABLED", "true"), "true") && upstream != "",
		SyncInterval:     durationOr("SYNC_INTERVAL", 15*time.Second),
		SyncBatchSize:    intOr("SYNC_BATCH_SIZE", 25),
		MaxSyncAttempts:  intOr("MAX_SYNC_ATTEMPTS", 0), // 0 = retry forever with backoff
		HeartbeatEnabled: strings.EqualFold(envOr("HEARTBEAT_ENABLED", "true"), "true"),
		HeartbeatEvery:   durationOr("HEARTBEAT_INTERVAL", 60*time.Second),

		EventBusEnabled: strings.EqualFold(os.Getenv("EVENT_BUS_ENABLED"), "true"),
		KafkaBrokers:    parseList(os.Getenv("KAFKA_BROKERS")),

		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if !c.UseMemoryStore && c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required unless STORE_MODE=memory")
	}
	if c.Audience == "" {
		return fmt.Errorf("AUDIENCE is required (e.g. iag.dms-depot-node)")
	}
	if c.JWKSURL == "" {
		return fmt.Errorf("JWKS_URL is required")
	}
	if c.DepotID == "" {
		return fmt.Errorf("DEPOT_ID is required")
	}
	if c.IsProduction() {
		if c.HasWildcardCORS() {
			return fmt.Errorf("set ALLOWED_ORIGINS in production (not *)")
		}
		if len(strings.TrimSpace(c.ServiceClientSecret)) < 16 {
			return fmt.Errorf("SERVICE_CLIENT_SECRET must be at least 16 characters in production")
		}
		if c.AutoMigrate {
			return fmt.Errorf("AUTO_MIGRATE must be false in production (run migrations out of band)")
		}
		if c.UseMemoryStore {
			return fmt.Errorf("STORE_MODE=memory is not allowed in production")
		}
		if c.SyncEnabled && c.UpstreamBaseURL == "" {
			return fmt.Errorf("DMS_UPSTREAM_URL is required when SYNC_ENABLED in production")
		}
	}
	return nil
}

func (c Config) IsProduction() bool {
	return c.Environment == "production" || c.Environment == "prod"
}

// StrictRBAC denies access when JWT permission lists are empty (fail-closed).
// Production always enforces it; dev allows empty permissions for local iteration.
func (c Config) StrictRBAC() bool { return c.IsProduction() }

func (c Config) HasWildcardCORS() bool {
	for _, o := range strings.Split(c.CORSOrigin, ",") {
		if strings.TrimSpace(o) == "*" {
			return true
		}
	}
	return c.CORSOrigin == "*"
}

func parseList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intOr(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func durationOr(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
