package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	platformotel "github.com/alvor-technologies/iag-platform-go/otel"
	platformserviceauth "github.com/alvor-technologies/iag-platform-go/serviceauth"
	"github.com/jackc/pgx/v5/pgxpool"

	depotdb "github.com/iag/dms-depot-node/db"
	"github.com/iag/dms-depot-node/internal/config"
	"github.com/iag/dms-depot-node/internal/db"
	"github.com/iag/dms-depot-node/internal/dmsclient"
	"github.com/iag/dms-depot-node/internal/events"
	"github.com/iag/dms-depot-node/internal/handlers"
	"github.com/iag/dms-depot-node/internal/middleware"
	"github.com/iag/dms-depot-node/internal/migrate"
	"github.com/iag/dms-depot-node/internal/models"
	"github.com/iag/dms-depot-node/internal/outbox"
	"github.com/iag/dms-depot-node/internal/platformauth"
	"github.com/iag/dms-depot-node/internal/router"
	"github.com/iag/dms-depot-node/internal/store"
	syncsvc "github.com/iag/dms-depot-node/internal/sync"
)

func main() {
	configureLogger()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	tp, err := platformotel.Init(ctx, platformotel.Config{
		ServiceName: cfg.ServiceName,
		Environment: cfg.Environment,
	})
	if err != nil {
		slog.Warn("otel disabled", "err", err)
	} else {
		defer func() {
			shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = tp.Shutdown(shutdownCtx)
		}()
	}

	// --- persistence ---
	var pool *pgxpool.Pool
	var st store.Store
	if cfg.UseMemoryStore {
		slog.Warn("STORE_MODE=memory — offline buffer is not durable across restarts")
		st = store.NewMemory()
	} else {
		connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool, err = db.Connect(connectCtx, cfg.DatabaseURL)
		cancel()
		if err != nil {
			slog.Error("connect postgres", "err", err)
			os.Exit(1)
		}
		defer pool.Close()
		if cfg.AutoMigrate {
			if err := autoMigrate(context.Background(), pool); err != nil {
				slog.Error("auto-migrate", "err", err)
				os.Exit(1)
			}
		}
		st = store.NewPostgres(pool)
	}

	// --- inbound auth ---
	verifier := platformauth.NewVerifier(cfg.JWKSURL, cfg.JWTIssuer, cfg.Audience)
	jwksCtx, jwksCancel := context.WithTimeout(ctx, 10*time.Second)
	// A transient JWKS fetch failure must not crash-loop the node on boot; degrade
	// to the background refresh loop and fail requests closed until keys load.
	if err := verifier.Refresh(jwksCtx); err != nil {
		slog.Warn("jwks refresh failed on boot; serving once keys load", "err", err)
	}
	jwksCancel()
	verifier.StartRefreshLoop(ctx, 15*time.Minute)

	if cfg.ServiceClientSecret != "" {
		go registerPermissionsLoop(ctx, cfg)
	}

	// --- events + outbox ---
	eventBus := events.New(events.Config{Brokers: cfg.KafkaBrokers, Enabled: cfg.EventBusEnabled})
	defer eventBus.Close()
	if pool != nil && eventBus.Enabled() {
		outboxStore := outbox.NewStore(pool)
		eventBus.SetOutbox(outboxStore)
		publisher := outbox.NewPublisher(outboxStore, eventBus)
		go publisher.Run(ctx)
		slog.Info("outbox publisher started")
	}

	// --- upstream sync ---
	upstream := dmsclient.New(dmsclient.Config{
		BaseURL:      cfg.UpstreamBaseURL,
		IngestPath:   cfg.IngestPath,
		TokenURL:     cfg.AuthTokenURL,
		ClientID:     cfg.ServiceClientID,
		ClientSecret: cfg.ServiceClientSecret,
		Audience:     cfg.UpstreamAudience,
	})
	engine := syncsvc.New(st, upstream, eventBus, syncsvc.Config{
		DepotID:        cfg.DepotID,
		Interval:       cfg.SyncInterval,
		BatchSize:      cfg.SyncBatchSize,
		MaxAttempts:    cfg.MaxSyncAttempts,
		HeartbeatEvery: cfg.HeartbeatEvery,
		Heartbeat:      cfg.HeartbeatEnabled && eventBus.Enabled(),
	})
	go engine.Run(ctx)

	// --- HTTP ---
	api := &handlers.API{Cfg: cfg, Store: st, Events: eventBus, Engine: engine}
	if pool != nil {
		api.Ping = func(ctx context.Context) error { return pool.Ping(ctx) }
	}
	platformAuth := middleware.NewPlatformAuth(verifier)
	engineHTTP := router.New(router.Options{Cfg: cfg, API: api, PlatformAuth: platformAuth})

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           engineHTTP,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       120 * time.Second,
	}

	listenErr := make(chan error, 1)
	go func() {
		slog.Info("depot-node listening",
			"addr", cfg.Addr,
			"depot", cfg.DepotID,
			"audience", cfg.Audience,
			"syncEnabled", cfg.SyncEnabled,
			"store", map[bool]string{true: "memory", false: "postgres"}[cfg.UseMemoryStore],
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-stop:
		slog.Info("shutdown", "signal", sig.String())
	case err := <-listenErr:
		slog.Error("listener died", "err", err)
		os.Exit(1)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	cancelApp()
}

func configureLogger() {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug") {
		level = slog.LevelDebug
	}
	var handler slog.Handler
	if strings.ToLower(os.Getenv("LOG_FORMAT")) == "json" {
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	}
	slog.SetDefault(slog.New(handler))
}

func autoMigrate(parent context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	applied, err := migrate.Up(ctx, pool, depotdb.Migrations())
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if len(applied) == 0 {
		slog.Info("schema already up to date")
	} else {
		slog.Info("migrations applied", "versions", applied)
	}
	return nil
}

func registerPermissionsLoop(ctx context.Context, cfg config.Config) {
	saClient := platformserviceauth.NewClient(platformserviceauth.Options{
		TokenURL:     cfg.AuthTokenURL,
		ClientID:     cfg.ServiceClientID,
		ClientSecret: cfg.ServiceClientSecret,
		Audience:     "iag.authentication",
	})
	descriptors := models.PermissionDescriptors()
	perms := make([]platformserviceauth.Permission, 0, len(descriptors))
	for _, d := range descriptors {
		perms = append(perms, platformserviceauth.Permission{Name: d.Name, Description: d.Description})
	}
	backoff := time.Second
	for {
		regCtx, c := context.WithTimeout(ctx, 10*time.Second)
		err := platformserviceauth.RegisterPermissions(regCtx, saClient, cfg.JWTIssuer, "dms-depot-node", perms)
		c()
		if err == nil {
			slog.Info("permissions registered", "count", len(perms))
			return
		}
		slog.Warn("permissions register failed", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Minute {
			backoff *= 2
		}
	}
}
