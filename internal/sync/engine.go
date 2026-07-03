// Package sync runs the background workers that drain the offline document
// buffer to the central DMS and emit periodic node heartbeats.
package sync

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/iag/dms-depot-node/internal/dmsclient"
	"github.com/iag/dms-depot-node/internal/events"
	"github.com/iag/dms-depot-node/internal/models"
	"github.com/iag/dms-depot-node/internal/store"
)

type Config struct {
	DepotID        string
	Interval       time.Duration
	BatchSize      int
	MaxAttempts    int // 0 = retry forever
	MaxBackoff     time.Duration
	ClaimBackoff   time.Duration // re-release window for in-flight claims
	HeartbeatEvery time.Duration
	Heartbeat      bool
}

// Engine drains buffered documents and emits heartbeats.
type Engine struct {
	cfg    Config
	store  store.Store
	client *dmsclient.Client
	bus    *events.Bus

	mu     sync.Mutex
	online bool
}

func New(st store.Store, client *dmsclient.Client, bus *events.Bus, cfg Config) *Engine {
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 25
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Minute
	}
	if cfg.ClaimBackoff <= 0 {
		cfg.ClaimBackoff = 2 * time.Minute
	}
	if cfg.HeartbeatEvery <= 0 {
		cfg.HeartbeatEvery = 60 * time.Second
	}
	return &Engine{cfg: cfg, store: st, client: client, bus: bus}
}

// Online reports the last observed upstream reachability.
func (e *Engine) Online() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.online
}

func (e *Engine) setOnline(v bool) {
	e.mu.Lock()
	e.online = v
	e.mu.Unlock()
}

// Run starts the drain loop and (optionally) the heartbeat loop, blocking until
// ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	if e.cfg.Heartbeat {
		go e.heartbeatLoop(ctx)
	}
	if !e.client.Enabled() {
		slog.Warn("sync engine idle: no upstream configured; documents will buffer only")
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, _ := e.RunOnce(ctx); n >= e.cfg.BatchSize {
				// A full batch likely means more is due; drain again promptly.
				_, _ = e.RunOnce(ctx)
			}
		}
	}
}

// RunOnce claims and syncs a single batch, returning the number processed. It
// is safe to call from an HTTP handler to force an immediate sweep.
func (e *Engine) RunOnce(ctx context.Context) (int, error) {
	if !e.client.Enabled() {
		return 0, nil
	}
	docs, err := e.store.ClaimPending(ctx, e.cfg.DepotID, e.cfg.BatchSize, e.cfg.ClaimBackoff)
	if err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		return 0, nil
	}
	var lastErr error
	okCount := 0
	for _, d := range docs {
		id := d.ID.String()
		upstreamID, err := e.client.Sync(ctx, d)
		if err != nil {
			lastErr = err
			if e.cfg.MaxAttempts > 0 && d.SyncAttempts >= e.cfg.MaxAttempts {
				slog.Error("document exhausted retries; leaving failed", "id", id, "attempts", d.SyncAttempts, "err", err)
			}
			_ = e.store.MarkFailed(ctx, id, err.Error(), e.backoff(d.SyncAttempts))
			continue
		}
		if err := e.store.MarkSynced(ctx, id, upstreamID); err != nil {
			slog.Warn("mark synced failed", "id", id, "err", err)
			continue
		}
		okCount++
		e.publishSynced(ctx, d, upstreamID)
	}
	e.setOnline(lastErr == nil)
	e.recordState(ctx, lastErr)
	slog.Info("sync sweep", "claimed", len(docs), "synced", okCount, "depot", e.cfg.DepotID)
	return len(docs), nil
}

func (e *Engine) recordState(ctx context.Context, lastErr error) {
	now := time.Now().UTC()
	st := store.SyncState{LastSyncAt: &now, LastSyncOK: lastErr == nil}
	if lastErr != nil {
		st.LastError = lastErr.Error()
	}
	if err := e.store.SetSyncState(ctx, e.cfg.DepotID, st); err != nil {
		slog.Warn("record sync state failed", "err", err)
	}
}

func (e *Engine) publishSynced(ctx context.Context, d models.Document, upstreamID string) {
	if e.bus == nil || !e.bus.Enabled() {
		return
	}
	_ = e.bus.Publish(ctx, events.TypeDocumentSynced, d.ID.String(), map[string]any{
		"id":         d.ID.String(),
		"depotId":    d.DepotID,
		"docType":    d.DocType,
		"reference":  d.Reference,
		"upstreamId": upstreamID,
	})
}

func (e *Engine) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.HeartbeatEvery)
	defer ticker.Stop()
	e.emitHeartbeat(ctx) // emit once at startup
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.emitHeartbeat(ctx)
		}
	}
}

func (e *Engine) emitHeartbeat(ctx context.Context) {
	// Refresh the online signal even when the drain loop is idle.
	if e.client.Enabled() {
		e.setOnline(e.client.Ping(ctx) == nil)
	}
	stats, err := e.store.Stats(ctx, e.cfg.DepotID)
	if err != nil {
		slog.Warn("heartbeat stats failed", "err", err)
		return
	}
	if e.bus == nil || !e.bus.Enabled() {
		return
	}
	_ = e.bus.Publish(ctx, events.TypeNodeHeartbeat, e.cfg.DepotID, map[string]any{
		"depotId":  e.cfg.DepotID,
		"online":   e.Online(),
		"buffered": stats.Buffered,
		"failed":   stats.Failed,
		"synced":   stats.Synced,
	})
}

func (e *Engine) backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := time.Duration(math.Pow(2, float64(attempts))) * time.Second
	if d > e.cfg.MaxBackoff {
		return e.cfg.MaxBackoff
	}
	return d
}
