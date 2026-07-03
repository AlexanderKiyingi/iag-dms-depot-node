// Package store persists the depot document buffer and per-depot sync state.
// Two backends implement Store: an in-memory store for local/dev runs
// (STORE_MODE=memory) and a Postgres-backed store for real deployments.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/iag/dms-depot-node/internal/models"
)

// ErrNotFound is returned when a document does not exist.
var ErrNotFound = errors.New("not found")

// ListFilter narrows a document listing.
type ListFilter struct {
	Status string
	Limit  int
}

// SyncState is the outcome of the most recent sync sweep for a depot.
type SyncState struct {
	LastSyncAt *time.Time
	LastSyncOK bool
	LastError  string
}

// Store is the persistence contract for the depot node.
type Store interface {
	// CreateDocument buffers a document. If a document with the same
	// (depotID, docType, reference) already exists (reference non-empty), the
	// existing row is returned with created=false (idempotent capture).
	CreateDocument(ctx context.Context, doc models.Document) (out models.Document, created bool, err error)
	GetDocument(ctx context.Context, id string) (models.Document, error)
	ListDocuments(ctx context.Context, f ListFilter) ([]models.Document, error)

	// ClaimPending atomically moves up to limit due documents to 'syncing',
	// pushing their availability out by backoff so a crash re-releases them.
	ClaimPending(ctx context.Context, depotID string, limit int, backoff time.Duration) ([]models.Document, error)
	MarkSynced(ctx context.Context, id, upstreamID string) error
	MarkFailed(ctx context.Context, id, errMsg string, retryDelay time.Duration) error
	// ResetForRetry returns a document to 'buffered' and makes it immediately due.
	ResetForRetry(ctx context.Context, id string) error

	Stats(ctx context.Context, depotID string) (models.SyncStats, error)

	GetSyncState(ctx context.Context, depotID string) (SyncState, error)
	SetSyncState(ctx context.Context, depotID string, s SyncState) error
}
