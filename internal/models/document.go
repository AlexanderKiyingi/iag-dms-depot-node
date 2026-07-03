package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Document status lifecycle.
const (
	StatusBuffered = "buffered" // captured locally, awaiting sync
	StatusSyncing  = "syncing"  // claimed by the sync engine, in flight
	StatusSynced   = "synced"   // accepted by the central DMS
	StatusFailed   = "failed"   // last attempt failed, will retry after backoff
)

// Recognised document types captured at a depot.
var DocTypes = map[string]bool{
	"delivery_note":    true,
	"invoice":          true,
	"proof_of_delivery": true,
	"stock_count":      true,
	"grn":              true, // goods received note
}

// Document is a single record captured at the depot and buffered for upstream
// sync to the central DMS.
type Document struct {
	ID           uuid.UUID       `json:"id"`
	DepotID      string          `json:"depotId"`
	DocType      string          `json:"docType"`
	Reference    string          `json:"reference"`
	OutletID     string          `json:"outletId,omitempty"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	SyncAttempts int             `json:"syncAttempts"`
	LastError    string          `json:"lastError,omitempty"`
	UpstreamID   string          `json:"upstreamId,omitempty"`
	CapturedBy   string          `json:"capturedBy,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
	SyncedAt     *time.Time      `json:"syncedAt,omitempty"`
}

// CaptureInput is the request body for buffering a new document.
type CaptureInput struct {
	DocType   string          `json:"docType"`
	Reference string          `json:"reference"`
	OutletID  string          `json:"outletId"`
	Payload   json.RawMessage `json:"payload"`
}

// SyncStats summarises the current buffer depth by status.
type SyncStats struct {
	Buffered   int        `json:"buffered"`
	Syncing    int        `json:"syncing"`
	Synced     int        `json:"synced"`
	Failed     int        `json:"failed"`
	LastSyncAt *time.Time `json:"lastSyncAt,omitempty"`
	LastSyncOK bool       `json:"lastSyncOk"`
	LastError  string     `json:"lastError,omitempty"`
}
