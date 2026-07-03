package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iag/dms-depot-node/internal/models"
)

// Postgres is the durable Store backend.
type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (s *Postgres) CreateDocument(ctx context.Context, doc models.Document) (models.Document, bool, error) {
	if len(doc.Payload) == 0 {
		doc.Payload = []byte("{}")
	}
	// ON CONFLICT on the partial unique index makes capture idempotent when a
	// reference is supplied. DO NOTHING + a follow-up SELECT resolves the row.
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO depot_documents (id, depot_id, doc_type, reference, outlet_id, payload, captured_by)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
		ON CONFLICT (depot_id, doc_type, reference) WHERE reference <> '' DO NOTHING
	`, doc.ID, doc.DepotID, doc.DocType, doc.Reference, doc.OutletID, doc.Payload, doc.CapturedBy)
	if err != nil {
		return models.Document{}, false, err
	}
	if tag.RowsAffected() == 0 && doc.Reference != "" {
		existing, err := s.getByReference(ctx, doc.DepotID, doc.DocType, doc.Reference)
		if err != nil {
			return models.Document{}, false, err
		}
		return existing, false, nil
	}
	out, err := s.GetDocument(ctx, doc.ID.String())
	if err != nil {
		return models.Document{}, false, err
	}
	return out, true, nil
}

const docColumns = `id, depot_id, doc_type, reference, outlet_id, payload, status,
	sync_attempts, COALESCE(last_error, ''), COALESCE(upstream_id, ''), captured_by,
	created_at, updated_at, synced_at`

func scanDoc(row pgx.Row) (models.Document, error) {
	var d models.Document
	err := row.Scan(&d.ID, &d.DepotID, &d.DocType, &d.Reference, &d.OutletID, &d.Payload,
		&d.Status, &d.SyncAttempts, &d.LastError, &d.UpstreamID, &d.CapturedBy,
		&d.CreatedAt, &d.UpdatedAt, &d.SyncedAt)
	return d, err
}

func (s *Postgres) GetDocument(ctx context.Context, id string) (models.Document, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+docColumns+` FROM depot_documents WHERE id = $1`, id)
	d, err := scanDoc(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Document{}, ErrNotFound
	}
	return d, err
}

func (s *Postgres) getByReference(ctx context.Context, depotID, docType, reference string) (models.Document, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+docColumns+`
		FROM depot_documents WHERE depot_id = $1 AND doc_type = $2 AND reference = $3`,
		depotID, docType, reference)
	d, err := scanDoc(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Document{}, ErrNotFound
	}
	return d, err
}

func (s *Postgres) ListDocuments(ctx context.Context, f ListFilter) ([]models.Document, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+docColumns+`
		FROM depot_documents
		WHERE ($1 = '' OR status = $1)
		ORDER BY created_at DESC
		LIMIT $2`, f.Status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.Document{}
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Postgres) ClaimPending(ctx context.Context, depotID string, limit int, backoff time.Duration) ([]models.Document, error) {
	if limit <= 0 {
		limit = 25
	}
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM depot_documents
			WHERE status IN ('buffered', 'failed')
			  AND available_at <= NOW()
			  AND ($1 = '' OR depot_id = $1)
			ORDER BY available_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE depot_documents d
		SET status = 'syncing',
		    sync_attempts = d.sync_attempts + 1,
		    available_at = NOW() + $3::interval,
		    updated_at = NOW()
		FROM due
		WHERE d.id = due.id
		RETURNING `+prefixCols("d")+`
	`, depotID, limit, fmt.Sprintf("%d milliseconds", backoff.Milliseconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.Document{}
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Postgres) MarkSynced(ctx context.Context, id, upstreamID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE depot_documents
		SET status = 'synced', upstream_id = $2, last_error = NULL,
		    synced_at = NOW(), updated_at = NOW()
		WHERE id = $1`, id, upstreamID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Postgres) MarkFailed(ctx context.Context, id, errMsg string, retryDelay time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE depot_documents
		SET status = 'failed', last_error = $2,
		    available_at = NOW() + $3::interval, updated_at = NOW()
		WHERE id = $1`, id, errMsg, fmt.Sprintf("%d milliseconds", retryDelay.Milliseconds()))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Postgres) ResetForRetry(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE depot_documents
		SET status = 'buffered', last_error = NULL,
		    available_at = NOW(), updated_at = NOW()
		WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Postgres) Stats(ctx context.Context, depotID string) (models.SyncStats, error) {
	var st models.SyncStats
	rows, err := s.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM depot_documents
		WHERE ($1 = '' OR depot_id = $1)
		GROUP BY status`, depotID)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return st, err
		}
		switch status {
		case models.StatusBuffered:
			st.Buffered = n
		case models.StatusSyncing:
			st.Syncing = n
		case models.StatusSynced:
			st.Synced = n
		case models.StatusFailed:
			st.Failed = n
		}
	}
	if err := rows.Err(); err != nil {
		return st, err
	}
	sync, err := s.GetSyncState(ctx, depotID)
	if err != nil {
		return st, err
	}
	st.LastSyncAt = sync.LastSyncAt
	st.LastSyncOK = sync.LastSyncOK
	st.LastError = sync.LastError
	return st, nil
}

func (s *Postgres) GetSyncState(ctx context.Context, depotID string) (SyncState, error) {
	var st SyncState
	row := s.pool.QueryRow(ctx, `
		SELECT last_sync_at, last_sync_ok, COALESCE(last_error, '')
		FROM depot_sync_state WHERE depot_id = $1`, depotID)
	err := row.Scan(&st.LastSyncAt, &st.LastSyncOK, &st.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return SyncState{}, nil
	}
	return st, err
}

func (s *Postgres) SetSyncState(ctx context.Context, depotID string, st SyncState) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO depot_sync_state (depot_id, last_sync_at, last_sync_ok, last_error, updated_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (depot_id) DO UPDATE
		SET last_sync_at = EXCLUDED.last_sync_at,
		    last_sync_ok = EXCLUDED.last_sync_ok,
		    last_error = EXCLUDED.last_error,
		    updated_at = NOW()`,
		depotID, st.LastSyncAt, st.LastSyncOK, nullable(st.LastError))
	return err
}

func prefixCols(alias string) string {
	return alias + `.id, ` + alias + `.depot_id, ` + alias + `.doc_type, ` + alias + `.reference, ` +
		alias + `.outlet_id, ` + alias + `.payload, ` + alias + `.status, ` + alias + `.sync_attempts, ` +
		`COALESCE(` + alias + `.last_error, ''), COALESCE(` + alias + `.upstream_id, ''), ` +
		alias + `.captured_by, ` + alias + `.created_at, ` + alias + `.updated_at, ` + alias + `.synced_at`
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
