package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/iag/dms-depot-node/internal/models"
)

// Memory is an in-process Store for local/dev runs. It is not durable across
// restarts — the offline buffer only survives process life.
type Memory struct {
	mu    sync.Mutex
	docs  map[string]models.Document // keyed by id
	order []string                   // insertion order for stable listing
	state map[string]SyncState       // keyed by depotID
	now   func() time.Time
}

func NewMemory() *Memory {
	return &Memory{
		docs:  map[string]models.Document{},
		state: map[string]SyncState{},
		now:   time.Now,
	}
}

func (m *Memory) CreateDocument(_ context.Context, doc models.Document) (models.Document, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if doc.Reference != "" {
		for _, id := range m.order {
			d := m.docs[id]
			if d.DepotID == doc.DepotID && d.DocType == doc.DocType && d.Reference == doc.Reference {
				return d, false, nil
			}
		}
	}
	now := m.now().UTC()
	doc.Status = models.StatusBuffered
	doc.CreatedAt = now
	doc.UpdatedAt = now
	if len(doc.Payload) == 0 {
		doc.Payload = []byte("{}")
	}
	m.docs[doc.ID.String()] = doc
	m.order = append(m.order, doc.ID.String())
	return doc, true, nil
}

func (m *Memory) GetDocument(_ context.Context, id string) (models.Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[id]
	if !ok {
		return models.Document{}, ErrNotFound
	}
	return d, nil
}

func (m *Memory) ListDocuments(_ context.Context, f ListFilter) ([]models.Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	out := make([]models.Document, 0, limit)
	// newest first
	for i := len(m.order) - 1; i >= 0 && len(out) < limit; i-- {
		d := m.docs[m.order[i]]
		if f.Status != "" && d.Status != f.Status {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

func (m *Memory) ClaimPending(_ context.Context, depotID string, limit int, backoff time.Duration) ([]models.Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 25
	}
	now := m.now().UTC()
	// oldest available first for FIFO drain
	ids := append([]string(nil), m.order...)
	sort.SliceStable(ids, func(i, j int) bool {
		return m.docs[ids[i]].UpdatedAt.Before(m.docs[ids[j]].UpdatedAt)
	})
	out := []models.Document{}
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		d := m.docs[id]
		if depotID != "" && d.DepotID != depotID {
			continue
		}
		if d.Status != models.StatusBuffered && d.Status != models.StatusFailed {
			continue
		}
		d.Status = models.StatusSyncing
		d.SyncAttempts++
		d.UpdatedAt = now.Add(backoff) // pushes re-release out if we crash mid-flight
		m.docs[id] = d
		out = append(out, d)
	}
	return out, nil
}

func (m *Memory) MarkSynced(_ context.Context, id, upstreamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[id]
	if !ok {
		return ErrNotFound
	}
	now := m.now().UTC()
	d.Status = models.StatusSynced
	d.UpstreamID = upstreamID
	d.LastError = ""
	d.SyncedAt = &now
	d.UpdatedAt = now
	m.docs[id] = d
	return nil
}

func (m *Memory) MarkFailed(_ context.Context, id, errMsg string, retryDelay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[id]
	if !ok {
		return ErrNotFound
	}
	now := m.now().UTC()
	d.Status = models.StatusFailed
	d.LastError = errMsg
	d.UpdatedAt = now.Add(retryDelay)
	m.docs[id] = d
	return nil
}

func (m *Memory) ResetForRetry(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[id]
	if !ok {
		return ErrNotFound
	}
	now := m.now().UTC()
	d.Status = models.StatusBuffered
	d.LastError = ""
	d.UpdatedAt = now
	m.docs[id] = d
	return nil
}

func (m *Memory) Stats(_ context.Context, depotID string) (models.SyncStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s models.SyncStats
	for _, id := range m.order {
		d := m.docs[id]
		if depotID != "" && d.DepotID != depotID {
			continue
		}
		switch d.Status {
		case models.StatusBuffered:
			s.Buffered++
		case models.StatusSyncing:
			s.Syncing++
		case models.StatusSynced:
			s.Synced++
		case models.StatusFailed:
			s.Failed++
		}
	}
	st := m.state[depotID]
	s.LastSyncAt = st.LastSyncAt
	s.LastSyncOK = st.LastSyncOK
	s.LastError = st.LastError
	return s, nil
}

func (m *Memory) GetSyncState(_ context.Context, depotID string) (SyncState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state[depotID], nil
}

func (m *Memory) SetSyncState(_ context.Context, depotID string, s SyncState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[depotID] = s
	return nil
}
