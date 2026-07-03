// Package dmsclient syncs buffered documents upstream to the central DMS,
// reached through the API gateway. Outbound calls carry a service-account
// token minted via client_credentials (aud=iag.dms).
package dmsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/alvor-technologies/iag-platform-go/serviceauth"
	"github.com/iag/dms-depot-node/internal/models"
)

type Config struct {
	BaseURL      string // e.g. https://gateway/api/v1/dms
	IngestPath   string // e.g. /v1/depot/documents
	TokenURL     string
	ClientID     string
	ClientSecret string
	Audience     string // aud requested on the service token (iag.dms)
}

type Client struct {
	baseURL    string
	ingestPath string
	sa         *serviceauth.Client
	http       *http.Client
	enabled    bool
}

// New builds an upstream client. When the base URL or service credentials are
// missing the client is disabled — documents buffer locally but never sync,
// which is the correct behaviour for an offline or unconfigured depot.
func New(cfg Config) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		ingestPath: "/" + strings.TrimLeft(cfg.IngestPath, "/"),
		http:       &http.Client{Timeout: 15 * time.Second},
	}
	if cfg.BaseURL == "" || cfg.TokenURL == "" || cfg.ClientID == "" || cfg.ClientSecret == "" {
		return c
	}
	c.sa = serviceauth.NewClient(serviceauth.Options{
		TokenURL:     cfg.TokenURL,
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		Audience:     cfg.Audience,
	})
	c.enabled = true
	return c
}

func (c *Client) Enabled() bool { return c != nil && c.enabled }

type ingestRequest struct {
	DepotID    string          `json:"depotId"`
	DocType    string          `json:"docType"`
	Reference  string          `json:"reference"`
	OutletID   string          `json:"outletId,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	CapturedBy string          `json:"capturedBy,omitempty"`
	CapturedAt time.Time       `json:"capturedAt"`
}

// Sync posts one buffered document upstream and returns the id the central DMS
// assigned it (empty if the response carried none). A non-2xx response is an
// error so the caller can schedule a retry.
func (c *Client) Sync(ctx context.Context, doc models.Document) (string, error) {
	if !c.Enabled() {
		return "", fmt.Errorf("dmsclient: sync disabled")
	}
	body, err := json.Marshal(ingestRequest{
		DepotID:    doc.DepotID,
		DocType:    doc.DocType,
		Reference:  doc.Reference,
		OutletID:   doc.OutletID,
		Payload:    doc.Payload,
		CapturedBy: doc.CapturedBy,
		CapturedAt: doc.CreatedAt,
	})
	if err != nil {
		return "", err
	}
	endpoint := c.baseURL + c.ingestPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// Idempotency key lets the central DMS dedupe replays of the same document.
	req.Header.Set("Idempotency-Key", doc.ID.String())
	if err := c.sa.AuthorizeRequest(ctx, req); err != nil {
		return "", fmt.Errorf("attach token: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("upstream %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return extractID(raw), nil
}

// Ping reports whether the upstream is reachable, used for the online/offline
// signal in heartbeats. A missing base URL is treated as offline.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("no upstream configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode >= 500 {
		return fmt.Errorf("upstream health %d", resp.StatusCode)
	}
	return nil
}

// extractID pulls an id out of a loosely-shaped JSON response: {"id":...},
// {"upstreamId":...} or {"data":{"id":...}}.
func extractID(raw []byte) string {
	var envelope struct {
		ID         string          `json:"id"`
		UpstreamID string          `json:"upstreamId"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	if envelope.ID != "" {
		return envelope.ID
	}
	if envelope.UpstreamID != "" {
		return envelope.UpstreamID
	}
	if len(envelope.Data) > 0 {
		var inner struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(envelope.Data, &inner); err == nil {
			return inner.ID
		}
	}
	return ""
}
