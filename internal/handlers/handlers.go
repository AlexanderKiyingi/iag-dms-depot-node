// Package handlers implements the depot-node HTTP API.
package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/alvor-technologies/iag-platform-go/apierr"
	"github.com/iag/dms-depot-node/internal/auth"
	"github.com/iag/dms-depot-node/internal/config"
	"github.com/iag/dms-depot-node/internal/events"
	"github.com/iag/dms-depot-node/internal/models"
	"github.com/iag/dms-depot-node/internal/store"
	syncsvc "github.com/iag/dms-depot-node/internal/sync"
)

// API bundles the handler dependencies.
type API struct {
	Cfg    config.Config
	Store  store.Store
	Events *events.Bus
	Engine *syncsvc.Engine
	Ping   func(ctx context.Context) error // readiness probe (nil = always ready)
}

func (a *API) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": a.Cfg.ServiceName, "depot": a.Cfg.DepotID})
}

func (a *API) Ready(c *gin.Context) {
	if a.Ping != nil {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		if err := a.Ping(ctx); err != nil {
			apierr.Write(c, http.StatusServiceUnavailable, apierr.CodeServiceUnavailable, "not ready")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// NodeInfo returns the depot node's identity, connectivity and buffer summary.
func (a *API) NodeInfo(c *gin.Context) {
	stats, err := a.Store.Stats(c.Request.Context(), a.Cfg.DepotID)
	if err != nil {
		apierr.Internal(c, "stats unavailable")
		return
	}
	online := false
	if a.Engine != nil {
		online = a.Engine.Online()
	}
	c.JSON(http.StatusOK, gin.H{
		"depotId":     a.Cfg.DepotID,
		"service":     a.Cfg.ServiceName,
		"environment": a.Cfg.Environment,
		"syncEnabled": a.Cfg.SyncEnabled,
		"online":      online,
		"stats":       stats,
	})
}

// CaptureDocument buffers a document at the depot for later upstream sync.
func (a *API) CaptureDocument(c *gin.Context) {
	var in models.CaptureInput
	if err := c.ShouldBindJSON(&in); err != nil {
		apierr.BadRequest(c, "invalid JSON body")
		return
	}
	in.DocType = strings.TrimSpace(in.DocType)
	if !models.DocTypes[in.DocType] {
		apierr.BadRequest(c, "unknown docType")
		return
	}
	doc := models.Document{
		ID:         uuid.New(),
		DepotID:    a.Cfg.DepotID,
		DocType:    in.DocType,
		Reference:  strings.TrimSpace(in.Reference),
		OutletID:   strings.TrimSpace(in.OutletID),
		Payload:    in.Payload,
		CapturedBy: auth.ActorName(c),
	}
	out, created, err := a.Store.CreateDocument(c.Request.Context(), doc)
	if err != nil {
		apierr.Internal(c, "could not buffer document")
		return
	}
	if created && a.Events != nil {
		_ = a.Events.Publish(c.Request.Context(), events.TypeDocumentBuffered, out.ID.String(), map[string]any{
			"id":        out.ID.String(),
			"depotId":   out.DepotID,
			"docType":   out.DocType,
			"reference": out.Reference,
			"outletId":  out.OutletID,
		})
	}
	if created {
		c.JSON(http.StatusCreated, out)
		return
	}
	c.JSON(http.StatusOK, out) // idempotent replay
}

func (a *API) ListDocuments(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	docs, err := a.Store.ListDocuments(c.Request.Context(), store.ListFilter{
		Status: strings.TrimSpace(c.Query("status")),
		Limit:  limit,
	})
	if err != nil {
		apierr.Internal(c, "could not list documents")
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": docs, "count": len(docs)})
}

func (a *API) GetDocument(c *gin.Context) {
	doc, err := a.Store.GetDocument(c.Request.Context(), c.Param("id"))
	if err != nil {
		apierr.NotFound(c, "document not found")
		return
	}
	c.JSON(http.StatusOK, doc)
}

// RetryDocument returns a document to the buffered state so the next sweep
// re-attempts it immediately.
func (a *API) RetryDocument(c *gin.Context) {
	id := c.Param("id")
	if err := a.Store.ResetForRetry(c.Request.Context(), id); err != nil {
		apierr.NotFound(c, "document not found")
		return
	}
	doc, err := a.Store.GetDocument(c.Request.Context(), id)
	if err != nil {
		apierr.Internal(c, "reload failed")
		return
	}
	c.JSON(http.StatusOK, doc)
}

func (a *API) SyncStatus(c *gin.Context) {
	stats, err := a.Store.Stats(c.Request.Context(), a.Cfg.DepotID)
	if err != nil {
		apierr.Internal(c, "stats unavailable")
		return
	}
	c.JSON(http.StatusOK, stats)
}

// RunSync triggers an immediate drain sweep.
func (a *API) RunSync(c *gin.Context) {
	if a.Engine == nil || !a.Cfg.SyncEnabled {
		apierr.Write(c, http.StatusConflict, apierr.CodeConflict, "sync is disabled on this node")
		return
	}
	n, err := a.Engine.RunOnce(c.Request.Context())
	if err != nil {
		apierr.Internal(c, "sync failed: "+err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"processed": n})
}
