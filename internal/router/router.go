// Package router wires the depot-node HTTP routes and middleware.
package router

import (
	"net/http"
	"strings"

	platformmw "github.com/alvor-technologies/iag-platform-go/middleware"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/iag/dms-depot-node/internal/auth"
	"github.com/iag/dms-depot-node/internal/config"
	"github.com/iag/dms-depot-node/internal/handlers"
	"github.com/iag/dms-depot-node/internal/middleware"
	"github.com/iag/dms-depot-node/internal/models"
)

type Options struct {
	Cfg          config.Config
	API          *handlers.API
	PlatformAuth *middleware.PlatformAuth
}

func New(opts Options) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(otelgin.Middleware(opts.Cfg.ServiceName))
	r.Use(platformmw.RequestID())
	r.Use(securityHeaders())
	r.Use(corsMiddleware(opts.Cfg.CORSOrigin))

	api := opts.API

	// Root service descriptor — headless edge node, no bundled UI.
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": opts.Cfg.ServiceName,
			"depot":   opts.Cfg.DepotID,
			"status":  "ok",
			"api":     "/v1",
		})
	})
	r.GET("/health", api.Health)
	r.GET("/healthz", api.Health)
	r.GET("/ready", api.Ready)

	v1 := r.Group("/v1")
	if opts.PlatformAuth != nil {
		v1.Use(opts.PlatformAuth.AttachPrincipal())
	}
	if opts.Cfg.StrictRBAC() {
		v1.Use(auth.StrictRBAC())
	}

	v1.GET("/node", auth.RequirePerm(models.PermView), api.NodeInfo)

	v1.GET("/documents", auth.RequirePerm(models.PermView), api.ListDocuments)
	v1.POST("/documents", auth.RequirePerm(models.PermCapture), api.CaptureDocument)
	v1.GET("/documents/:id", auth.RequirePerm(models.PermView), api.GetDocument)
	v1.POST("/documents/:id/retry", auth.RequirePerm(models.PermManage), api.RetryDocument)

	v1.GET("/sync/status", auth.RequirePerm(models.PermView), api.SyncStatus)
	v1.POST("/sync/run", auth.RequirePerm(models.PermManage), api.RunSync)

	return r
}

// securityHeaders emits the platform-wide baseline. The depot node is a headless
// JSON API, so the CSP denies everything intended for a browser context.
func securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Header("Permissions-Policy", "geolocation=(), microphone=(), camera=(), interest-cohort=()")
		if c.Request.TLS != nil || strings.EqualFold(c.GetHeader("X-Forwarded-Proto"), "https") {
			c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload")
		}
		c.Next()
	}
}

func corsMiddleware(allowed string) gin.HandlerFunc {
	allowAny := allowed == "" || allowed == "*"
	allowedOrigins := splitAllowedOrigins(allowed)
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if allowAny || (origin != "" && originAllowed(origin, allowedOrigins)) {
			if origin != "" {
				c.Header("Access-Control-Allow-Origin", origin)
			} else if allowAny {
				c.Header("Access-Control-Allow-Origin", "*")
			}
			c.Header("Vary", "Origin")
		}
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, If-Match, X-Requested-With, X-Request-ID, Idempotency-Key")
		c.Header("Access-Control-Expose-Headers", "X-Request-ID")
		c.Header("Access-Control-Max-Age", "86400")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func splitAllowedOrigins(allowed string) []string {
	if allowed == "" || allowed == "*" {
		return nil
	}
	parts := strings.Split(allowed, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func originAllowed(origin string, allowed []string) bool {
	for _, candidate := range allowed {
		if origin == candidate {
			return true
		}
	}
	return false
}
