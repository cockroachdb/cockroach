// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/penglongli/gin-metrics/ginmetrics"
)

const (
	DefaultPort        = 8080
	DefaultMetricsPort = 8081

	ApiShutdownTimeout = 3 * time.Second
)

type Api struct {
	engine        *gin.Engine
	mainServer    *http.Server
	metricsServer *http.Server
	controllers   []controllers.IController

	baseURL string

	port        int
	metrics     bool
	metricsPort int

	// trustedProxies is the list of trusted proxy CIDRs for X-Forwarded-For parsing.
	// When nil, Gin uses RemoteAddr directly (no proxy headers trusted).
	trustedProxies []string

	// authHeader is the HTTP header to extract authentication tokens from
	authHeader string

	// authenticator is the pluggable authentication/authorization implementation
	// Always present - uses DisabledAuthenticator for development/testing
	authenticator auth.IAuthenticator
}

func (a *Api) Init(l *logger.Logger) {

	// Init gin engine and set up server-wide middlewares
	gin.DefaultWriter = l
	if l.LogLevel >= slog.LevelInfo {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	// Full API mode: create gin engine with all middlewares
	ginEngine := gin.New()

	// Configure trusted proxies for correct ClientIP() resolution.
	// Without this, Gin trusts all proxies by default, allowing X-Forwarded-For
	// spoofing which bypasses service account IP origin checks.
	if err := ginEngine.SetTrustedProxies(a.trustedProxies); err != nil {
		l.Error("invalid trusted proxy configuration", slog.Any("error", err))
		panic(errors.Wrap(err, "invalid trusted proxy CIDRs"))
	}
	if len(a.trustedProxies) == 0 {
		l.Warn("no trusted proxies configured; ClientIP() will use RemoteAddr directly")
	} else {
		l.Info("trusted proxies configured", slog.Any("cidrs", a.trustedProxies))
	}
	ginEngine.Use(gin.Recovery())
	ginEngine.Use(a.securityHeaders())
	ginEngine.Use(a.requestSizeLimit())
	ginEngine.Use(a.requestID())
	ginEngine.Use(a.traceContext())
	ginEngine.Use(a.slogFormatter(l))

	// Add Prometheus metrics endpoint
	if a.metrics {
		m := ginmetrics.GetMonitor()
		m.SetMetricPrefix(fmt.Sprintf("%s_", configtypes.MetricsNamespace))
		m.SetMetricPath("/metrics")

		if a.port == a.metricsPort {
			l.Info(
				"Starting metrics service on the same port as the API",
				slog.Int("port", a.metricsPort),
			)
			m.Use(ginEngine)
		} else {
			l.Info("Starting metrics service on a different port than the API",
				slog.Int("port", a.metricsPort),
			)
			metricRouter := gin.New()
			metricRouter.Use(gin.Recovery())
			metricRouter.Use(a.requestID())
			metricRouter.Use(a.traceContext())
			m.UseWithoutExposingEndpoint(ginEngine)
			m.Expose(metricRouter)
			// Store metrics server for proper lifecycle management
			a.metricsServer = &http.Server{
				Addr:    fmt.Sprintf(":%d", a.metricsPort),
				Handler: metricRouter,
			}
		}

	}

	// Add controllers' handlers and routes
	for _, controller := range a.controllers {
		for _, handler := range controller.GetControllerHandlers() {

			var handlers []gin.HandlerFunc

			// Add authentication/authorization middleware if required
			if handler.GetAuthenticationType() != controllers.AuthenticationTypeNone {
				// Start with authentication middleware using the controller's method
				middlewares := []gin.HandlerFunc{controller.AuthMiddleware(a.authenticator, a.authHeader)}

				// Add authorization middleware if requirements are specified
				if authzReq := handler.GetAuthorizationRequirement(); authzReq != nil {
					middlewares = append(middlewares, controller.AuthzMiddleware(a.authenticator, authzReq))
				}

				// Append the actual handler functions
				handlers = append(middlewares, handler.GetRouteHandlers()...)
			} else {
				handlers = handler.GetRouteHandlers()
			}

			ginEngine.Handle(
				handler.GetMethod(),
				fmt.Sprintf("%s%s", a.baseURL, handler.GetPath()),
				handlers...,
			)
		}
	}

	a.engine = ginEngine
	a.mainServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", a.port),
		Handler: a.engine,
	}
}

func (a *Api) Start(ctx context.Context, errChan chan<- error) error {
	// Start the main API server if not in metrics-only mode
	if a.mainServer != nil {
		go func() {
			err := a.mainServer.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errChan <- utils.NewCriticalError(err)
			}
		}()
	}

	// Start the metrics server if it exists (either metrics-only mode or separate port)
	if a.metricsServer != nil {
		go func() {
			err := a.metricsServer.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errChan <- utils.NewCriticalError(err)
			}
		}()
	}

	return nil
}

func (a *Api) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), ApiShutdownTimeout)
	defer cancel()

	// Shutdown main API server first if it exists (not in metrics-only mode)
	// This stops accepting new API requests
	if a.mainServer != nil {
		if err := a.mainServer.Shutdown(ctx); err != nil {
			return errors.Wrap(err, "failed to shutdown main server")
		}
	}

	// Shutdown metrics server last so it can track the shutdown process
	if a.metricsServer != nil {
		if err := a.metricsServer.Shutdown(ctx); err != nil {
			return errors.Wrap(err, "failed to shutdown metrics server")
		}
	}

	return nil
}

func (a *Api) GetGinEngine() *gin.Engine {
	return a.engine
}

// requestID generates a new request ID and binds it to the request
func (a *Api) requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		xRequestID := uuid.New().String()
		c.Request.Header.Set(controllers.XRequestIDKeyHeader, xRequestID)
		c.Writer.Header().Set(controllers.XRequestIDKeyHeader, xRequestID)
		c.Set(controllers.XRequestIDKeyHeader, xRequestID)
		c.Next()
	}
}

// traceContext sets the trace context for the request
func (a *Api) traceContext() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(
			controllers.XCloudTraceContextHeader,
			c.Request.Header.Get(controllers.XCloudTraceContextHeader),
		)
		c.Next()
	}
}

// securityHeaders adds security headers to all responses
func (a *Api) securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Prevent MIME sniffing
		c.Header("X-Content-Type-Options", "nosniff")
		c.Next()
	}
}

// requestSizeLimit limits the size of incoming requests
func (a *Api) requestSizeLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Limit request body size to 1MB
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20) // 1MB
		c.Next()
	}
}

func (a *Api) slogFormatter(l *logger.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		// Keep track of request start time
		start := timeutil.Now()

		// Inject request attributes
		attributes := []slog.Attr{
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("ip", c.ClientIP()),
			slog.String("user-agent", c.Request.UserAgent()),
			slog.String(controllers.SessionUserEmail, c.GetString(controllers.SessionUserEmail)),
		}

		// We inject request_id if present
		if c.GetString(controllers.XRequestIDKeyHeader) != "" {
			attributes = append(
				attributes,
				slog.String("request_id", c.GetString(controllers.XRequestIDKeyHeader)),
			)
		}

		// We inject XCloudTraceContext if present (for GCP tracing)
		if c.Request.Header.Get(controllers.XCloudTraceContextHeader) != "" {
			attributes = append(
				attributes,
				slog.String(
					controllers.TraceLogPrefix,
					c.Request.Header.Get(controllers.XCloudTraceContextHeader),
				),
			)
		}

		// Create a contextualized logger for the request
		// and inject it into the context
		// Use Handler().WithAttrs() to create a single handler wrapper instead of
		// a chain of handlers (which causes memory accumulation over time)
		reqLogger := &logger.Logger{
			Logger:   slog.New(l.Logger.Handler().WithAttrs(attributes)),
			LogLevel: l.LogLevel,
		}
		c.Set("logger", reqLogger)

		// Process the request
		c.Next()

		// Keep track of request end time and calculate latency
		end := timeutil.Now()
		latency := end.Sub(start)

		// Inject response attributes
		attributes = append(
			attributes,
			slog.Int("status", c.Writer.Status()),
			slog.Any("errors", c.Errors),
			slog.Time("time", end),
			slog.Duration("latency", latency),
		)

		// Log the request with the appropriate log level
		switch {
		case c.Writer.Status() >= http.StatusBadRequest && c.Writer.Status() < http.StatusInternalServerError:
			l.LogAttrs(context.Background(), slog.LevelWarn, c.Errors.String(), attributes...)
		case c.Writer.Status() >= http.StatusInternalServerError:
			l.LogAttrs(context.Background(), slog.LevelError, c.Errors.String(), attributes...)
		case c.Request.URL.Path == fmt.Sprintf("%s%s", a.baseURL, health.ControllerPath):
			l.LogAttrs(context.Background(), slog.LevelDebug, "Health check", attributes...)
		default:
			l.LogAttrs(context.Background(), slog.LevelInfo, "Incoming request", attributes...)
		}
	}
}
