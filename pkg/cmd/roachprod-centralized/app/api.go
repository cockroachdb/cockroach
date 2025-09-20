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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
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
	engine      *gin.Engine
	server      *http.Server
	controllers []controllers.IController

	baseURL string

	port        int
	metrics     bool
	metricsPort int

	authenticationDisabled bool
	authenticationAudience string
	authenticationHeader   string
}

func (a *Api) Init(l *utils.Logger) {

	// Init gin engine and set up server-wide middlewares
	gin.DefaultWriter = l
	if l.LogLevel >= slog.LevelInfo {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	ginEngine := gin.New()
	ginEngine.Use(gin.Recovery())
	ginEngine.Use(a.requestID())
	ginEngine.Use(a.traceContext())
	ginEngine.Use(a.slogFormatter(l))

	// Add Prometheus metrics endpoint
	if a.metrics {
		m := ginmetrics.GetMonitor()
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
			go func() {
				_ = metricRouter.Run(fmt.Sprintf(":%d", a.metricsPort))
			}()
		}

	}

	// Add controllers' handlers and routes
	for _, controller := range a.controllers {
		for _, handler := range controller.GetHandlers() {

			var handlers []gin.HandlerFunc

			// Add authentication handler if authentication is enabled
			// and then endpoint is authenticated (which is the default)
			switch {
			case handler.GetAuthenticationType() != controllers.AuthenticationTypeNone:
				handlers = append(
					[]gin.HandlerFunc{
						func(c *gin.Context) {
							controller.Authentication(
								c,
								a.authenticationDisabled,
								a.authenticationHeader,
								a.authenticationAudience,
							)
						},
					},
					handler.GetHandlers()...,
				)
			default:
				handlers = handler.GetHandlers()
			}

			ginEngine.Handle(
				handler.GetMethod(),
				fmt.Sprintf("%s%s", a.baseURL, handler.GetPath()),
				handlers...,
			)
		}
	}

	a.engine = ginEngine
	a.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", a.port),
		Handler: a.engine,
	}
}

func (a *Api) Start(ctx context.Context, errChan chan<- error) error {
	a.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", a.port),
		Handler: a.engine,
	}

	go func() {
		err := a.server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- utils.NewCriticalError(err)
		}
	}()

	return nil
}

func (a *Api) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), ApiShutdownTimeout)
	defer cancel()

	return a.server.Shutdown(ctx)
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

func (a *Api) slogFormatter(l *utils.Logger) gin.HandlerFunc {
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
		reqLogger := &utils.Logger{
			Logger:   l.Logger.With(),
			LogLevel: l.LogLevel,
		}
		for _, attr := range attributes {
			reqLogger.Logger = reqLogger.Logger.With(attr.Key, attr.Value)
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
		default:
			l.LogAttrs(context.Background(), slog.LevelInfo, "Incoming request", attributes...)
		}
	}
}
