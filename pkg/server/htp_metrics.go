// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"net/http"
	"regexp"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

const (
	MethodLabel     = "method"
	PathLabel       = "path"
	StatusCodeLabel = "statusCode"
)

var pathVarsRegex = regexp.MustCompile("{([A-z]+)(:[^}]*)?}")

var serverHTTPMetricsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.http.metrics.enabled",
	"Enables to collection of http metrics",
	false,
)

// responseWriter wraps http.ResponseWriter with a statusCode field to provide
// access to the status code in metric reporting.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

// WriteHeader implements http.ResponseWriter
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

type HttpServerMetrics struct {
	RequestMetrics *metric.HistogramVec
	registry       *metric.Registry
	settings       *cluster.Settings
}

func NewHttpServerMetrics(reg *metric.Registry, settings *cluster.Settings) *HttpServerMetrics {
	ops := metric.NewHistogramVecOpts(metric.Metadata{
		Name:        "http.server.request.duration.seconds",
		Help:        "Duration of an HTTP request in seconds.",
		Measurement: "Duration",
		Unit:        metric.Unit_SECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}, prometheus.HistogramOpts{
		Buckets: prometheus.ExponentialBucketsRange(
			0.001, // 1ms min
			30,    // 30 second max
			50,    // 50 buckets
		),
	})

	histogramVec := metric.NewExportedHistogramVec(ops, []string{MethodLabel, PathLabel, StatusCodeLabel})
	reg.AddMetric(histogramVec)
	return &HttpServerMetrics{
		RequestMetrics: histogramVec,
		registry:       reg,
		settings:       settings,
	}
}

// registerMetricsMiddleware registers a middleware function on to the provided mux.Router to
// capture metrics on http requests. The underlying metric uses a metric.HistogramVec, which
// isn't recorded in tsdb.
func (m *HttpServerMetrics) registerMetricsMiddleware(router *mux.Router) {
	metricsMiddleWare := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !serverHTTPMetricsEnabled.Get(&m.settings.SV) {
				next.ServeHTTP(w, r)
			} else {
				route := mux.CurrentRoute(r)
				path, _ := route.GetPathTemplate()
				rw := newResponseWriter(w)
				sw := timeutil.NewStopWatch()
				sw.Start()
				next.ServeHTTP(rw, r)
				sw.Stop()
				m.RequestMetrics.Observe(map[string]string{
					"path":       formatPathVars(path),
					"method":     r.Method,
					"statusCode": strconv.Itoa(rw.statusCode),
				}, sw.Elapsed().Seconds())
			}
		})
	}
	router.Use(metricsMiddleWare)
}

// formatPathVars replaces named path variables with just the
// variable name, wrapped in <>. Any variable regex will be
// removed. For example:
// "/api/v2/database_metadata/{database_id:[0-9]+}" is
// turned into" "/api/v2/database_metadata/<database_id>"
func formatPathVars(path string) string {
	return pathVarsRegex.ReplaceAllString(path, "<$1>")
}
