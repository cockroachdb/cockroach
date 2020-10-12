// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Server is a TCP server that proxies SQL connections to a
// configurable backend. It may also run an HTTP server to expose a
// health check and prometheus metrics.
type Server struct {
	opts            *Options
	mux             *http.ServeMux
	metrics         *Metrics
	metricsRegistry *metric.Registry

	promMu             syncutil.Mutex
	prometheusExporter metric.PrometheusExporter
}

// NewServer constructs a new proxy server and provisions metrics and health
// checks as well.
func NewServer(opts Options) *Server {
	mux := http.NewServeMux()

	registry := metric.NewRegistry()

	proxyMetrics := MakeProxyMetrics()

	registry.AddMetricStruct(proxyMetrics)

	s := &Server{
		opts:               &opts,
		mux:                mux,
		metrics:            &proxyMetrics,
		metricsRegistry:    registry,
		prometheusExporter: metric.MakePrometheusExporter(),
	}

	// /_status/{healthz,vars} matches CRDB's healthcheck and metrics
	// endpoints.
	mux.HandleFunc("/_status/vars/", s.handleVars)
	mux.HandleFunc("/_status/healthz/", s.handleHealth)

	return s
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// TODO(chrisseto): Ideally, this health check should actually check the
	// proxy's health in some fashion. How to actually check the health of a
	// proxy remains to be seen.
	// It has been noted that an overloaded CRDB server may fail to respond to
	// a simple HTTP request, such as this one, within a short time frame
	// (~5 seconds). Therefore, this health check provides a non-zero amount of
	// value as it indicates if the service is _massively_ overloaded or not.
	// In Kubernetes, a failed liveness check will result in the container
	// being terminated and replaced.
	// Its reasonable to assume that many, if not most, of the connections
	// being served by this proxy are unusable, if this check can not be
	// handled.  An operator may adjust this window by changing the timeout on
	// the check.
	w.WriteHeader(http.StatusOK)
	// Explicitly ignore any errors from writing our body as there's
	// nothing to be done if the write fails.
	_, _ = w.Write([]byte("OK"))
}

func (s *Server) handleVars(w http.ResponseWriter, r *http.Request) {
	s.promMu.Lock()
	defer s.promMu.Unlock()

	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	s.prometheusExporter.ScrapeRegistry(s.metricsRegistry, true /* includeChildMetrics*/)
	if err := s.prometheusExporter.PrintAsText(w); err != nil {
		log.Errorf(r.Context(), "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ServeHTTP starts the proxy's HTTP server on the given listener.
// The server provides Prometheus metrics at /_status/vars
// and a health check endpoint at /_status/healthz.
func (s *Server) ServeHTTP(ctx context.Context, ln net.Listener) error {
	srv := http.Server{
		Handler: s.mux,
	}

	go func() {
		<-ctx.Done()

		// Wait up to 15 seconds for the HTTP server to shut itself
		// down. The HTTP service is an auxiliary service for health
		// checking and metrics, which does not need a completely
		// graceful shutdown.
		_ = contextutil.RunWithTimeout(
			context.Background(),
			"http server shutdown",
			15*time.Second,
			func(shutdownCtx context.Context) error {
				// Ignore any errors as this routine will only be called
				// when the server is shutting down.
				_ = srv.Shutdown(shutdownCtx)

				return nil
			},
		)
	}()

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// Serve serves a listener according to the Options given in NewServer().
// Incoming client connections are taken through the Postgres handshake and
// relayed to the configured backend server.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			s.metrics.CurConnCount.Inc(1)
			defer conn.Close()
			defer s.metrics.CurConnCount.Dec(1)
			tBegin := timeutil.Now()
			log.Infof(context.Background(), "handling client %s", conn.RemoteAddr())
			err := s.Proxy(conn)
			log.Infof(context.Background(), "client %s disconnected after %.2fs: %v",
				conn.RemoteAddr(), timeutil.Since(tBegin).Seconds(), err)
		}()
	}
}
