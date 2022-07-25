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
	"net/http/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var (
	awaitNoConnectionsInterval = time.Minute
)

// Server is a TCP server that proxies SQL connections to a configurable
// backend. It may also run an HTTP server to expose a health check and
// prometheus metrics.
type Server struct {
	Stopper         *stop.Stopper
	handler         *proxyHandler
	mux             *http.ServeMux
	metrics         *metrics
	metricsRegistry *metric.Registry

	prometheusExporter metric.PrometheusExporter
}

// NewServer constructs a new proxy server and provisions metrics and health
// checks as well.
func NewServer(ctx context.Context, stopper *stop.Stopper, options ProxyOptions) (*Server, error) {
	registry := metric.NewRegistry()

	proxyMetrics := makeProxyMetrics()
	registry.AddMetricStruct(&proxyMetrics)

	handler, err := newProxyHandler(ctx, stopper, registry, &proxyMetrics, options)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()

	s := &Server{
		Stopper:            stopper,
		handler:            handler,
		mux:                mux,
		metrics:            &proxyMetrics,
		metricsRegistry:    registry,
		prometheusExporter: metric.MakePrometheusExporter(),
	}

	// /_status/{healthz,vars} matches CRDB's healthcheck and metrics
	// endpoints.
	mux.HandleFunc("/_status/vars/", s.handleVars)
	mux.HandleFunc("/_status/healthz/", s.handleHealth)

	// Taken from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return s, nil
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
	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	scrape := func(pm *metric.PrometheusExporter) {
		pm.ScrapeRegistry(s.metricsRegistry, true /* includeChildMetrics*/)
	}
	if err := s.prometheusExporter.ScrapeAndPrintAsText(w, scrape); err != nil {
		log.Errorf(r.Context(), "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ServeHTTP starts the proxy's HTTP server on the given listener.
// The server provides Prometheus metrics at /_status/vars,
// a health check endpoint at /_status/healthz, and pprof debug
// endpoints at /debug/pprof.
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
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	err := s.Stopper.RunAsyncTask(ctx, "listen-quiesce", func(ctx context.Context) {
		<-s.Stopper.ShouldQuiesce()
		if err := ln.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
			log.Fatalf(ctx, "closing proxy listener: %s", err)
		}
	})
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		err = s.Stopper.RunAsyncTask(ctx, "proxy-con-serve", func(ctx context.Context) {
			defer func() { _ = conn.Close() }()
			s.metrics.CurConnCount.Inc(1)
			defer s.metrics.CurConnCount.Dec(1)
			remoteAddr := conn.RemoteAddr()
			ctxWithTag := logtags.AddTag(ctx, "client", log.SafeOperational(remoteAddr))
			if err := s.handler.handle(ctxWithTag, conn); err != nil {
				log.Infof(ctxWithTag, "connection error: %v", err)
			}
		})
		if err != nil {
			return err
		}
	}
}

// AwaitNoConnections returns a channel that is closed once the server has no open connections.
// This is meant to be used after the server has stopped accepting new connections and we are
// waiting to shutdown the server without inturrupting existing connections
//
// If the context is cancelled the channel will never close because we have to end the async task
// to allow the stopper to completely finish
func (s *Server) AwaitNoConnections(ctx context.Context) <-chan struct{} {
	c := make(chan struct{})

	_ = s.Stopper.RunAsyncTask(ctx, "await-no-connections", func(context.Context) {
		for {
			connCount := s.metrics.CurConnCount.Value()
			if connCount == 0 {
				close(c)
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(awaitNoConnectionsInterval):
				continue
			}
		}

	})

	return c
}
