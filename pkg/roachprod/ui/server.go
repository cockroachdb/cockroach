//go:build bazel

// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io/fs"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/assetbundle"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/websocket"
)

//go:embed assets.tar.zst
var assetsBytes []byte

var assets fs.FS

func init() {
	var err error
	assets, err = assetbundle.AsFS(bytes.NewBuffer(assetsBytes))
	if err != nil {
		panic(err)
	}
}

// Server represents the roachprod UI HTTP server.
type Server struct {
	port            int
	logger          *logger.Logger
	server          *http.Server
	clusterManager  ClusterManager
	NewCluster      NewClusterFunc
	logBroadcaster  *logBroadcasterImpl
	ssoTokenExpired atomic.Bool // Track if AWS SSO token is expired
	upgrader        websocket.Upgrader
}

// NewClusterFunc is a function type for creating a new SyncedCluster.
type NewClusterFunc func(l *logger.Logger, name string) (ClusterOperations, error)

// NewServer creates a new UI server instance.
func NewServer(port int, l *logger.Logger) *Server {
	return &Server{
		port:           port,
		logger:         l,
		logBroadcaster: NewLogBroadcaster(),
		upgrader:       createUpgrader(port),
	}
}

// Start starts the HTTP server and blocks until context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/clusters", s.corsMiddleware(s.handleClusters))
	mux.HandleFunc("/api/clusters/create", s.corsMiddleware(s.handleCreateCluster))
	mux.HandleFunc("/api/clusters/delete", s.corsMiddleware(s.handleDeleteClusters))
	mux.HandleFunc("/api/clusters/", s.corsMiddleware(s.handleClusterActions))
	mux.HandleFunc("/api/aws-sso-login", s.corsMiddleware(s.handleAWSSSOLogin))
	mux.HandleFunc("/api/aws-sso-status", s.corsMiddleware(s.handleSSOStatus))
	mux.HandleFunc("/api/cloud-providers/", s.corsMiddleware(s.handleCloudProviderOptions))
	mux.HandleFunc("/api/current-user", s.corsMiddleware(s.handleCurrentUser))
	mux.HandleFunc("/api/ssh/", s.handleSSH)
	mux.HandleFunc("/api/logs", s.handleLogs)

	// Serve static files from the asset bundle
	// The assets fs.FS contains the files at the root level
	fileServer := http.FileServer(http.FS(assets))
	mux.Handle("/", fileServer)

	s.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           s.loggingMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      5 * time.Minute, // Allow for long-running operations like cluster creation
		IdleTimeout:       120 * time.Second,
	}

	s.logger.Printf("Starting roachprod UI server on http://localhost:%d", s.port)
	s.logger.Printf("Press Ctrl+C to stop")

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		s.logger.Printf("Shutting down roachprod UI server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// loggingMiddleware logs HTTP requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}

// corsMiddleware adds CORS headers for localhost development.
func (s *Server) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		// Only allow localhost origins to prevent CSRF attacks
		expectedLocalhost := fmt.Sprintf("http://localhost:%d", s.port)
		expected127 := fmt.Sprintf("http://127.0.0.1:%d", s.port)
		if origin == expectedLocalhost || origin == expected127 {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

