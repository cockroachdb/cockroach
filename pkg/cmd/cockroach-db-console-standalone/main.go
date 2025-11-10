// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// cockroach-db-console-standalone is a minimal binary for UI development
// that has ZERO dependencies on pkg/cli to keep builds fast.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ui/future"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Parse simple flags
	host := "localhost:26257"
	port := "9080"

	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]

		// Handle --flag=value format
		if len(arg) > 7 && arg[:7] == "--host=" {
			host = arg[7:]
			continue
		}
		if len(arg) > 7 && arg[:7] == "--port=" {
			port = arg[7:]
			continue
		}

		// Handle --flag value format
		switch arg {
		case "--host":
			if i+1 < len(os.Args) {
				host = os.Args[i+1]
				i++ // Skip next arg
			}
		case "--port":
			if i+1 < len(os.Args) {
				port = os.Args[i+1]
				i++ // Skip next arg
			}
		case "--help", "-h":
			fmt.Println("Usage: cockroach-db-console-standalone [--host=localhost:26257] [--port=9080]")
			fmt.Println("       cockroach-db-console-standalone [--host localhost:26257] [--port 9080]")
			fmt.Println("")
			fmt.Println("Minimal DB Console server for fast UI development iteration.")
			fmt.Println("Connects to a running CockroachDB node and serves the UI.")
			return nil
		}
	}

	ctx := context.Background()

	// Create a stopper for graceful shutdown
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Set up signal handling
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		fmt.Fprintln(os.Stderr, "\nShutting down DB Console...")
		stopper.Stop(ctx)
	}()

	// Dial the node using simple gRPC
	adminClient, statusClient, tsClient, err := future.DialRemoteClients(ctx, host)
	if err != nil {
		return errors.Wrap(err, "failed to dial CockroachDB node")
	}

	// Verify connection by checking cluster info
	clusterResp, err := adminClient.Cluster(ctx, &serverpb.ClusterRequest{})
	if err != nil {
		return errors.Wrapf(err, "failed to connect to CockroachDB at %s", host)
	}

	// Get build info from the remote node
	detailsResp, err := statusClient.Details(ctx, &serverpb.DetailsRequest{NodeId: "1"})
	if err != nil {
		return errors.Wrap(err, "failed to get node details")
	}

	buildInfo := detailsResp.BuildInfo
	fmt.Printf("Connected to CockroachDB cluster: %s (version: %s)\n",
		clusterResp.ClusterID, buildInfo.Tag)

	// Extract version in vMajor.Minor format from tag
	version := extractMajorMinorVersion(buildInfo.Tag)

	// Create the UI handler
	uiHandler := future.MakeFutureHandler(future.IndexHTMLArgs{
		Insecure:  true, // Always insecure for dev
		NodeID:    detailsResp.NodeID.String(),
		Tag:       buildInfo.Tag,
		Version:   version,
		ClusterID: clusterResp.ClusterID,
		Admin:     adminClient,
		Status:    statusClient,
		TS:        tsClient,
	})

	// Create HTTP server
	listenAddr := fmt.Sprintf("localhost:%s", port)
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", listenAddr)
	}

	server := &http.Server{
		Handler: uiHandler,
	}

	fmt.Printf("DB Console available at http://%s\n", listenAddr)
	fmt.Println("Press Ctrl+C to stop")

	// Start serving in a goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Wait for shutdown or error
	select {
	case <-stopper.ShouldQuiesce():
		if err := server.Shutdown(ctx); err != nil {
			log.Dev.Warningf(ctx, "HTTP server shutdown error: %v", err)
		}
		return nil
	case err := <-errCh:
		return errors.Wrap(err, "HTTP server error")
	}
}

func extractMajorMinorVersion(tag string) string {
	re := regexp.MustCompile(`^v(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(tag)
	if len(matches) >= 3 {
		return fmt.Sprintf("v%s.%s", matches[1], matches[2])
	}
	return tag
}
