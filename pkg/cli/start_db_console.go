// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ui/future"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var startDBConsoleCmd = &cobra.Command{
	Use:   "start-db-console",
	Short: "start a standalone DB Console server",
	Long: `
Start a standalone DB Console server that connects to a running CockroachDB node.
This allows for fast development iteration with hot reloading while keeping the
CockroachDB node running.

The console will be served on http://localhost:9080 by default.
`,
	Example: `  cockroach start-db-console --host=localhost:26257 --insecure`,
	Args:    cobra.NoArgs,
}

// GetStartDBConsoleCmd returns the start-db-console command for use in minimal builds.
func GetStartDBConsoleCmd() *cobra.Command {
	return startDBConsoleCmd
}

var dbConsoleCtx = struct {
	host     string
	port     string
	insecure bool
}{
	host:     "localhost:26257",
	port:     "9080",
	insecure: true,
}

func init() {
	startDBConsoleCmd.RunE = clierrorplus.MaybeDecorateError(runStartDBConsole)

	// Add flags
	startDBConsoleCmd.Flags().StringVar(
		&dbConsoleCtx.host,
		"host",
		dbConsoleCtx.host,
		"CockroachDB node address to connect to (host:port)")

	startDBConsoleCmd.Flags().StringVar(
		&dbConsoleCtx.port,
		"port",
		dbConsoleCtx.port,
		"Port to serve the DB Console on")

	startDBConsoleCmd.Flags().BoolVar(
		&dbConsoleCtx.insecure,
		"insecure",
		dbConsoleCtx.insecure,
		"Use insecure connection (development only)")
}

func runStartDBConsole(cmd *cobra.Command, _ []string) error {
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

	// Dial the node to get clients using simple gRPC dial
	// This is simpler for a development tool and doesn't require cluster ID matching
	adminClient, statusClient, tsClient, err := future.DialRemoteClients(ctx, dbConsoleCtx.host)
	if err != nil {
		return errors.Wrap(err, "failed to dial CockroachDB node")
	}

	// Verify connection by checking cluster info
	clusterResp, err := adminClient.Cluster(ctx, &serverpb.ClusterRequest{})
	if err != nil {
		return errors.Wrapf(err, "failed to connect to CockroachDB at %s", dbConsoleCtx.host)
	}

	// Get build info from the remote node
	detailsResp, err := statusClient.Details(ctx, &serverpb.DetailsRequest{NodeId: "1"})
	if err != nil {
		return errors.Wrap(err, "failed to get node details")
	}

	buildInfo := detailsResp.BuildInfo
	fmt.Printf("Connected to CockroachDB cluster: %s (version: %s)\n",
		clusterResp.ClusterID, buildInfo.Tag)

	// Extract version in vMajor.Minor format (e.g., "v24.3") from the tag
	// Tag format is typically "v24.3.0" or "v24.3.0-alpha.1"
	version := extractMajorMinorVersion(buildInfo.Tag)

	// Create the UI handler with actual build info from the server
	uiHandler := future.MakeFutureHandler(future.IndexHTMLArgs{
		Insecure:  dbConsoleCtx.insecure,
		NodeID:    detailsResp.NodeID.String(),
		Tag:       buildInfo.Tag,
		Version:   version,
		ClusterID: clusterResp.ClusterID,
		Admin:     adminClient,
		Status:    statusClient,
		TS:        tsClient,
	})

	// Create HTTP server
	listenAddr := fmt.Sprintf("localhost:%s", dbConsoleCtx.port)
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", listenAddr)
	}

	server := &http.Server{
		Handler: uiHandler,
	}

	fmt.Printf("DB Console available at http://%s/future\n", listenAddr)
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
		// Gracefully shutdown the HTTP server
		if err := server.Shutdown(ctx); err != nil {
			log.Dev.Warningf(ctx, "HTTP server shutdown error: %v", err)
		}
		return nil
	case err := <-errCh:
		return errors.Wrap(err, "HTTP server error")
	}
}

// extractMajorMinorVersion extracts the major.minor version from a build tag.
// For example: "v24.3.0" -> "v24.3", "v24.3.0-alpha.1" -> "v24.3"
func extractMajorMinorVersion(tag string) string {
	// Match vMAJOR.MINOR pattern (e.g., v24.3)
	re := regexp.MustCompile(`^v(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(tag)
	if len(matches) >= 3 {
		return fmt.Sprintf("v%s.%s", matches[1], matches[2])
	}
	// Fallback to the full tag if parsing fails
	return tag
}
