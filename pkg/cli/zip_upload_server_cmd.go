// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugZipUploadServerCmd = &cobra.Command{
	Use:   "upload-server",
	Short: "stream debug data directly to a CRL upload server",
	Long: `
Stream debug data from all nodes directly to a CRL-hosted upload server,
bypassing the local zip file. Each node uploads its own data in parallel.

Requires --upload-server-url and --upload-server-api-key (or the
COCKROACH_UPLOAD_SERVER_API_KEY environment variable).

If the coordinator node fails mid-upload, the CLI automatically retries
on a fallback host (discovered from the cluster or via --fallback-hosts),
resuming the same upload session.
`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(runDebugZipUploadServer),
}

func runDebugZipUploadServer(cmd *cobra.Command, args []string) error {
	if uploadServerCtx.serverURL == "" {
		return errors.New("--upload-server-url is required")
	}
	if uploadServerCtx.apiKey == "" {
		return errors.New("--upload-server-api-key is required (or set COCKROACH_UPLOAD_SERVER_API_KEY)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if uploadServerCtx.fromFile != "" {
		return runUploadFromZipFile(ctx, uploadServerCtx.fromFile)
	}

	timeout := 10 * time.Minute
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}

	// Parse labels.
	labels := parseUploadLabels()

	// Build the RPC request.
	req := &serverpb.UploadDebugDataRequest{
		ServerUrl:              uploadServerCtx.serverURL,
		ApiKey:                 uploadServerCtx.apiKey,
		Redact:                 zipCtx.redact,
		CpuProfSeconds:         int32(zipCtx.cpuProfDuration / time.Second),
		Labels:                 labels,
		IncludeRangeInfo:       zipCtx.includeRangeInfo,
		IncludeGoroutineStacks: zipCtx.includeStacks,
	}

	// Build the candidate host list for failover.
	primaryHost := serverCfg.Addr
	candidates, err := buildCandidateHosts(ctx, primaryHost)
	if err != nil {
		// Non-fatal: we can still try the primary.
		fmt.Fprintf(stderr, "Warning: could not discover fallback hosts: %v\n", err)
		candidates = []string{primaryHost}
	}

	fmt.Fprintf(stderr, "Streaming debug data to %s...\n", uploadServerCtx.serverURL)
	start := timeutil.Now()

	// Track session info across retries.
	var sessionID, uploadToken string
	var resp *serverpb.UploadDebugDataResponse
	var attemptedHosts []string

	for i, host := range candidates {
		attemptedHosts = append(attemptedHosts, host)

		// If we have a session_id from a previous attempt, set it
		// on the request so the coordinator resumes instead of
		// creating a new session.
		if sessionID != "" {
			req.SessionId = sessionID
			req.UploadToken = uploadToken
			fmt.Fprintf(stderr, "Retrying on %s (resuming session %s)...\n", host, sessionID)
		}

		var attemptErr error
		resp, attemptErr = tryUploadToHost(ctx, host, req, timeout, &sessionID, &uploadToken)
		if attemptErr == nil {
			// Upload completed.
			break
		}

		if !isRetriableCoordinatorError(attemptErr) {
			return errors.Wrapf(attemptErr, "uploading debug data to %s", host)
		}

		// Retriable error: log and try next host.
		fmt.Fprintf(stderr, "Upload to %s failed (retriable): %v\n", host, attemptErr)
		if i == len(candidates)-1 {
			return errors.Wrapf(
				attemptErr,
				"upload failed on all hosts (%s)",
				strings.Join(attemptedHosts, ", "),
			)
		}
	}

	elapsed := timeutil.Since(start)
	printUploadResult(resp, elapsed)

	if resp.NodesFailed > 0 {
		return errors.Newf("%d node(s) failed during upload", resp.NodesFailed)
	}
	return nil
}

// parseUploadLabels parses the --upload-server-labels flag values into
// a string map.
func parseUploadLabels() map[string]string {
	labels := map[string]string{}
	for _, l := range uploadServerCtx.labels {
		for _, kv := range strings.Split(l, ",") {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) == 2 {
				labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	return labels
}

// buildCandidateHosts returns the ordered list of hosts to try. If
// --fallback-hosts is provided, those are appended to the primary.
// Otherwise, the CLI auto-discovers node addresses from the cluster.
func buildCandidateHosts(ctx context.Context, primaryHost string) ([]string, error) {
	if len(uploadServerCtx.fallbackHosts) > 0 {
		candidates := []string{primaryHost}
		candidates = append(candidates, uploadServerCtx.fallbackHosts...)
		return candidates, nil
	}

	// Auto-discover fallback hosts from the cluster.
	conn, finish, err := newClientConn(ctx, serverCfg)
	if err != nil {
		// Can't connect at all — return primary only.
		return []string{primaryHost}, errors.Wrap(err, "connecting for node discovery")
	}
	defer finish()

	statusClient := conn.NewStatusClient()
	nodesResp, err := statusClient.NodesList(ctx, &serverpb.NodesListRequest{})
	if err != nil {
		return []string{primaryHost}, errors.Wrap(err, "listing nodes")
	}

	candidates := []string{primaryHost}
	for _, node := range nodesResp.Nodes {
		addr := node.Address.AddressField
		if addr != "" && addr != primaryHost {
			candidates = append(candidates, addr)
		}
	}
	return candidates, nil
}

// tryUploadToHost opens a streaming UploadDebugData RPC to the given
// host and reads the session_id from the first event. If the stream
// completes, it returns the final response. The sessionID and
// uploadToken pointers are updated as soon as a SessionCreated event
// is received, so the caller can use them for retry even if the
// stream breaks.
func tryUploadToHost(
	ctx context.Context,
	host string,
	req *serverpb.UploadDebugDataRequest,
	timeout time.Duration,
	sessionID *string,
	uploadToken *string,
) (*serverpb.UploadDebugDataResponse, error) {
	// Temporarily set the target address for this attempt.
	origAddr := serverCfg.Addr
	serverCfg.Addr = host
	defer func() { serverCfg.Addr = origAddr }()

	conn, finish, err := newClientConn(ctx, serverCfg)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to host")
	}
	defer finish()

	statusClient := conn.NewStatusClient()

	var resp *serverpb.UploadDebugDataResponse
	err = timeutil.RunWithTimeout(ctx, "upload debug data", timeout, func(ctx context.Context) error {
		stream, streamErr := statusClient.UploadDebugData(ctx, req)
		if streamErr != nil {
			return streamErr
		}

		for {
			event, recvErr := stream.Recv()
			if recvErr == io.EOF {
				return nil
			}
			if recvErr != nil {
				return recvErr
			}

			switch e := event.Event.(type) {
			case *serverpb.UploadDebugDataEvent_SessionCreated:
				*sessionID = e.SessionCreated.SessionId
				*uploadToken = e.SessionCreated.UploadToken
			case *serverpb.UploadDebugDataEvent_Completed:
				resp = e.Completed
			}
		}
	})
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return nil, errors.New("stream ended without a completion event")
	}
	return resp, nil
}

// isRetriableCoordinatorError returns true if the error indicates a
// transient coordinator failure (connection lost, timeout) that
// should be retried on a different host.
func isRetriableCoordinatorError(err error) bool {
	if grpcutil.IsConnectionUnavailable(err) {
		return true
	}
	if grpcutil.IsClosedConnection(err) {
		return true
	}
	if grpcutil.IsTimeout(err) {
		return true
	}
	// Network-level errors (connection refused, reset, etc.).
	if opErr := (*net.OpError)(nil); errors.As(err, &opErr) {
		return true
	}
	// Don't retry auth errors — they'll fail on every host.
	if grpcutil.IsAuthError(err) {
		return false
	}
	if grpcutil.IsConnectionRejected(err) {
		return false
	}
	return false
}

// printUploadResult prints the final upload result to stderr.
func printUploadResult(resp *serverpb.UploadDebugDataResponse, elapsed time.Duration) {
	fmt.Fprintf(stderr, "\nUpload complete.\n")
	fmt.Fprintf(stderr, "  Session ID:         %s\n", resp.SessionId)
	fmt.Fprintf(stderr, "  Artifacts uploaded:  %d\n", resp.ArtifactsUploaded)
	fmt.Fprintf(stderr, "  Nodes succeeded:    %d\n", resp.NodesSucceeded)
	fmt.Fprintf(stderr, "  Nodes failed:       %d\n", resp.NodesFailed)
	fmt.Fprintf(stderr, "  Duration:           %s\n", elapsed.Round(time.Second))
	if resp.CloudPath != "" {
		fmt.Fprintf(stderr, "  Cloud path:         %s\n", resp.CloudPath)
	}

	// Print per-node errors if any.
	for _, ns := range resp.NodeStatuses {
		if len(ns.Errors) > 0 {
			fmt.Fprintf(stderr, "\n  Node %d errors:\n", ns.NodeId)
			for _, e := range ns.Errors {
				fmt.Fprintf(stderr, "    - %s\n", e)
			}
		}
	}
}
