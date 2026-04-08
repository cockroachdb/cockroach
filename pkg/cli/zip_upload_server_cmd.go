// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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

	// Connect to the cluster.
	conn, finish, err := newClientConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "connecting to cluster")
	}
	defer finish()

	statusClient := conn.NewStatusClient()

	// Parse labels.
	labels := map[string]string{}
	for _, l := range uploadServerCtx.labels {
		for _, kv := range strings.Split(l, ",") {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) == 2 {
				labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	// Convert node IDs from []int to []int32 for the proto.
	var nodeIDs []int32
	for _, id := range uploadServerCtx.nodeIDs {
		nodeIDs = append(nodeIDs, int32(id))
	}

	// Build the RPC request.
	req := &serverpb.UploadDebugDataRequest{
		ServerUrl:              uploadServerCtx.serverURL,
		ApiKey:                 uploadServerCtx.apiKey,
		Redact:                 zipCtx.redact,
		CpuProfSeconds:         int32(zipCtx.cpuProfDuration / time.Second),
		Labels:                 labels,
		IncludeRangeInfo:       zipCtx.includeRangeInfo,
		IncludeGoroutineStacks: zipCtx.includeStacks,
		ReuploadSessionId:      uploadServerCtx.reuploadSession,
		NodeIds:                nodeIDs,
	}

	if req.ReuploadSessionId != "" {
		fmt.Fprintf(stderr, "Reopening session %s and streaming debug data to %s...\n",
			req.ReuploadSessionId, uploadServerCtx.serverURL)
	} else {
		fmt.Fprintf(stderr, "Streaming debug data to %s...\n", uploadServerCtx.serverURL)
	}
	start := timeutil.Now()

	var resp *serverpb.UploadDebugDataResponse
	err = timeutil.RunWithTimeout(ctx, "upload debug data", timeout, func(ctx context.Context) error {
		var rpcErr error
		resp, rpcErr = statusClient.UploadDebugData(ctx, req)
		return rpcErr
	})
	if err != nil {
		return errors.Wrap(err, "uploading debug data")
	}

	elapsed := timeutil.Since(start)

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

	if resp.NodesFailed > 0 {
		var failedNodeArgs string
		for _, ns := range resp.NodeStatuses {
			if ns.NodeId != 0 && len(ns.Errors) > 0 {
				failedNodeArgs += fmt.Sprintf(" --node=%d", ns.NodeId)
			}
		}
		fmt.Fprintf(stderr, "\nTo retry failed nodes into the same session, run:\n")
		fmt.Fprintf(stderr, "  cockroach debug zip upload-server --reupload-session=%s%s ...\n",
			resp.SessionId, failedNodeArgs)
		return errors.Newf("%d node(s) failed during upload", resp.NodesFailed)
	}

	return nil
}
