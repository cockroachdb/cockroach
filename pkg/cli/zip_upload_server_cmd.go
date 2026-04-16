// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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

	// Parse labels and node IDs (shared by both sync and async paths).
	labels := map[string]string{}
	for _, l := range uploadServerCtx.labels {
		for _, kv := range strings.Split(l, ",") {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) == 2 {
				labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	var nodeIDs []int32
	for _, id := range uploadServerCtx.nodeIDs {
		nodeIDs = append(nodeIDs, int32(id))
	}

	if uploadServerCtx.async {
		return runDebugZipUploadServerAsync(ctx, labels, nodeIDs)
	}
	return runDebugZipUploadServerSync(ctx, labels, nodeIDs)
}

func runDebugZipUploadServerSync(
	ctx context.Context, labels map[string]string, nodeIDs []int32,
) error {
	timeout := 10 * time.Minute
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}

	conn, finish, err := newClientConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "connecting to cluster")
	}
	defer finish()

	statusClient := conn.NewStatusClient()

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

func runDebugZipUploadServerAsync(
	ctx context.Context, labels map[string]string, nodeIDs []int32,
) error {
	// Create the job via the Admin RPC.
	conn, finish, err := newClientConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "connecting to cluster")
	}
	defer finish()

	adminClient := conn.NewAdminClient()

	req := &serverpb.StartUploadDebugDataRequest{
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

	resp, err := adminClient.StartUploadDebugData(ctx, req)
	if err != nil {
		return errors.Wrap(err, "creating upload job")
	}

	jobID := resp.JobID
	fmt.Fprintf(stderr, "Upload job created: %d\n", jobID)
	fmt.Fprintf(stderr, "Monitor in DB Console: /#/jobs/%d\n", jobID)
	fmt.Fprintf(stderr, "Or via SQL: SHOW JOB %d\n\n", jobID)

	// Open a SQL connection to poll job status. Use an internal app name so that
	// crdb_internal access is permitted without user-facing session overrides.
	const appName = catconstants.InternalAppNamePrefix + " cockroach debug zip upload-server"
	sqlConn, err := makeSQLClient(ctx, appName, useSystemDb)
	if err != nil {
		fmt.Fprintf(stderr, "Could not open SQL connection for polling: %v\n", err)
		fmt.Fprintf(stderr, "The job is running in the background. Check status with: SHOW JOB %d\n", jobID)
		return nil
	}
	defer func() { _ = sqlConn.Close() }()

	return pollUploadJob(ctx, sqlConn, jobID)
}

// pollUploadJob polls the job status until it reaches a terminal state,
// printing progress updates to stderr.
func pollUploadJob(ctx context.Context, sqlConn clisqlclient.Conn, jobID int64) error {
	start := timeutil.Now()
	lastStatus := ""
	pollInterval := 2 * time.Second

	const query = `SELECT status::STRING,
       COALESCE(running_status, '')::STRING,
       fraction_completed::FLOAT8,
       COALESCE(error, '')::STRING
FROM crdb_internal.jobs WHERE job_id = $1`

	for {
		row, err := sqlConn.QueryRow(ctx, query, jobID)
		if err != nil {
			return errors.Wrapf(err, "polling job %d", jobID)
		}
		if len(row) < 4 {
			return errors.Newf("job %d not found", jobID)
		}

		status := driverValueToString(row[0])
		runningStatus := driverValueToString(row[1])
		fraction := driverValueToFloat(row[2])
		jobErr := driverValueToString(row[3])

		if runningStatus != lastStatus && runningStatus != "" {
			elapsed := timeutil.Since(start).Round(time.Second)
			fmt.Fprintf(stderr, "[%s] %s (%.0f%% complete)\n",
				elapsed, runningStatus, fraction*100)
			lastStatus = runningStatus
		}

		switch status {
		case "succeeded":
			elapsed := timeutil.Since(start).Round(time.Second)
			fmt.Fprintf(stderr, "\nUpload complete (job %d, %s elapsed).\n", jobID, elapsed)
			return nil
		case "failed":
			return errors.Newf("upload job %d failed: %s", jobID, jobErr)
		case "canceled":
			return errors.Newf("upload job %d was canceled", jobID)
		case "reverting", "revert-failed":
			return errors.Newf("upload job %d is reverting: %s", jobID, jobErr)
		}

		select {
		case <-ctx.Done():
			fmt.Fprintf(stderr, "\nInterrupted. The job continues in the background: SHOW JOB %d\n", jobID)
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func driverValueToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(v)
	}
}

func driverValueToFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case []byte:
		f, _ := strconv.ParseFloat(string(val), 64)
		return f
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}
