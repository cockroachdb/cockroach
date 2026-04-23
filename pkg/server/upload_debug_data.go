// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// uploadDebugDataFanOutBaseTimeout bounds how long the coordinator will
// wait for the slowest worker's UploadNodeDebugData RPC. It is augmented
// by CpuProfSeconds when CPU profiling is enabled (see uploadDebugDataFanOut).
const uploadDebugDataFanOutBaseTimeout = 30 * time.Minute

// UploadNodeDebugData on the tenant status server is a stub. Secondary
// tenants must not drive debug-data uploads, since the collection paths
// (engine stats, gossip, historical profiles, etc.) all require access to
// the underlying KV node.
func (t *statusServer) UploadNodeDebugData(
	ctx context.Context, _ *serverpb.UploadNodeDebugDataRequest,
) (*serverpb.UploadNodeDebugDataResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented,
		"upload node debug data is not supported on secondary tenants")
}

// UploadNodeDebugData is the worker RPC invoked by the UPLOAD_DEBUG_DATA
// job's coordinator on every target node. The receiver collects the
// per-node debug artifacts (stacks, profiles, logs, ranges, internal
// tables, etc.) and streams them directly to GCS using the credentials
// the coordinator provides.
//
// Per-artifact failures are reported in the response's Errors slice
// rather than aborting the whole RPC, so a partial failure on one artifact
// does not poison the rest of the node's collection.
func (s *systemStatusServer) UploadNodeDebugData(
	ctx context.Context, req *serverpb.UploadNodeDebugDataRequest,
) (*serverpb.UploadNodeDebugDataResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	if req.ServerUrl == "" || req.SessionId == "" || req.UploadToken == "" {
		return nil, status.Error(codes.InvalidArgument,
			"server_url, session_id and upload_token are required")
	}
	if req.GcsAccessToken == "" || req.GcsBucket == "" {
		return nil, status.Error(codes.InvalidArgument,
			"gcs_access_token and gcs_bucket are required")
	}

	nodeID := s.node.Descriptor.NodeID
	log.Ops.Infof(ctx, "uploading debug data for node %d (session %s)", nodeID, req.SessionId)

	client := newWorkerUploadClient(req.ServerUrl, req.SessionId, req.UploadToken)
	defer func() { _ = client.closeGCS() }()

	if err := client.initGCSClientWithCredentials(
		ctx, req.GcsAccessToken, req.GcsBucket, req.GcsPrefix,
	); err != nil {
		return nil, errors.Wrapf(err, "initializing GCS client for node %d", nodeID)
	}

	collector := nodeArtifactCollector{
		server: s,
		client: client,
		nodeID: nodeID,
	}
	artifacts, errs := collector.collect(ctx, req)
	return &serverpb.UploadNodeDebugDataResponse{
		ArtifactsUploaded: artifacts,
		Errors:            errs,
	}, nil
}

// nodeArtifactCollector bundles the shared state used while collecting and
// uploading the per-node artifacts for a single UploadNodeDebugData call.
// It keeps the method surface small and lets each step accumulate errors
// into the same slice without threading them through every signature.
type nodeArtifactCollector struct {
	server *systemStatusServer
	client *uploadDebugDataClient
	nodeID roachpb.NodeID

	artifacts int32
	errs      []string
}

// collect runs every per-node collection step. It never returns an error:
// per-step failures are appended to errs and the next step is still run,
// so the coordinator ends up with best-effort data plus a structured list
// of failures.
func (c *nodeArtifactCollector) collect(
	ctx context.Context, req *serverpb.UploadNodeDebugDataRequest,
) (int32, []string) {
	c.collectGoroutineStacks(ctx, req.IncludeGoroutineStacks)
	c.collectHeapProfile(ctx)
	c.collectCPUProfile(ctx, req.CpuProfSeconds)
	c.collectEngineStats(ctx)
	c.collectNodeDetails(ctx, req.Redact)
	c.collectGossip(ctx)
	c.collectNodeStatus(ctx)
	if req.IncludeRangeInfo {
		c.collectRanges(ctx, req.Redact)
	}
	c.collectLogFiles(ctx, req.Redact)
	c.collectHistoricalProfiles(ctx)
	c.collectPerNodeInternalTables(ctx, req.Redact)
	return c.artifacts, c.errs
}

// record accounts for one artifact outcome. If uploadErr is nil the
// artifact count is bumped; otherwise a human-readable error tagged with
// label is appended to errs.
func (c *nodeArtifactCollector) record(label string, uploadErr error) {
	if uploadErr != nil {
		c.errs = append(c.errs, fmt.Sprintf("%s: %v", label, uploadErr))
		return
	}
	c.artifacts++
}

// nodePath returns the per-node artifact path for the current node.
func (c *nodeArtifactCollector) nodePath(relative string) string {
	return fmt.Sprintf("nodes/%d/%s", c.nodeID, relative)
}

func (c *nodeArtifactCollector) collectGoroutineStacks(ctx context.Context, stopTheWorld bool) {
	// Always collect the two cheap goroutine profiles.
	if stopTheWorld {
		// Stop-the-world dump: expensive but most useful for debugging
		// deadlocks/stalls.
		stacks, err := stacksLocal(&serverpb.StacksRequest{
			Type: serverpb.StacksType_GOROUTINE_STACKS,
		})
		if err != nil {
			c.errs = append(c.errs, fmt.Sprintf("stacks: %v", err))
		} else {
			c.record("upload stacks", c.client.uploadArtifactBytes(ctx,
				c.nodePath("stacks.txt"), "text/plain", stacks.Data))
		}
	}

	stacksLabels, err := stacksLocal(&serverpb.StacksRequest{
		Type: serverpb.StacksType_GOROUTINE_STACKS_DEBUG_1,
	})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("stacks_with_labels: %v", err))
	} else {
		c.record("upload stacks_with_labels", c.client.uploadArtifactBytes(ctx,
			c.nodePath("stacks_with_labels.txt"), "text/plain", stacksLabels.Data))
	}

	stacksPprof, err := stacksLocal(&serverpb.StacksRequest{
		Type: serverpb.StacksType_GOROUTINE_STACKS_DEBUG_3,
	})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("stacks.pprof: %v", err))
	} else {
		c.record("upload stacks.pprof", c.client.uploadArtifactBytes(ctx,
			c.nodePath("stacks.pprof"), "application/octet-stream", stacksPprof.Data))
	}
}

func (c *nodeArtifactCollector) collectHeapProfile(ctx context.Context) {
	heap, err := c.server.Profile(ctx, &serverpb.ProfileRequest{
		NodeId: "local",
		Type:   serverpb.ProfileRequest_HEAP,
	})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("heap profile: %v", err))
		return
	}
	c.record("upload heap", c.client.uploadArtifactBytes(ctx,
		c.nodePath("heap.pprof"), "application/octet-stream", heap.Data))
}

func (c *nodeArtifactCollector) collectCPUProfile(ctx context.Context, cpuProfSeconds int32) {
	if cpuProfSeconds <= 0 {
		return
	}
	cpuProf, err := c.server.Profile(ctx, &serverpb.ProfileRequest{
		NodeId:  "local",
		Type:    serverpb.ProfileRequest_CPU,
		Seconds: cpuProfSeconds,
		Labels:  true,
	})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("cpu profile: %v", err))
		return
	}
	c.record("upload cpu", c.client.uploadArtifactBytes(ctx,
		c.nodePath("cpu.pprof"), "application/octet-stream", cpuProf.Data))
}

func (c *nodeArtifactCollector) collectEngineStats(ctx context.Context) {
	engineStats, err := c.server.EngineStats(ctx, &serverpb.EngineStatsRequest{NodeId: "local"})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("engine stats: %v", err))
		return
	}
	lsm := debug.FormatLSMStats(engineStats.StatsByStoreId)
	c.record("upload lsm", c.client.uploadArtifactBytes(ctx,
		c.nodePath("lsm.txt"), "text/plain", []byte(lsm)))
}

func (c *nodeArtifactCollector) collectNodeDetails(ctx context.Context, redactFlag bool) {
	details, err := c.server.Details(ctx, &serverpb.DetailsRequest{
		NodeId: "local",
		Redact: redactFlag,
	})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("details: %v", err))
		return
	}
	c.record("upload details", c.client.uploadArtifactJSON(ctx,
		c.nodePath("details.json"), details))
}

func (c *nodeArtifactCollector) collectGossip(ctx context.Context) {
	gossipInfo, err := c.server.Gossip(ctx, &serverpb.GossipRequest{NodeId: "local"})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("gossip: %v", err))
		return
	}
	c.record("upload gossip", c.client.uploadArtifactJSON(ctx,
		c.nodePath("gossip.json"), gossipInfo))
}

func (c *nodeArtifactCollector) collectNodeStatus(ctx context.Context) {
	nodesResp, err := c.server.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("status: %v", err))
		return
	}
	var localStatus *statuspb.NodeStatus
	for i := range nodesResp.Nodes {
		if nodesResp.Nodes[i].Desc.NodeID == c.nodeID {
			localStatus = &nodesResp.Nodes[i]
			break
		}
	}
	if localStatus == nil {
		return
	}
	c.record("upload status", c.client.uploadArtifactJSON(ctx,
		c.nodePath("status.json"), localStatus))
}

func (c *nodeArtifactCollector) collectRanges(ctx context.Context, redactFlag bool) {
	rangesResp, err := c.server.Ranges(ctx, &serverpb.RangesRequest{
		NodeId: "local",
		Redact: redactFlag,
	})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("ranges: %v", err))
		return
	}
	// Sort ranges by RangeID to match the debug zip artifact layout and
	// give operators a stable diff across multiple uploads.
	sort.Slice(rangesResp.Ranges, func(i, j int) bool {
		return rangesResp.Ranges[i].State.Desc.RangeID < rangesResp.Ranges[j].State.Desc.RangeID
	})
	c.record("upload ranges", c.client.uploadArtifactJSON(ctx,
		c.nodePath("ranges.json"), rangesResp))
}

// collectLogFiles enumerates this node's log files and streams each of
// them to GCS via an io.Pipe, so formatted log entries never need to be
// materialized in a single contiguous buffer.
func (c *nodeArtifactCollector) collectLogFiles(ctx context.Context, redactFlag bool) {
	logFiles, err := c.server.LogFilesList(ctx, &serverpb.LogFilesListRequest{NodeId: "local"})
	if err != nil {
		c.errs = append(c.errs, fmt.Sprintf("log files list: %v", err))
		return
	}

	for _, file := range logFiles.Files {
		entries, err := c.server.LogFile(ctx, &serverpb.LogFileRequest{
			NodeId: "local",
			File:   file.Name,
			Redact: redactFlag,
		})
		if err != nil {
			c.errs = append(c.errs, fmt.Sprintf("log file %s: %v", file.Name, err))
			continue
		}

		// Snapshot the values we need on the closure; entries is owned by
		// the response object and safe to read from the producer goroutine.
		logEntries := entries.Entries
		artifactPath := c.nodePath(fmt.Sprintf("logs/%s", file.Name))
		doRedact := redactFlag

		uploadErr := c.client.uploadArtifact(ctx, artifactPath, "text/plain",
			func() (io.ReadCloser, error) {
				pr, pw := io.Pipe()
				go func() {
					defer func() { _ = pw.Close() }()
					for _, e := range logEntries {
						if doRedact && !e.Redactable {
							e.Message = "REDACTEDBYZIP"
						}
						if fmtErr := log.FormatLegacyEntry(e, pw); fmtErr != nil {
							_ = pw.CloseWithError(fmtErr)
							return
						}
					}
				}()
				return pr, nil
			})
		c.record(fmt.Sprintf("upload log %s", file.Name), uploadErr)
	}
}

// historicalProfileKinds lists the kinds of historical profile dumps we
// upload, along with the subdirectory they are placed in. Mirrors the
// layout used by `cockroach debug zip`.
var historicalProfileKinds = []struct {
	fileType serverpb.FileType
	subdir   string
}{
	{serverpb.FileType_HEAP, "heapprof"},
	{serverpb.FileType_GOROUTINES, "goroutines"},
	{serverpb.FileType_CPU, "cpuprof"},
	{serverpb.FileType_EXECUTIONTRACE, "executiontraces"},
}

// collectHistoricalProfiles uploads on-disk historical heap / goroutine /
// CPU profile and execution trace files, one artifact per file.
func (c *nodeArtifactCollector) collectHistoricalProfiles(ctx context.Context) {
	for _, kind := range historicalProfileKinds {
		fileList, err := c.server.GetFiles(ctx, &serverpb.GetFilesRequest{
			NodeId:   "local",
			Type:     kind.fileType,
			ListOnly: true,
		})
		if err != nil {
			c.errs = append(c.errs, fmt.Sprintf("%s list: %v", kind.subdir, err))
			continue
		}

		for _, f := range fileList.Files {
			fileData, err := c.server.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   "local",
				Type:     kind.fileType,
				Patterns: []string{f.Name},
				ListOnly: false,
			})
			if err != nil {
				c.errs = append(c.errs, fmt.Sprintf("%s/%s: %v", kind.subdir, f.Name, err))
				continue
			}
			if len(fileData.Files) == 0 {
				continue
			}
			c.record(
				fmt.Sprintf("upload %s/%s", kind.subdir, f.Name),
				c.client.uploadArtifactBytes(ctx,
					c.nodePath(fmt.Sprintf("%s/%s", kind.subdir, f.Name)),
					"application/octet-stream",
					fileData.Files[0].Contents),
			)
		}
	}
}

// collectPerNodeInternalTables dumps the per-node crdb_internal tables
// defined in uploadPerNodeTables as TSV files under nodes/<nid>/.
func (c *nodeArtifactCollector) collectPerNodeInternalTables(ctx context.Context, redactFlag bool) {
	for _, table := range uploadPerNodeTables {
		artifact := c.nodePath(fmt.Sprintf("%s.txt", sanitizeTableName(table.name)))
		if err := dumpTableToTSV(ctx, c.server.internalExecutor, c.client,
			table, artifact, redactFlag,
		); err != nil {
			c.errs = append(c.errs, fmt.Sprintf("table %s: %v", table.name, err))
		} else {
			c.artifacts++
		}
	}
}

// uploadDebugDataClusterInfo returns the cluster id and commissioned node
// count. Used by the UPLOAD_DEBUG_DATA job resumer to create (or validate)
// an upload-server session.
func (s *systemStatusServer) uploadDebugDataClusterInfo(
	ctx context.Context,
) (clusterID string, nodeCount int, err error) {
	nodeStatuses, err := s.serverIterator.getAllNodes(ctx)
	if err != nil {
		return "", 0, srverrors.ServerError(ctx, err)
	}
	return s.rpcCtx.StorageClusterID.Get().String(), len(nodeStatuses), nil
}

// uploadDebugDataFanOut dispatches the per-node UploadNodeDebugData RPCs
// in parallel and reports results through progressFn / errorFn. It is the
// backing implementation for sql.UploadDebugDataFanOutFn.
//
// If nodeIDs is non-empty, only those nodes receive the worker RPC. The
// remaining nodes receive a no-op response so iterateNodes still reports
// cluster completeness. This matches the "retry failed nodes" flow, which
// reuses the same session id and only re-runs the failed subset.
func (s *systemStatusServer) uploadDebugDataFanOut(
	ctx context.Context,
	req *serverpb.UploadNodeDebugDataRequest,
	nodeIDs []int32,
	progressFn func(nodeID roachpb.NodeID, resp *serverpb.UploadNodeDebugDataResponse),
	errorFn func(nodeID roachpb.NodeID, err error),
) error {
	requested := make(map[int32]bool, len(nodeIDs))
	for _, nid := range nodeIDs {
		requested[nid] = true
	}

	nodeFn := func(
		ctx context.Context,
		statusClient serverpb.RPCStatusClient,
		nodeID roachpb.NodeID,
	) (*serverpb.UploadNodeDebugDataResponse, error) {
		if len(requested) > 0 && !requested[int32(nodeID)] {
			return &serverpb.UploadNodeDebugDataResponse{}, nil
		}
		return statusClient.UploadNodeDebugData(ctx, req)
	}

	fanoutTimeout := uploadDebugDataFanOutBaseTimeout
	if req.CpuProfSeconds > 0 {
		fanoutTimeout += time.Duration(req.CpuProfSeconds) * time.Second
	}

	return iterateNodes(
		ctx,
		s.serverIterator,
		s.stopper,
		redact.Sprintf("uploading debug data"),
		fanoutTimeout,
		s.dialNode,
		nodeFn,
		progressFn,
		errorFn,
	)
}

// uploadClusterData collects and uploads cluster-scoped status-RPC
// artifacts: node list, problem ranges, and tenant-ranges-by-locality.
// It is called once per upload session by the coordinator (the resumer).
func (s *systemStatusServer) uploadClusterData(
	ctx context.Context, client *uploadDebugDataClient, includeRangeInfo bool,
) (artifacts int32, errs []string) {
	// /cluster/nodes.json
	nodesResp, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		errs = append(errs, fmt.Sprintf("cluster nodes: %v", err))
	} else {
		if uploadErr := client.uploadArtifactJSON(
			ctx, "cluster/nodes.json", nodesResp,
		); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload cluster nodes: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	if !includeRangeInfo {
		return artifacts, errs
	}

	// /cluster/reports/problemranges.json
	problemRanges, err := s.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
	if err != nil {
		errs = append(errs, fmt.Sprintf("problem ranges: %v", err))
	} else {
		if uploadErr := client.uploadArtifactJSON(
			ctx, "cluster/reports/problemranges.json", problemRanges,
		); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload problem ranges: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// /cluster/tenant_ranges/<locality>.json, one file per locality (sorted
	// so the artifact set is deterministic across runs).
	tenantRanges, err := s.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
	if err != nil {
		errs = append(errs, fmt.Sprintf("tenant ranges: %v", err))
		return artifacts, errs
	}
	localities := make([]string, 0, len(tenantRanges.RangesByLocality))
	for k := range tenantRanges.RangesByLocality {
		localities = append(localities, k)
	}
	sort.Strings(localities)
	for _, locality := range localities {
		rangeList := tenantRanges.RangesByLocality[locality]
		artifact := fmt.Sprintf("cluster/tenant_ranges/%s.json", locality)
		if uploadErr := client.uploadArtifactJSON(ctx, artifact, rangeList); uploadErr != nil {
			errs = append(errs,
				fmt.Sprintf("upload tenant ranges %s: %v", locality, uploadErr))
		} else {
			artifacts++
		}
	}

	return artifacts, errs
}

// uploadClusterTables dumps the cluster-scoped crdb_internal and system
// tables (uploadClusterWideTables ∪ uploadSystemTables) as TSV artifacts
// under cluster/.
func (s *systemStatusServer) uploadClusterTables(
	ctx context.Context, client *uploadDebugDataClient, redactFlag bool,
) (artifacts int32, errs []string) {
	total := len(uploadClusterWideTables) + len(uploadSystemTables)
	all := make([]uploadTableDef, 0, total)
	all = append(all, uploadClusterWideTables...)
	all = append(all, uploadSystemTables...)

	for _, table := range all {
		artifact := fmt.Sprintf("cluster/%s.txt", sanitizeTableName(table.name))
		if err := dumpTableToTSV(ctx, s.internalExecutor, client,
			table, artifact, redactFlag,
		); err != nil {
			errs = append(errs, fmt.Sprintf("table %s: %v", table.name, err))
			continue
		}
		artifacts++
	}
	return artifacts, errs
}

// dumpTableToTSV runs the query associated with table (choosing the
// redacted form when requested, falling back to queryFallback if the
// primary fails in the unredacted case), formats the rows as
// tab-separated text with a header row, and uploads the result as
// artifactPath.
//
// Rows already seen before an error are still uploaded, to preserve
// partial information.
func dumpTableToTSV(
	ctx context.Context,
	ie *sql.InternalExecutor,
	client *uploadDebugDataClient,
	table uploadTableDef,
	artifactPath string,
	redactFlag bool,
) error {
	query := table.query
	if redactFlag && table.queryRedacted != "" {
		query = table.queryRedacted
	}

	it, err := ie.QueryIteratorEx(
		ctx,
		redact.RedactableString("upload-"+table.name),
		nil, // txn
		sessiondata.NodeUserSessionDataOverride,
		query,
	)
	if err != nil && !redactFlag && table.queryFallback != "" {
		it, err = ie.QueryIteratorEx(
			ctx,
			redact.RedactableString("upload-"+table.name+"-fallback"),
			nil,
			sessiondata.NodeUserSessionDataOverride,
			table.queryFallback,
		)
	}
	if err != nil {
		return errors.Wrap(err, "query")
	}

	var buf bytes.Buffer
	cols := it.Types()
	for i, col := range cols {
		if i > 0 {
			buf.WriteByte('\t')
		}
		buf.WriteString(col.Name)
	}
	buf.WriteByte('\n')

	var iterErr error
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			iterErr = err
			break
		}
		if !ok {
			break
		}
		row := it.Cur()
		for i, d := range row {
			if i > 0 {
				buf.WriteByte('\t')
			}
			if d == tree.DNull {
				buf.WriteString("NULL")
			} else {
				buf.WriteString(d.String())
			}
		}
		buf.WriteByte('\n')
	}
	if closeErr := it.Close(); closeErr != nil {
		iterErr = errors.CombineErrors(iterErr, closeErr)
	}

	// Always upload whatever rows we already serialized, even if the
	// iteration failed partway through: partial data is strictly more
	// useful than nothing for debugging.
	if uploadErr := client.uploadArtifactBytes(
		ctx, artifactPath, "text/tab-separated-values", buf.Bytes(),
	); uploadErr != nil {
		return errors.CombineErrors(iterErr, uploadErr)
	}
	return iterErr
}

// sanitizeTableName makes a table name safe to embed in a GCS object
// name: dots become underscores so that GCS-side tools treat each table
// as a distinct file rather than inferring an extension.
func sanitizeTableName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}
