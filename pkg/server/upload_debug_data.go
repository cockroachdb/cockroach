// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UploadDebugData is the tenant-level stub. Secondary tenants cannot
// use this RPC.
func (t *statusServer) UploadDebugData(
	ctx context.Context, req *serverpb.UploadDebugDataRequest,
) (*serverpb.UploadDebugDataResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "upload debug data is not supported on secondary tenants")
}

// UploadNodeDebugData is the tenant-level stub. Secondary tenants
// cannot use this RPC.
func (t *statusServer) UploadNodeDebugData(
	ctx context.Context, req *serverpb.UploadNodeDebugDataRequest,
) (*serverpb.UploadNodeDebugDataResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "upload node debug data is not supported on secondary tenants")
}

// UploadDebugData on the system status server coordinates the debug
// data upload across all nodes. It:
//  1. Creates a session on the upload server.
//  2. Fans out UploadNodeDebugData RPCs to all (or selected) nodes.
//  3. Completes the session on the upload server.
func (s *systemStatusServer) UploadDebugData(
	ctx context.Context, req *serverpb.UploadDebugDataRequest,
) (*serverpb.UploadDebugDataResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	if req.ServerUrl == "" {
		return nil, status.Error(codes.InvalidArgument, "server_url is required")
	}
	if req.ApiKey == "" {
		return nil, status.Error(codes.InvalidArgument, "api_key is required")
	}

	// Get the node count and cluster info for session creation.
	nodeStatuses, err := s.serverIterator.getAllNodes(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	nodeCount := len(nodeStatuses)

	clusterID := s.rpcCtx.StorageClusterID.Get().String()

	// Create session on the upload server.
	client := newUploadServerClientForRPC(req.ServerUrl, req.ApiKey, 5*time.Minute)
	if err := client.createSession(ctx, clusterID, nodeCount, req.Redact, req.Labels); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	log.Ops.Infof(ctx, "upload session created: %s", client.sessionID)

	// Prepare the per-node request.
	nodeReq := &serverpb.UploadNodeDebugDataRequest{
		ServerUrl:              req.ServerUrl,
		SessionId:              client.sessionID,
		UploadToken:            client.uploadToken,
		Redact:                 req.Redact,
		CpuProfSeconds:         req.CpuProfSeconds,
		IncludeRangeInfo:       req.IncludeRangeInfo,
		IncludeGoroutineStacks: req.IncludeGoroutineStacks,
	}

	// Build the set of requested node IDs (empty = all).
	requestedNodes := map[int32]bool{}
	for _, nid := range req.NodeIds {
		requestedNodes[nid] = true
	}

	// Fan out to all nodes.
	var mu syncutil.Mutex
	var nodeStatuses2 []serverpb.NodeUploadStatus
	var totalArtifacts int32
	var nodesSucceeded, nodesFailed int32
	var nodesCompleted []int32

	nodeFn := func(
		ctx context.Context,
		statusClient serverpb.RPCStatusClient,
		nodeID roachpb.NodeID,
	) (*serverpb.UploadNodeDebugDataResponse, error) {
		// Skip nodes not in the requested set (if specified).
		if len(requestedNodes) > 0 && !requestedNodes[int32(nodeID)] {
			return &serverpb.UploadNodeDebugDataResponse{}, nil
		}
		return statusClient.UploadNodeDebugData(ctx, nodeReq)
	}

	responseFn := func(nodeID roachpb.NodeID, resp *serverpb.UploadNodeDebugDataResponse) {
		mu.Lock()
		defer mu.Unlock()
		nStatus := serverpb.NodeUploadStatus{
			NodeId:            int32(nodeID),
			ArtifactsUploaded: resp.ArtifactsUploaded,
			Errors:            resp.Errors,
		}
		nodeStatuses2 = append(nodeStatuses2, nStatus)
		totalArtifacts += resp.ArtifactsUploaded
		if len(resp.Errors) > 0 {
			nodesFailed++
		} else {
			nodesSucceeded++
			nodesCompleted = append(nodesCompleted, int32(nodeID))
		}
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		mu.Lock()
		defer mu.Unlock()
		nodesFailed++
		nodeStatuses2 = append(nodeStatuses2, serverpb.NodeUploadStatus{
			NodeId: int32(nodeID),
			Errors: []string{err.Error()},
		})
		log.Dev.Warningf(ctx, "upload failed for node %d: %v", nodeID, err)
	}

	// Use a generous timeout for the fan-out since log files, profile
	// collection, and table dumps can be slow.
	fanoutTimeout := 30 * time.Minute
	if req.CpuProfSeconds > 0 {
		fanoutTimeout += time.Duration(req.CpuProfSeconds) * time.Second
	}

	if err := iterateNodes(
		ctx,
		s.serverIterator,
		s.stopper,
		redact.Sprintf("uploading debug data"),
		fanoutTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// Complete the session.
	if err := client.completeSession(ctx, int(totalArtifacts), nodesCompleted); err != nil {
		log.Dev.Warningf(ctx, "failed to complete upload session: %v", err)
	}

	return &serverpb.UploadDebugDataResponse{
		SessionId:         client.sessionID,
		ArtifactsUploaded: totalArtifacts,
		NodesSucceeded:    nodesSucceeded,
		NodesFailed:       nodesFailed,
		NodeStatuses:      nodeStatuses2,
	}, nil
}

// UploadNodeDebugData collects and uploads debug data from the local
// node directly to the upload server.
func (s *systemStatusServer) UploadNodeDebugData(
	ctx context.Context, req *serverpb.UploadNodeDebugDataRequest,
) (*serverpb.UploadNodeDebugDataResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	nodeID := s.node.Descriptor.NodeID
	log.Ops.Infof(ctx, "uploading debug data for node %d", nodeID)

	client := newUploadServerClientForNodeRPC(
		req.ServerUrl, req.SessionId, req.UploadToken, 5*time.Minute,
	)

	var errs []string
	var artifacts int32

	// Collect stacks.txt (stop-the-world) only if requested.
	if req.IncludeGoroutineStacks {
		stacks, err := stacksLocal(&serverpb.StacksRequest{
			Type: serverpb.StacksType_GOROUTINE_STACKS,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("stacks: %v", err))
		} else {
			name := fmt.Sprintf("nodes/%d/stacks.txt", nodeID)
			if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "stack", stacks.Data); uploadErr != nil {
				errs = append(errs, fmt.Sprintf("upload stacks: %v", uploadErr))
			} else {
				artifacts++
			}
		}
	}

	// Collect stacks with labels (debug=1, no stop-the-world).
	stacksLabels, err := stacksLocal(&serverpb.StacksRequest{
		Type: serverpb.StacksType_GOROUTINE_STACKS_DEBUG_1,
	})
	if err != nil {
		errs = append(errs, fmt.Sprintf("stacks_with_labels: %v", err))
	} else {
		name := fmt.Sprintf("nodes/%d/stacks_with_labels.txt", nodeID)
		if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "stack", stacksLabels.Data); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload stacks_with_labels: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// Collect goroutine profile (debug=3 pprof format, no stop-the-world).
	stacksPprof, err := stacksLocal(&serverpb.StacksRequest{
		Type: serverpb.StacksType_GOROUTINE_STACKS_DEBUG_3,
	})
	if err != nil {
		errs = append(errs, fmt.Sprintf("stacks.pprof: %v", err))
	} else {
		name := fmt.Sprintf("nodes/%d/stacks.pprof", nodeID)
		if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "profile", stacksPprof.Data); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload stacks.pprof: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// Collect heap profile.
	heap, err := s.Profile(ctx, &serverpb.ProfileRequest{
		NodeId: "local",
		Type:   serverpb.ProfileRequest_HEAP,
	})
	if err != nil {
		errs = append(errs, fmt.Sprintf("heap profile: %v", err))
	} else {
		name := fmt.Sprintf("nodes/%d/heap.pprof", nodeID)
		if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "profile", heap.Data); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload heap: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// Collect CPU profile if requested.
	if req.CpuProfSeconds > 0 {
		cpuProf, err := s.Profile(ctx, &serverpb.ProfileRequest{
			NodeId:  "local",
			Type:    serverpb.ProfileRequest_CPU,
			Seconds: req.CpuProfSeconds,
			Labels:  true,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("cpu profile: %v", err))
		} else {
			name := fmt.Sprintf("nodes/%d/cpu.pprof", nodeID)
			if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "profile", cpuProf.Data); uploadErr != nil {
				errs = append(errs, fmt.Sprintf("upload cpu: %v", uploadErr))
			} else {
				artifacts++
			}
		}
	}

	// Collect engine stats (LSM).
	engineStats, err := s.EngineStats(ctx, &serverpb.EngineStatsRequest{NodeId: "local"})
	if err != nil {
		errs = append(errs, fmt.Sprintf("engine stats: %v", err))
	} else {
		name := fmt.Sprintf("nodes/%d/lsm.txt", nodeID)
		lsmText := formatLSMStatsForUpload(engineStats)
		if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "engine-stats", []byte(lsmText)); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload lsm: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// Collect node details.
	details, err := s.Details(ctx, &serverpb.DetailsRequest{
		NodeId: "local",
		Redact: req.Redact,
	})
	if err != nil {
		errs = append(errs, fmt.Sprintf("details: %v", err))
	} else {
		name := fmt.Sprintf("nodes/%d/details.json", nodeID)
		if uploadErr := client.uploadArtifactJSON(ctx, name, int32(nodeID), "metadata", details); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload details: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// Collect gossip info.
	gossipInfo, err := s.Gossip(ctx, &serverpb.GossipRequest{NodeId: "local"})
	if err != nil {
		errs = append(errs, fmt.Sprintf("gossip: %v", err))
	} else {
		name := fmt.Sprintf("nodes/%d/gossip.json", nodeID)
		if uploadErr := client.uploadArtifactJSON(ctx, name, int32(nodeID), "metadata", gossipInfo); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload gossip: %v", uploadErr))
		} else {
			artifacts++
		}
	}

	// Collect node status (from the Nodes response, find local node).
	nodesResp, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		errs = append(errs, fmt.Sprintf("status: %v", err))
	} else {
		var localStatus *statuspb.NodeStatus
		for i := range nodesResp.Nodes {
			if nodesResp.Nodes[i].Desc.NodeID == nodeID {
				localStatus = &nodesResp.Nodes[i]
				break
			}
		}
		if localStatus != nil {
			name := fmt.Sprintf("nodes/%d/status.json", nodeID)
			if uploadErr := client.uploadArtifactJSON(ctx, name, int32(nodeID), "metadata", localStatus); uploadErr != nil {
				errs = append(errs, fmt.Sprintf("upload status: %v", uploadErr))
			} else {
				artifacts++
			}
		}
	}

	// Collect range info if requested.
	if req.IncludeRangeInfo {
		rangesResp, err := s.Ranges(ctx, &serverpb.RangesRequest{
			NodeId: "local",
			Redact: req.Redact,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("ranges: %v", err))
		} else {
			// Sort ranges by RangeID to match debug zip behavior.
			sort.Slice(rangesResp.Ranges, func(i, j int) bool {
				return rangesResp.Ranges[i].State.Desc.RangeID < rangesResp.Ranges[j].State.Desc.RangeID
			})
			name := fmt.Sprintf("nodes/%d/ranges.json", nodeID)
			if uploadErr := client.uploadArtifactJSON(ctx, name, int32(nodeID), "metadata", rangesResp); uploadErr != nil {
				errs = append(errs, fmt.Sprintf("upload ranges: %v", uploadErr))
			} else {
				artifacts++
			}
		}
	}

	// Collect log files.
	logArtifacts, logErrs := s.uploadLogFiles(ctx, client, nodeID, req.Redact)
	artifacts += logArtifacts
	errs = append(errs, logErrs...)

	// Collect historical profiles.
	profArtifacts, profErrs := s.uploadHistoricalProfiles(ctx, client, nodeID)
	artifacts += profArtifacts
	errs = append(errs, profErrs...)

	// Collect internal SQL tables.
	tableArtifacts, tableErrs := s.uploadInternalTables(ctx, client, nodeID, req.Redact)
	artifacts += tableArtifacts
	errs = append(errs, tableErrs...)

	return &serverpb.UploadNodeDebugDataResponse{
		ArtifactsUploaded: artifacts,
		Errors:            errs,
	}, nil
}

// uploadLogFiles retrieves and uploads all log files from the local node.
func (s *systemStatusServer) uploadLogFiles(
	ctx context.Context, client *uploadServerRPCClient, nodeID roachpb.NodeID, redact bool,
) (artifacts int32, errs []string) {
	logFiles, err := s.LogFilesList(ctx, &serverpb.LogFilesListRequest{NodeId: "local"})
	if err != nil {
		return 0, []string{fmt.Sprintf("log files list: %v", err)}
	}

	for _, file := range logFiles.Files {
		entries, err := s.LogFile(ctx, &serverpb.LogFileRequest{
			NodeId: "local",
			File:   file.Name,
			Redact: redact,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("log file %s: %v", file.Name, err))
			continue
		}

		var buf bytes.Buffer
		for _, e := range entries.Entries {
			if redact && !e.Redactable {
				e.Message = "REDACTEDBYZIP"
			}
			if fmtErr := log.FormatLegacyEntry(e, &buf); fmtErr != nil {
				errs = append(errs, fmt.Sprintf("format log entry %s: %v", file.Name, fmtErr))
				break
			}
		}

		name := fmt.Sprintf("nodes/%d/logs/%s", nodeID, file.Name)
		if uploadErr := client.uploadArtifactBytes(ctx, name, int32(nodeID), "log", buf.Bytes()); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload log %s: %v", file.Name, uploadErr))
		} else {
			artifacts++
		}
	}
	return artifacts, errs
}

// uploadHistoricalProfiles retrieves and uploads historical profiles
// (heap, goroutine dumps, CPU profiles, execution traces) from the
// local node.
func (s *systemStatusServer) uploadHistoricalProfiles(
	ctx context.Context, client *uploadServerRPCClient, nodeID roachpb.NodeID,
) (artifacts int32, errs []string) {
	profileTypes := []struct {
		fileType serverpb.FileType
		subdir   string
	}{
		{serverpb.FileType_HEAP, "heapprof"},
		{serverpb.FileType_GOROUTINES, "goroutines"},
		{serverpb.FileType_CPU, "cpuprof"},
		{serverpb.FileType_EXECUTIONTRACE, "executiontraces"},
	}

	for _, pt := range profileTypes {
		// List files of this type.
		fileList, err := s.GetFiles(ctx, &serverpb.GetFilesRequest{
			NodeId:   "local",
			Type:     pt.fileType,
			ListOnly: true,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s list: %v", pt.subdir, err))
			continue
		}

		// Retrieve and upload each file.
		for _, f := range fileList.Files {
			fileData, err := s.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   "local",
				Type:     pt.fileType,
				Patterns: []string{f.Name},
				ListOnly: false,
			})
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s/%s: %v", pt.subdir, f.Name, err))
				continue
			}
			if len(fileData.Files) == 0 {
				continue
			}

			name := fmt.Sprintf("nodes/%d/%s/%s", nodeID, pt.subdir, f.Name)
			if uploadErr := client.uploadArtifactBytes(
				ctx, name, int32(nodeID), "profile", fileData.Files[0].Contents,
			); uploadErr != nil {
				errs = append(errs, fmt.Sprintf("upload %s/%s: %v", pt.subdir, f.Name, uploadErr))
			} else {
				artifacts++
			}
		}
	}
	return artifacts, errs
}

// uploadInternalTables queries per-node crdb_internal tables and
// uploads the results as tab-separated text.
func (s *systemStatusServer) uploadInternalTables(
	ctx context.Context, client *uploadServerRPCClient, nodeID roachpb.NodeID, doRedact bool,
) (artifacts int32, errs []string) {
	for _, table := range uploadPerNodeTables {
		query := table.query
		if doRedact && table.queryRedacted != "" {
			query = table.queryRedacted
		}

		it, err := s.internalExecutor.QueryIteratorEx(
			ctx,
			redact.RedactableString("upload-"+table.name),
			nil, // txn
			sessiondata.NodeUserSessionDataOverride,
			query,
		)
		if err != nil {
			errs = append(errs, fmt.Sprintf("table %s: %v", table.name, err))
			continue
		}

		var buf bytes.Buffer

		// Write column header from iterator metadata.
		cols := it.Types()
		for i, col := range cols {
			if i > 0 {
				buf.WriteByte('\t')
			}
			buf.WriteString(col.Name)
		}
		buf.WriteByte('\n')

		// Write rows.
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
		if iterErr != nil {
			errs = append(errs, fmt.Sprintf("table %s iteration: %v", table.name, iterErr))
			// Still upload partial data if we have any.
		}

		sanitizedName := strings.ReplaceAll(table.name, ".", "_")
		artifactName := fmt.Sprintf("nodes/%d/%s.txt", nodeID, sanitizedName)
		if uploadErr := client.uploadArtifactBytes(
			ctx, artifactName, int32(nodeID), "table", buf.Bytes(),
		); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload table %s: %v", table.name, uploadErr))
		} else {
			artifacts++
		}
	}
	return artifacts, errs
}

// uploadTableDef defines a per-node internal table to query and upload.
// query is used for unredacted mode (or when queryRedacted is empty).
// queryRedacted is used when redaction is requested.
type uploadTableDef struct {
	name          string
	query         string
	queryRedacted string
}

// uploadPerNodeTables mirrors the per-node table list from
// pkg/cli/zip_table_registry.go (zipInternalTablesPerNode). When
// redaction is requested, queryRedacted is used; otherwise query is
// used. Both forms are pre-computed to avoid importing the cli
// package.
var uploadPerNodeTables = []uploadTableDef{
	{
		name:          "crdb_internal.active_range_feeds",
		query:         "SELECT * FROM crdb_internal.active_range_feeds",
		queryRedacted: "SELECT id, tags, start_after, diff, node_id, range_id, created, range_start, range_end, resolved, resolved_age, last_event, catchup FROM crdb_internal.active_range_feeds",
	},
	{
		name:          "crdb_internal.feature_usage",
		query:         "SELECT * FROM crdb_internal.feature_usage",
		queryRedacted: "SELECT feature_name, usage_count FROM crdb_internal.feature_usage",
	},
	{
		name:          "crdb_internal.gossip_alerts",
		query:         "SELECT * FROM crdb_internal.gossip_alerts",
		queryRedacted: "SELECT node_id, store_id, category, description, value FROM crdb_internal.gossip_alerts",
	},
	{
		name:          "crdb_internal.gossip_liveness",
		query:         "SELECT * FROM crdb_internal.gossip_liveness",
		queryRedacted: "SELECT node_id, epoch, expiration, draining, decommissioning, membership, updated_at FROM crdb_internal.gossip_liveness",
	},
	{
		name:  "crdb_internal.gossip_nodes",
		query: "SELECT * FROM crdb_internal.gossip_nodes",
		queryRedacted: `SELECT node_id, network, '<redacted>' as address, '<redacted>' as advertise_address, ` +
			`sql_network, '<redacted>' as sql_address, '<redacted>' as advertise_sql_address, attrs, ` +
			`'<redacted>' as locality, fnv32(cluster_name) as cluster_name, server_version, build_tag, ` +
			`started_at, is_live, ranges, leases FROM crdb_internal.gossip_nodes`,
	},
	{
		name:          "crdb_internal.leases",
		query:         "SELECT * FROM crdb_internal.leases",
		queryRedacted: "SELECT node_id, table_id, name, parent_id, expiration, deleted FROM crdb_internal.leases",
	},
	{
		name:          "crdb_internal.kv_session_based_leases",
		query:         "SELECT * FROM crdb_internal.kv_session_based_leases",
		queryRedacted: "SELECT desc_id, version, sql_instance_id, session_id, crdb_region FROM crdb_internal.kv_session_based_leases",
	},
	{
		name:          "crdb_internal.node_build_info",
		query:         "SELECT * FROM crdb_internal.node_build_info",
		queryRedacted: "SELECT node_id, field, value FROM crdb_internal.node_build_info",
	},
	{
		name:          "crdb_internal.node_contention_events",
		query:         "SELECT * FROM crdb_internal.node_contention_events",
		queryRedacted: "SELECT table_id, index_id, num_contention_events, cumulative_contention_time, txn_id, count FROM crdb_internal.node_contention_events",
	},
	{
		name:          "crdb_internal.node_distsql_flows",
		query:         "SELECT * FROM crdb_internal.node_distsql_flows",
		queryRedacted: "SELECT flow_id, node_id, since, crdb_internal.hide_sql_constants(stmt) as stmt FROM crdb_internal.node_distsql_flows",
	},
	{
		name:  "crdb_internal.node_execution_insights",
		query: "SELECT * FROM crdb_internal.node_execution_insights",
		queryRedacted: "SELECT session_id, txn_id, txn_fingerprint_id, stmt_id, stmt_fingerprint_id, " +
			"query, status, start_time, end_time, full_scan, user_name, app_name, database_name, " +
			"plan_gist, rows_read, rows_written, priority, retries, exec_node_ids, kv_node_ids, " +
			"error_code, crdb_internal.redact(last_error_redactable) as last_error_redactable " +
			"FROM crdb_internal.node_execution_insights",
	},
	{
		name: "crdb_internal.node_inflight_trace_spans",
		query: `WITH spans AS (
			SELECT * FROM crdb_internal.node_inflight_trace_spans
			WHERE duration > INTERVAL '10' ORDER BY trace_id ASC, duration DESC
		) SELECT * FROM spans, LATERAL crdb_internal.payloads_for_span(span_id)`,
		queryRedacted: `WITH spans AS (
			SELECT * FROM crdb_internal.node_inflight_trace_spans
			WHERE duration > INTERVAL '10' ORDER BY trace_id ASC, duration DESC
		) SELECT trace_id, parent_span_id, span_id, goroutine_id, finished, start_time, duration, operation, payload_type
		FROM spans, LATERAL crdb_internal.payloads_for_span(span_id)`,
	},
	{
		name:          "crdb_internal.node_memory_monitors",
		query:         "SELECT * FROM crdb_internal.node_memory_monitors",
		queryRedacted: "SELECT level, name, id, parent_id, used, reserved_used, reserved_reserved, stopped FROM crdb_internal.node_memory_monitors",
	},
	{
		name:          "crdb_internal.node_metrics",
		query:         "SELECT * FROM crdb_internal.node_metrics",
		queryRedacted: "SELECT store_id, name, value FROM crdb_internal.node_metrics",
	},
	{
		name:  "crdb_internal.node_queries",
		query: "SELECT * FROM crdb_internal.node_queries",
		queryRedacted: "SELECT query_id, txn_id, node_id, session_id, user_name, start, " +
			"application_name, distributed, phase, full_scan, " +
			"crdb_internal.hide_sql_constants(query) as query, num_txn_retries, num_txn_auto_retries " +
			"FROM crdb_internal.node_queries",
	},
	{
		name:  "crdb_internal.node_runtime_info",
		query: "SELECT * FROM crdb_internal.node_runtime_info",
		queryRedacted: `SELECT * FROM (
			SELECT "node_id", "component", "field", "value"
			FROM crdb_internal.node_runtime_info
			WHERE field NOT IN ('URL', 'Host', 'URI') UNION
			SELECT "node_id", "component", "field", '<redacted>' AS value
			FROM crdb_internal.node_runtime_info
			WHERE field IN ('URL', 'Host', 'URI')
		) ORDER BY node_id`,
	},
	{
		name:  "crdb_internal.node_sessions",
		query: "SELECT * FROM crdb_internal.node_sessions",
		queryRedacted: "SELECT node_id, session_id, user_name, application_name, num_txns_executed, " +
			"session_start, active_query_start, kv_txn, alloc_bytes, max_alloc_bytes, status, " +
			"session_end, crdb_internal.hide_sql_constants(active_queries) as active_queries, " +
			"crdb_internal.hide_sql_constants(last_active_query) as last_active_query, trace_id, goroutine_id " +
			"FROM crdb_internal.node_sessions",
	},
	{
		name:  "crdb_internal.node_statement_statistics",
		query: "SELECT * FROM crdb_internal.node_statement_statistics",
		queryRedacted: "SELECT node_id, application_name, flags, statement_id, key, anonymized, " +
			"count, first_attempt_count, max_retries, rows_avg, rows_var, " +
			"parse_lat_avg, parse_lat_var, run_lat_avg, run_lat_var, " +
			"service_lat_avg, service_lat_var, overhead_lat_avg, overhead_lat_var, " +
			"bytes_read_avg, bytes_read_var, rows_read_avg, rows_read_var, " +
			"network_bytes_avg, network_bytes_var, network_msgs_avg, network_msgs_var, " +
			"max_mem_usage_avg, max_mem_usage_var, max_disk_usage_avg, max_disk_usage_var, " +
			"contention_time_avg, contention_time_var, cpu_sql_nanos_avg, cpu_sql_nanos_var, " +
			"mvcc_step_avg, mvcc_step_var, mvcc_step_internal_avg, mvcc_step_internal_var, " +
			"mvcc_seek_avg, mvcc_seek_var, mvcc_seek_internal_avg, mvcc_seek_internal_var, " +
			"mvcc_block_bytes_avg, mvcc_block_bytes_var, " +
			"mvcc_block_bytes_in_cache_avg, mvcc_block_bytes_in_cache_var, " +
			"mvcc_key_bytes_avg, mvcc_key_bytes_var, mvcc_value_bytes_avg, mvcc_value_bytes_var, " +
			"mvcc_point_count_avg, mvcc_point_count_var, " +
			"mvcc_points_covered_by_range_tombstones_avg, mvcc_points_covered_by_range_tombstones_var, " +
			"mvcc_range_key_count_avg, mvcc_range_key_count_var, " +
			"mvcc_range_key_contained_points_avg, mvcc_range_key_contained_points_var, " +
			"mvcc_range_key_skipped_points_avg, mvcc_range_key_skipped_points_var, " +
			"implicit_txn, full_scan, sample_plan, database_name, exec_node_ids, kv_node_ids, " +
			"used_follower_read, txn_fingerprint_id, index_recommendations, " +
			"latency_seconds_min, latency_seconds_max " +
			"FROM crdb_internal.node_statement_statistics",
	},
	{
		name:  "crdb_internal.node_transaction_statistics",
		query: "SELECT * FROM crdb_internal.node_transaction_statistics",
		queryRedacted: "SELECT node_id, application_name, key, statement_ids, count, max_retries, " +
			"service_lat_avg, service_lat_var, retry_lat_avg, retry_lat_var, " +
			"commit_lat_avg, commit_lat_var, rows_read_avg, rows_read_var, " +
			"network_bytes_avg, network_bytes_var, network_msgs_avg, network_msgs_var, " +
			"max_mem_usage_avg, max_mem_usage_var, max_disk_usage_avg, max_disk_usage_var, " +
			"contention_time_avg, contention_time_var, cpu_sql_nanos_avg, cpu_sql_nanos_var, " +
			"mvcc_step_avg, mvcc_step_var, mvcc_step_internal_avg, mvcc_step_internal_var, " +
			"mvcc_seek_avg, mvcc_seek_var, mvcc_seek_internal_avg, mvcc_seek_internal_var, " +
			"mvcc_block_bytes_avg, mvcc_block_bytes_var, " +
			"mvcc_block_bytes_in_cache_avg, mvcc_block_bytes_in_cache_var, " +
			"mvcc_key_bytes_avg, mvcc_key_bytes_var, mvcc_value_bytes_avg, mvcc_value_bytes_var, " +
			"mvcc_point_count_avg, mvcc_point_count_var, " +
			"mvcc_points_covered_by_range_tombstones_avg, mvcc_points_covered_by_range_tombstones_var, " +
			"mvcc_range_key_count_avg, mvcc_range_key_count_var, " +
			"mvcc_range_key_contained_points_avg, mvcc_range_key_contained_points_var, " +
			"mvcc_range_key_skipped_points_avg, mvcc_range_key_skipped_points_var " +
			"FROM crdb_internal.node_transaction_statistics",
	},
	{
		name:  "crdb_internal.node_transactions",
		query: "SELECT * FROM crdb_internal.node_transactions",
		queryRedacted: "SELECT id, node_id, session_id, start, application_name, " +
			"num_stmts, num_retries, num_auto_retries FROM crdb_internal.node_transactions",
	},
	{
		name:  "crdb_internal.node_txn_execution_insights",
		query: "SELECT * FROM crdb_internal.node_txn_execution_insights",
		queryRedacted: "SELECT txn_id, txn_fingerprint_id, query, implicit_txn, session_id, " +
			"start_time, end_time, user_name, app_name, rows_read, rows_written, priority, " +
			"retries, contention, problems, causes, stmt_execution_ids, last_error_code, " +
			"crdb_internal.redact(last_error_redactable) as last_error_redactable " +
			"FROM crdb_internal.node_txn_execution_insights",
	},
	{
		name:          "crdb_internal.node_txn_stats",
		query:         "SELECT * FROM crdb_internal.node_txn_stats",
		queryRedacted: "SELECT node_id, application_name, txn_count, txn_time_avg_sec, txn_time_var_sec, committed_count, implicit_count FROM crdb_internal.node_txn_stats",
	},
	{
		name:          "crdb_internal.node_tenant_capabilities_cache",
		query:         "SELECT * FROM crdb_internal.node_tenant_capabilities_cache",
		queryRedacted: "SELECT tenant_id, capability_name, capability_value FROM crdb_internal.node_tenant_capabilities_cache",
	},
	{
		name:          "crdb_internal.cluster_replication_node_streams",
		query:         "SELECT * FROM crdb_internal.cluster_replication_node_streams",
		queryRedacted: "SELECT stream_id, consumer, spans, state, read, emit, last_read_ms, last_emit_ms, seq, chkpts, last_chkpt, batches, megabytes, last_kb, rf_chk, rf_adv, rf_last_adv, resolved_age FROM crdb_internal.cluster_replication_node_streams",
	},
	{
		name:          "crdb_internal.cluster_replication_node_stream_spans",
		query:         "SELECT * FROM crdb_internal.cluster_replication_node_stream_spans",
		queryRedacted: "SELECT stream_id, consumer FROM crdb_internal.cluster_replication_node_stream_spans",
	},
	{
		name:          "crdb_internal.cluster_replication_node_stream_checkpoints",
		query:         "SELECT * FROM crdb_internal.cluster_replication_node_stream_checkpoints",
		queryRedacted: "SELECT stream_id, consumer, resolved, resolved_age FROM crdb_internal.cluster_replication_node_stream_checkpoints",
	},
	{
		name:  "crdb_internal.logical_replication_node_processors",
		query: "SELECT * FROM crdb_internal.logical_replication_node_processors",
		queryRedacted: "SELECT stream_id, consumer, state, recv_time, last_recv_time, ingest_time, " +
			"flush_time, flush_count, flush_kvs, flush_bytes, flush_batches, last_flush_time, " +
			"chunks_running, chunks_done, last_kvs_done, last_kvs_todo, last_batches, last_slowest, " +
			"checkpoints, retry_size, resolved_age " +
			"FROM crdb_internal.logical_replication_node_processors",
	},
	{
		name:  "crdb_internal.cluster_replication_node_processors",
		query: "SELECT * FROM crdb_internal.cluster_replication_node_processors",
		queryRedacted: "SELECT stream_id, processor_id, state, recv_wait, last_recv_wait, " +
			"flush_wait, last_flush_wait, events_received, flush_cnt, last_event_age, last_flush_age " +
			"FROM crdb_internal.cluster_replication_node_processors",
	},
}
