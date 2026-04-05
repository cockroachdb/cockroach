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
	"strconv"
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

// drpcStatusServer wraps statusServer for DRPC registration,
// providing DRPC-specific streaming method signatures.
type drpcStatusServer struct {
	*statusServer
}

// UploadDebugData satisfies the DRPCStatusServer interface for the
// tenant-level stub.
func (s *drpcStatusServer) UploadDebugData(
	req *serverpb.UploadDebugDataRequest, stream serverpb.DRPCStatus_UploadDebugDataStream,
) error {
	return s.statusServer.uploadDebugDataImpl(req, stream)
}

// drpcSystemStatusServer wraps systemStatusServer for DRPC
// registration, providing DRPC-specific streaming method signatures.
type drpcSystemStatusServer struct {
	*systemStatusServer
}

// UploadDebugData satisfies the DRPCStatusServer interface for the
// system-level implementation.
func (s *drpcSystemStatusServer) UploadDebugData(
	req *serverpb.UploadDebugDataRequest, stream serverpb.DRPCStatus_UploadDebugDataStream,
) error {
	return s.systemStatusServer.uploadDebugDataImpl(req, stream)
}

// UploadDebugData is the tenant-level gRPC stub. Secondary tenants
// cannot use this RPC.
func (t *statusServer) UploadDebugData(
	req *serverpb.UploadDebugDataRequest, stream serverpb.Status_UploadDebugDataServer,
) error {
	return t.uploadDebugDataImpl(req, stream)
}

// uploadDebugDataImpl is the common implementation that works with
// both gRPC and DRPC stream types.
func (t *statusServer) uploadDebugDataImpl(
	req *serverpb.UploadDebugDataRequest, stream serverpb.RPCStatus_UploadDebugDataStream,
) error {
	ctx := t.AnnotateCtx(stream.Context())
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return err
	}
	return status.Error(codes.Unimplemented, "upload debug data is not supported on secondary tenants")
}

// StartDebugUpload is the tenant-level stub.
func (t *statusServer) StartDebugUpload(
	ctx context.Context, req *serverpb.UploadDebugDataRequest,
) (*serverpb.StartDebugUploadResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "start debug upload is not supported on secondary tenants")
}

// GetDebugUploadStatus is the tenant-level stub.
func (t *statusServer) GetDebugUploadStatus(
	ctx context.Context, req *serverpb.GetDebugUploadStatusRequest,
) (*serverpb.GetDebugUploadStatusResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "get debug upload status is not supported on secondary tenants")
}

// ResumeDebugUpload is the tenant-level stub.
func (t *statusServer) ResumeDebugUpload(
	ctx context.Context, req *serverpb.ResumeDebugUploadRequest,
) (*serverpb.StartDebugUploadResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "resume debug upload is not supported on secondary tenants")
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

// uploadFanoutResult holds the aggregated result of the upload
// fan-out across all nodes plus cluster-level data.
type uploadFanoutResult struct {
	sessionID      string
	artifacts      int32
	succeeded      int32
	failed         int32
	nodeStatuses   []serverpb.NodeUploadStatus
	nodesCompleted []int32
}

// runUploadFanout runs the core upload logic: cluster-level data
// collection and per-node fan-out. skipNodes contains node IDs that
// have already completed (used during resume). When skipClusterData
// is true, cluster-level data is not re-collected.
func (s *systemStatusServer) runUploadFanout(
	ctx context.Context,
	client *uploadServerRPCClient,
	req *serverpb.UploadDebugDataRequest,
	skipNodes map[int32]bool,
	skipClusterData bool,
) (*uploadFanoutResult, error) {
	result := &uploadFanoutResult{sessionID: client.sessionID}

	// Collect cluster-level data (coordinator only) unless skipped.
	var clusterTotalArtifacts int32
	var clusterAllErrs []string
	if !skipClusterData {
		clusterArtifacts, clusterDataErrs := s.uploadClusterData(
			ctx, client, req,
		)
		clusterTableArtifacts, clusterTableErrs := s.uploadClusterTables(
			ctx, client, req.Redact,
		)
		clusterAllErrs = append(clusterDataErrs, clusterTableErrs...)
		clusterTotalArtifacts = clusterArtifacts + clusterTableArtifacts
	}

	// Prepare the per-node request. Pass the coordinator's GCS
	// credentials so all nodes upload to the same session folder.
	nodeReq := &serverpb.UploadNodeDebugDataRequest{
		ServerUrl:              req.ServerUrl,
		SessionId:              client.sessionID,
		UploadToken:            client.uploadToken,
		Redact:                 req.Redact,
		CpuProfSeconds:         req.CpuProfSeconds,
		IncludeRangeInfo:       req.IncludeRangeInfo,
		IncludeGoroutineStacks: req.IncludeGoroutineStacks,
		GcsAccessToken:         client.gcsAccessToken,
		GcsBucket:              client.gcsBucket,
		GcsPrefix:              client.gcsPrefix,
	}

	// Build the set of requested node IDs (empty = all).
	requestedNodes := map[int32]bool{}
	for _, nid := range req.NodeIds {
		requestedNodes[nid] = true
	}

	// Fan out to all nodes.
	var mu syncutil.Mutex

	nodeFn := func(
		ctx context.Context,
		statusClient serverpb.RPCStatusClient,
		nodeID roachpb.NodeID,
	) (*serverpb.UploadNodeDebugDataResponse, error) {
		nid := int32(nodeID)
		// Skip nodes not in the requested set (if specified).
		if len(requestedNodes) > 0 && !requestedNodes[nid] {
			return &serverpb.UploadNodeDebugDataResponse{}, nil
		}
		// Skip nodes already completed (resume case).
		if skipNodes[nid] {
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
		result.nodeStatuses = append(result.nodeStatuses, nStatus)
		result.artifacts += resp.ArtifactsUploaded
		if len(resp.Errors) > 0 {
			result.failed++
		} else {
			result.succeeded++
			result.nodesCompleted = append(result.nodesCompleted, int32(nodeID))
		}
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		mu.Lock()
		defer mu.Unlock()
		result.failed++
		result.nodeStatuses = append(result.nodeStatuses, serverpb.NodeUploadStatus{
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
		return nil, err
	}

	// Add cluster-level artifacts and status.
	result.artifacts += clusterTotalArtifacts
	if clusterTotalArtifacts > 0 || len(clusterAllErrs) > 0 {
		result.nodeStatuses = append(result.nodeStatuses, serverpb.NodeUploadStatus{
			NodeId:            0,
			ArtifactsUploaded: clusterTotalArtifacts,
			Errors:            clusterAllErrs,
		})
	}

	// Complete the session.
	if err := client.completeSession(ctx, int(result.artifacts), result.nodesCompleted); err != nil {
		log.Dev.Warningf(ctx, "failed to complete upload session: %v", err)
	}

	return result, nil
}

// setupUploadClient creates and initializes an upload server client.
// For resume (req.SessionId != ""), it reopens the existing session
// via the reupload endpoint and returns the set of already-completed
// nodes. For new sessions, it creates a fresh session.
func (s *systemStatusServer) setupUploadClient(
	ctx context.Context, req *serverpb.UploadDebugDataRequest,
) (client *uploadServerRPCClient, skipNodes map[int32]bool, skipClusterData bool, err error) {
	nodeStatuses, err := s.serverIterator.getAllNodes(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	nodeCount := len(nodeStatuses)
	clusterID := s.rpcCtx.StorageClusterID.Get().String()

	client = newUploadServerClientForRPC(req.ServerUrl, req.ApiKey, 5*time.Minute)
	skipNodes = map[int32]bool{}

	if req.SessionId != "" {
		// Resume an existing session.
		client.sessionID = req.SessionId
		if req.UploadToken != "" {
			client.uploadToken = req.UploadToken
		}

		// Reopen the session to get fresh credentials.
		if reuploadErr := client.reuploadSession(ctx, nil, "coordinator failover"); reuploadErr != nil {
			return nil, nil, false, errors.Wrap(reuploadErr, "reopening session")
		}
		log.Ops.Infof(ctx, "upload session reopened for resume: %s", client.sessionID)

		// Query which nodes are already done.
		sessionStatus, statusErr := client.getSessionStatus(ctx)
		if statusErr != nil {
			return nil, nil, false, errors.Wrap(statusErr, "querying session status")
		}
		for nodeIDStr, count := range sessionStatus.ArtifactsByNode {
			if count > 0 {
				if nodeIDStr == "0" {
					skipClusterData = true
				} else {
					nid, parseErr := strconv.Atoi(nodeIDStr)
					if parseErr == nil {
						skipNodes[int32(nid)] = true
					}
				}
			}
		}
		log.Ops.Infof(ctx, "resume: skipping %d completed nodes, skipClusterData=%t",
			len(skipNodes), skipClusterData)
	} else {
		// Create a new session.
		if createErr := client.createSession(ctx, clusterID, nodeCount, req.Redact, req.Labels); createErr != nil {
			return nil, nil, false, createErr
		}
		log.Ops.Infof(ctx, "upload session created: %s", client.sessionID)
	}

	// Initialize the GCS client for chunked resumable uploads.
	if gcsErr := client.initGCSClient(ctx); gcsErr != nil {
		return nil, nil, false, errors.Wrap(gcsErr, "initializing GCS client")
	}

	return client, skipNodes, skipClusterData, nil
}

// UploadDebugData on the system status server coordinates the debug
// data upload across all nodes. Delegates to the common
// implementation for both gRPC and DRPC.
func (s *systemStatusServer) UploadDebugData(
	req *serverpb.UploadDebugDataRequest, stream serverpb.Status_UploadDebugDataServer,
) error {
	return s.uploadDebugDataImpl(req, stream)
}

// uploadDebugDataImpl streams events back to the caller: first a
// SessionCreated event with the session_id, then a Completed event
// with the final result. Supports resuming an existing session when
// req.SessionId is set.
func (s *systemStatusServer) uploadDebugDataImpl(
	req *serverpb.UploadDebugDataRequest, stream serverpb.RPCStatus_UploadDebugDataStream,
) error {
	ctx := stream.Context()
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return err
	}

	if req.ServerUrl == "" {
		return status.Error(codes.InvalidArgument, "server_url is required")
	}
	if req.ApiKey == "" {
		return status.Error(codes.InvalidArgument, "api_key is required")
	}

	client, skipNodes, skipClusterData, err := s.setupUploadClient(ctx, req)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	defer func() { _ = client.closeGCS() }()

	// Send the session_id to the client immediately so it can
	// retry on a different node if this coordinator fails.
	if err := stream.Send(&serverpb.UploadDebugDataEvent{
		Event: &serverpb.UploadDebugDataEvent_SessionCreated{
			SessionCreated: &serverpb.SessionCreated{
				SessionId:   client.sessionID,
				UploadToken: client.uploadToken,
			},
		},
	}); err != nil {
		return err
	}

	result, err := s.runUploadFanout(ctx, client, req, skipNodes, skipClusterData)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}

	// Send the final result.
	return stream.Send(&serverpb.UploadDebugDataEvent{
		Event: &serverpb.UploadDebugDataEvent_Completed{
			Completed: &serverpb.UploadDebugDataResponse{
				SessionId:         result.sessionID,
				ArtifactsUploaded: result.artifacts,
				NodesSucceeded:    result.succeeded,
				NodesFailed:       result.failed,
				NodeStatuses:      result.nodeStatuses,
			},
		},
	})
}

// StartDebugUpload creates a session and starts the fan-out in a
// background goroutine, returning immediately with the session_id.
// DB Console uses this to trigger uploads without a long-lived
// connection.
func (s *systemStatusServer) StartDebugUpload(
	ctx context.Context, req *serverpb.UploadDebugDataRequest,
) (*serverpb.StartDebugUploadResponse, error) {
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

	client, skipNodes, skipClusterData, err := s.setupUploadClient(ctx, req)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	sessionID := client.sessionID
	uploadToken := client.uploadToken

	// Run the fan-out in a background goroutine managed by the
	// stopper so it is cleaned up on node shutdown.
	if err := s.stopper.RunAsyncTask(
		s.AnnotateCtx(context.Background()),
		"upload-debug-data-async",
		func(ctx context.Context) {
			defer func() { _ = client.closeGCS() }()
			ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
			defer cancel()

			result, fanoutErr := s.runUploadFanout(ctx, client, req, skipNodes, skipClusterData)
			if fanoutErr != nil {
				log.Ops.Errorf(ctx, "async upload failed for session %s: %v", sessionID, fanoutErr)
				return
			}
			log.Ops.Infof(ctx, "async upload completed for session %s: %d artifacts, %d succeeded, %d failed",
				sessionID, result.artifacts, result.succeeded, result.failed)
		},
	); err != nil {
		_ = client.closeGCS()
		return nil, srverrors.ServerError(ctx, err)
	}

	return &serverpb.StartDebugUploadResponse{
		SessionId:   sessionID,
		UploadToken: uploadToken,
	}, nil
}

// GetDebugUploadStatus proxies a status request to the upload server
// and returns per-node completion information. Any node can serve
// this request.
func (s *systemStatusServer) GetDebugUploadStatus(
	ctx context.Context, req *serverpb.GetDebugUploadStatusRequest,
) (*serverpb.GetDebugUploadStatusResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.ServerUrl == "" {
		return nil, status.Error(codes.InvalidArgument, "server_url is required")
	}
	if req.UploadToken == "" {
		return nil, status.Error(codes.InvalidArgument, "upload_token is required")
	}

	client := newUploadServerClientForNodeRPC(
		req.ServerUrl, req.SessionId, req.UploadToken, 30*time.Second,
	)

	sessionStatus, err := client.getSessionStatus(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	artifactsByNode := make(map[string]int32, len(sessionStatus.ArtifactsByNode))
	for k, v := range sessionStatus.ArtifactsByNode {
		artifactsByNode[k] = int32(v)
	}

	return &serverpb.GetDebugUploadStatusResponse{
		State:             sessionStatus.State,
		ArtifactsReceived: int32(sessionStatus.ArtifactsReceived),
		ArtifactsByNode:   artifactsByNode,
	}, nil
}

// ResumeDebugUpload resumes a previously failed or incomplete upload
// session. It calls the reupload endpoint, determines completed
// nodes, and starts a background goroutine to fan out to the
// remaining nodes.
func (s *systemStatusServer) ResumeDebugUpload(
	ctx context.Context, req *serverpb.ResumeDebugUploadRequest,
) (*serverpb.StartDebugUploadResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.ServerUrl == "" {
		return nil, status.Error(codes.InvalidArgument, "server_url is required")
	}
	if req.ApiKey == "" {
		return nil, status.Error(codes.InvalidArgument, "api_key is required")
	}

	// Build an UploadDebugDataRequest so setupUploadClient and
	// runUploadFanout can reuse the same code paths.
	uploadReq := &serverpb.UploadDebugDataRequest{
		ServerUrl:              req.ServerUrl,
		ApiKey:                 req.ApiKey,
		Redact:                 req.Redact,
		CpuProfSeconds:         req.CpuProfSeconds,
		IncludeRangeInfo:       req.IncludeRangeInfo,
		IncludeGoroutineStacks: req.IncludeGoroutineStacks,
		SessionId:              req.SessionId,
	}

	client, skipNodes, skipClusterData, err := s.setupUploadClient(ctx, uploadReq)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	sessionID := client.sessionID
	uploadToken := client.uploadToken

	if err := s.stopper.RunAsyncTask(
		s.AnnotateCtx(context.Background()),
		"resume-debug-upload-async",
		func(ctx context.Context) {
			defer func() { _ = client.closeGCS() }()
			ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
			defer cancel()

			result, fanoutErr := s.runUploadFanout(ctx, client, uploadReq, skipNodes, skipClusterData)
			if fanoutErr != nil {
				log.Ops.Errorf(ctx, "async resume upload failed for session %s: %v", sessionID, fanoutErr)
				return
			}
			log.Ops.Infof(ctx, "async resume upload completed for session %s: %d artifacts, %d succeeded, %d failed",
				sessionID, result.artifacts, result.succeeded, result.failed)
		},
	); err != nil {
		_ = client.closeGCS()
		return nil, srverrors.ServerError(ctx, err)
	}

	return &serverpb.StartDebugUploadResponse{
		SessionId:   sessionID,
		UploadToken: uploadToken,
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
	defer func() { _ = client.closeGCS() }()

	// Initialize the GCS client. Use the coordinator's credentials if
	// provided so all nodes upload to the same session folder.
	if req.GcsAccessToken != "" && req.GcsBucket != "" && req.GcsPrefix != "" {
		if err := client.initGCSClientWithCredentials(
			ctx, req.GcsAccessToken, req.GcsBucket, req.GcsPrefix,
		); err != nil {
			return nil, errors.Wrapf(err, "initializing GCS client for node %d", nodeID)
		}
	} else {
		if err := client.initGCSClient(ctx); err != nil {
			return nil, errors.Wrapf(err, "initializing GCS client for node %d", nodeID)
		}
	}

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
			if uploadErr := client.uploadArtifactBytes(ctx, name, stacks.Data); uploadErr != nil {
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
		if uploadErr := client.uploadArtifactBytes(ctx, name, stacksLabels.Data); uploadErr != nil {
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
		if uploadErr := client.uploadArtifactBytes(ctx, name, stacksPprof.Data); uploadErr != nil {
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
		if uploadErr := client.uploadArtifactBytes(ctx, name, heap.Data); uploadErr != nil {
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
			if uploadErr := client.uploadArtifactBytes(ctx, name, cpuProf.Data); uploadErr != nil {
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
		if uploadErr := client.uploadArtifactBytes(ctx, name, []byte(lsmText)); uploadErr != nil {
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
		if uploadErr := client.uploadArtifactJSON(ctx, name, details); uploadErr != nil {
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
		if uploadErr := client.uploadArtifactJSON(ctx, name, gossipInfo); uploadErr != nil {
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
			if uploadErr := client.uploadArtifactJSON(ctx, name, localStatus); uploadErr != nil {
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
			if uploadErr := client.uploadArtifactJSON(ctx, name, rangesResp); uploadErr != nil {
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

		// Stream formatted log entries directly to GCS via io.Pipe,
		// avoiding a buffer allocation for the formatted output. The
		// entries slice stays in memory (from the RPC response), but
		// each call to the factory re-iterates and re-formats into a
		// fresh pipe.
		logEntries := entries.Entries
		doRedact := redact
		name := fmt.Sprintf("nodes/%d/logs/%s", nodeID, file.Name)
		uploadErr := client.uploadArtifactStreaming(
			ctx, name, "text/plain",
			func() (io.ReadCloser, error) {
				pr, pw := io.Pipe()
				go func() {
					defer func() { _ = pw.Close() }()
					for _, e := range logEntries {
						if doRedact && !e.Redactable {
							e.Message = "REDACTEDBYZIP"
						}
						if fmtErr := log.FormatLegacyEntry(e, pw); fmtErr != nil {
							pw.CloseWithError(fmtErr)
							return
						}
					}
				}()
				return pr, nil
			},
		)
		if uploadErr != nil {
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
				ctx, name, fileData.Files[0].Contents,
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
			ctx, artifactName, buf.Bytes(),
		); uploadErr != nil {
			errs = append(errs, fmt.Sprintf("upload table %s: %v", table.name, uploadErr))
		} else {
			artifacts++
		}
	}
	return artifacts, errs
}

// uploadTableDef defines an internal table to query and upload.
// query is used for unredacted mode (or when queryRedacted is empty).
// queryRedacted is used when redaction is requested.
// queryFallback is tried if query fails (unredacted mode only).
type uploadTableDef struct {
	name          string
	query         string
	queryRedacted string
	queryFallback string
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

// uploadClusterData collects and uploads cluster-level status RPC
// artifacts (nodes, problem ranges, tenant ranges).
func (s *systemStatusServer) uploadClusterData(
	ctx context.Context, client *uploadServerRPCClient, req *serverpb.UploadDebugDataRequest,
) (artifacts int32, errs []string) {
	// Upload cluster nodes.
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

	if req.IncludeRangeInfo {
		// Upload problem ranges.
		problemRanges, err := s.ProblemRanges(
			ctx, &serverpb.ProblemRangesRequest{},
		)
		if err != nil {
			errs = append(errs, fmt.Sprintf("problem ranges: %v", err))
		} else {
			if uploadErr := client.uploadArtifactJSON(
				ctx, "cluster/reports/problemranges.json", problemRanges,
			); uploadErr != nil {
				errs = append(errs,
					fmt.Sprintf("upload problem ranges: %v", uploadErr))
			} else {
				artifacts++
			}
		}

		// Upload tenant ranges (one file per locality).
		tenantRanges, err := s.TenantRanges(
			ctx, &serverpb.TenantRangesRequest{},
		)
		if err != nil {
			errs = append(errs, fmt.Sprintf("tenant ranges: %v", err))
		} else {
			localities := make(
				[]string, 0, len(tenantRanges.RangesByLocality),
			)
			for k := range tenantRanges.RangesByLocality {
				localities = append(localities, k)
			}
			sort.Strings(localities)
			for _, locality := range localities {
				rangeList := tenantRanges.RangesByLocality[locality]
				name := fmt.Sprintf(
					"cluster/tenant_ranges/%s.json", locality,
				)
				if uploadErr := client.uploadArtifactJSON(
					ctx, name, rangeList,
				); uploadErr != nil {
					errs = append(errs,
						fmt.Sprintf("upload tenant ranges %s: %v",
							locality, uploadErr))
				} else {
					artifacts++
				}
			}
		}
	}

	return artifacts, errs
}

// uploadClusterTables queries cluster-wide internal tables and system
// tables and uploads the results as tab-separated text.
func (s *systemStatusServer) uploadClusterTables(
	ctx context.Context, client *uploadServerRPCClient, doRedact bool,
) (artifacts int32, errs []string) {
	allTables := make(
		[]uploadTableDef, 0,
		len(uploadClusterWideTables)+len(uploadSystemTables),
	)
	allTables = append(allTables, uploadClusterWideTables...)
	allTables = append(allTables, uploadSystemTables...)

	for _, table := range allTables {
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
		if err != nil && !doRedact && table.queryFallback != "" {
			// Try fallback query.
			it, err = s.internalExecutor.QueryIteratorEx(
				ctx,
				redact.RedactableString(
					"upload-"+table.name+"-fallback",
				),
				nil, // txn
				sessiondata.NodeUserSessionDataOverride,
				table.queryFallback,
			)
		}
		if err != nil {
			errs = append(errs,
				fmt.Sprintf("table %s: %v", table.name, err))
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
			errs = append(errs,
				fmt.Sprintf("table %s iteration: %v",
					table.name, iterErr))
			// Still upload partial data if we have any.
		}

		sanitizedName := strings.ReplaceAll(table.name, ".", "_")
		artifactName := fmt.Sprintf("cluster/%s.txt", sanitizedName)
		if uploadErr := client.uploadArtifactBytes(
			ctx, artifactName, buf.Bytes(),
		); uploadErr != nil {
			errs = append(errs,
				fmt.Sprintf("upload table %s: %v",
					table.name, uploadErr))
		} else {
			artifacts++
		}
	}
	return artifacts, errs
}

// uploadClusterWideTables mirrors the cluster-wide table list from
// pkg/cli/zip_table_registry.go (zipInternalTablesPerCluster). When
// redaction is requested, queryRedacted is used; otherwise query is
// used.
var uploadClusterWideTables = []uploadTableDef{
	{
		name:  "crdb_internal.cluster_contention_events",
		query: "SELECT * FROM crdb_internal.cluster_contention_events",
		queryRedacted: "SELECT table_id, index_id, " +
			"IF(crdb_internal.is_system_table_key(key), " +
			"crdb_internal.pretty_key(key, 0), 'redacted') as pretty_key, " +
			"num_contention_events, cumulative_contention_time, txn_id, count " +
			"FROM crdb_internal.cluster_contention_events",
	},
	{
		name:          "crdb_internal.cluster_database_privileges",
		query:         "SELECT * FROM crdb_internal.cluster_database_privileges",
		queryRedacted: "SELECT database_name, grantee, privilege_type, is_grantable FROM crdb_internal.cluster_database_privileges",
	},
	{
		name:  "crdb_internal.cluster_distsql_flows",
		query: "SELECT * FROM crdb_internal.cluster_distsql_flows",
		queryRedacted: "SELECT flow_id, node_id, since, " +
			"crdb_internal.hide_sql_constants(stmt) as stmt " +
			"FROM crdb_internal.cluster_distsql_flows",
	},
	{
		name:          "crdb_internal.cluster_inspect_errors",
		query:         "SELECT * FROM crdb_internal.cluster_inspect_errors",
		queryRedacted: "SELECT error_id, job_id, error_type, aost, database_id, schema_id, id FROM crdb_internal.cluster_inspect_errors",
	},
	{
		name:  "crdb_internal.cluster_locks",
		query: "SELECT * FROM crdb_internal.cluster_locks",
		queryRedacted: "SELECT range_id, table_id, database_name, schema_name, " +
			"table_name, index_name, txn_id, ts, lock_strength, durability, " +
			"granted, contended, duration, isolation_level " +
			"FROM crdb_internal.cluster_locks",
	},
	{
		name:  "crdb_internal.cluster_queries",
		query: "SELECT * FROM crdb_internal.cluster_queries",
		queryRedacted: "SELECT query_id, txn_id, node_id, session_id, user_name, " +
			"start, application_name, distributed, phase, full_scan, " +
			"crdb_internal.hide_sql_constants(query) as query, " +
			"num_txn_retries, num_txn_auto_retries " +
			"FROM crdb_internal.cluster_queries",
	},
	{
		name:  "crdb_internal.cluster_sessions",
		query: "SELECT * FROM crdb_internal.cluster_sessions",
		queryRedacted: "SELECT node_id, session_id, user_name, application_name, " +
			"num_txns_executed, session_start, active_query_start, kv_txn, " +
			"alloc_bytes, max_alloc_bytes, status, session_end, " +
			"crdb_internal.hide_sql_constants(active_queries) as active_queries, " +
			"crdb_internal.hide_sql_constants(last_active_query) as last_active_query, " +
			"trace_id, goroutine_id " +
			"FROM crdb_internal.cluster_sessions",
	},
	{
		name: "crdb_internal.cluster_settings",
		query: `SELECT
			variable,
			CASE
			  WHEN sensitive THEN '<redacted>'
			  ELSE value
			END value,
			type,
			public,
			sensitive,
			reportable,
			description,
			default_value,
			origin
		FROM crdb_internal.cluster_settings`,
		queryRedacted: `SELECT
			variable,
			CASE
			  WHEN NOT reportable AND value != default_value THEN '<redacted>'
			  WHEN sensitive THEN '<redacted>'
			  ELSE value
			END value,
			type,
			public,
			sensitive,
			reportable,
			description,
			default_value,
			origin
		FROM crdb_internal.cluster_settings`,
	},
	{
		name: "crdb_internal.probe_ranges_1s_read_limit_100",
		query: `SELECT * FROM crdb_internal.probe_ranges(INTERVAL '1000ms', 'read')` +
			` WHERE error != '' ORDER BY end_to_end_latency_ms DESC LIMIT 100`,
		queryRedacted: `SELECT * FROM crdb_internal.probe_ranges(INTERVAL '1000ms', 'read')` +
			` WHERE error != '' ORDER BY end_to_end_latency_ms DESC LIMIT 100`,
	},
	{
		name:  "crdb_internal.cluster_transactions",
		query: "SELECT * FROM crdb_internal.cluster_transactions",
		queryRedacted: "SELECT id, node_id, session_id, start, application_name, " +
			"num_stmts, num_retries, num_auto_retries, isolation_level, " +
			"priority, quality_of_service " +
			"FROM crdb_internal.cluster_transactions",
	},
	{
		name:  "crdb_internal.create_function_statements",
		query: `SELECT * FROM "".crdb_internal.create_function_statements`,
		queryRedacted: "SELECT database_id, database_name, schema_id, function_id, " +
			"function_name, " +
			"crdb_internal.hide_sql_constants(create_statement) as create_statement " +
			`FROM "".crdb_internal.create_function_statements`,
	},
	{
		name:  "crdb_internal.create_trigger_statements",
		query: `SELECT * FROM "".crdb_internal.create_trigger_statements`,
		queryRedacted: "SELECT database_id, database_name, schema_id, schema_name, " +
			"table_id, table_name, trigger_id, trigger_name, " +
			"crdb_internal.hide_sql_constants(create_statement) as create_statement " +
			`FROM "".crdb_internal.create_trigger_statements`,
	},
	{
		name:  "crdb_internal.create_procedure_statements",
		query: `SELECT * FROM "".crdb_internal.create_procedure_statements`,
		queryRedacted: "SELECT database_id, database_name, schema_id, procedure_id, " +
			"procedure_name, " +
			"crdb_internal.hide_sql_constants(create_statement) as create_statement " +
			`FROM "".crdb_internal.create_procedure_statements`,
	},
	{
		name:  "crdb_internal.create_schema_statements",
		query: `SELECT * FROM "".crdb_internal.create_schema_statements`,
		queryRedacted: "SELECT database_id, database_name, schema_name, descriptor_id, " +
			"create_statement " +
			`FROM "".crdb_internal.create_schema_statements`,
	},
	{
		name:  "crdb_internal.create_statements",
		query: `SELECT * FROM "".crdb_internal.create_statements`,
		queryRedacted: "SELECT database_id, database_name, schema_name, descriptor_id, " +
			"descriptor_type, state, validate_statements, has_partitions, " +
			"is_multi_region, is_virtual, is_temporary, " +
			"crdb_internal.hide_sql_constants(create_statement) as create_statement, " +
			"crdb_internal.hide_sql_constants(fk_statements) as fk_statements, " +
			"crdb_internal.hide_sql_constants(create_nofks) as create_nofks, " +
			"crdb_internal.redact(create_redactable) as create_redactable " +
			`FROM "".crdb_internal.create_statements`,
	},
	{
		name:  "crdb_internal.create_type_statements",
		query: `SELECT * FROM "".crdb_internal.create_type_statements`,
		queryRedacted: "SELECT database_id, database_name, schema_name, descriptor_id, " +
			"descriptor_name, " +
			"crdb_internal.hide_sql_constants(create_statement) as create_statement " +
			`FROM "".crdb_internal.create_type_statements`,
	},
	{
		name:          "crdb_internal.cluster_replication_spans",
		query:         `SELECT * FROM "".crdb_internal.cluster_replication_spans`,
		queryRedacted: `SELECT job_id, resolved, resolved_age FROM "".crdb_internal.cluster_replication_spans`,
	},
	{
		name:          "crdb_internal.logical_replication_spans",
		query:         `SELECT * FROM "".crdb_internal.logical_replication_spans`,
		queryRedacted: `SELECT job_id, resolved, resolved_age FROM "".crdb_internal.logical_replication_spans`,
	},
	{
		name:  "crdb_internal.default_privileges",
		query: "SELECT * FROM crdb_internal.default_privileges",
		queryRedacted: "SELECT database_name, schema_name, role, for_all_roles, " +
			"object_type, grantee, privilege_type, is_grantable " +
			"FROM crdb_internal.default_privileges",
	},
	{
		name:          "crdb_internal.index_usage_statistics",
		query:         "SELECT * FROM crdb_internal.index_usage_statistics",
		queryRedacted: "SELECT table_id, index_id, total_reads, last_read FROM crdb_internal.index_usage_statistics",
	},
	{
		name:  "crdb_internal.invalid_objects",
		query: "SELECT * FROM crdb_internal.invalid_objects",
		queryRedacted: "SELECT id, database_name, schema_name, obj_name, " +
			"crdb_internal.redact(error_redactable) as error_redactable " +
			"FROM crdb_internal.invalid_objects",
	},
	{
		name:  "crdb_internal.jobs",
		query: "SELECT * FROM crdb_internal.jobs",
		queryRedacted: "SELECT job_id, job_type, description, user_name, status, " +
			"running_status, created, finished, modified, " +
			"fraction_completed, high_water_timestamp, coordinator_id " +
			"FROM crdb_internal.jobs",
	},
	{
		name:  "crdb_internal.system_jobs",
		query: "SELECT * FROM crdb_internal.system_jobs",
		queryRedacted: `SELECT
			"id",
			"status",
			"created",
			'redacted' AS "payload",
			"progress",
			"created_by_type",
			"created_by_id",
			"claim_session_id",
			"claim_instance_id",
			"num_runs",
			"last_run"
			FROM crdb_internal.system_jobs`,
	},
	{
		name:          "crdb_internal.kv_system_privileges",
		query:         "SELECT * FROM crdb_internal.kv_system_privileges",
		queryRedacted: "SELECT username, path, privileges, grant_options, user_id FROM crdb_internal.kv_system_privileges",
	},
	{
		name:          "crdb_internal.kv_node_liveness",
		query:         "SELECT * FROM crdb_internal.kv_node_liveness",
		queryRedacted: "SELECT node_id, epoch, expiration, draining, membership FROM crdb_internal.kv_node_liveness",
	},
	{
		name:  "crdb_internal.kv_node_status",
		query: "SELECT * FROM crdb_internal.kv_node_status",
		queryRedacted: `SELECT
				"node_id",
				"network",
				'<redacted>' as address,
				"attrs",
				"locality",
				"server_version",
				"go_version",
				"tag",
				"time",
				"revision",
				"cgo_compiler",
				"platform",
				"distribution",
				"type",
				"dependencies",
				"started_at",
				"updated_at",
				"metrics",
				'<redacted>' as args,
				'<redacted>' as env,
				"activity"
			FROM crdb_internal.kv_node_status`,
	},
	{
		name:  "crdb_internal.kv_store_status",
		query: "SELECT * FROM crdb_internal.kv_store_status",
		queryRedacted: "SELECT node_id, store_id, attrs, capacity, available, used, " +
			"logical_bytes, range_count, lease_count, writes_per_second, " +
			"bytes_per_replica, writes_per_replica, metrics, properties " +
			"FROM crdb_internal.kv_store_status",
	},
	{
		name:  "crdb_internal.partitions",
		query: "SELECT * FROM crdb_internal.partitions",
		queryRedacted: "SELECT table_id, index_id, parent_name, name, columns, " +
			"column_names, zone_id, subzone_id " +
			"FROM crdb_internal.partitions",
	},
	{
		name:          "crdb_internal.regions",
		query:         "SELECT * FROM crdb_internal.regions",
		queryRedacted: "SELECT region, zones FROM crdb_internal.regions",
	},
	{
		name:  "crdb_internal.schema_changes",
		query: "SELECT * FROM crdb_internal.schema_changes",
		queryRedacted: "SELECT table_id, parent_id, name, type, target_id, " +
			"target_name, state, direction " +
			"FROM crdb_internal.schema_changes",
	},
	{
		name:  "crdb_internal.super_regions",
		query: "SELECT * FROM crdb_internal.super_regions",
		queryRedacted: "SELECT id, database_name, super_region_name, regions " +
			"FROM crdb_internal.super_regions",
	},
	{
		name:  "crdb_internal.kv_protected_ts_records",
		query: "SELECT * FROM crdb_internal.kv_protected_ts_records",
		queryRedacted: "SELECT id, ts, meta_type, meta, num_spans, spans, verified, " +
			"target, decoded_meta, decoded_target, num_ranges, last_updated " +
			"FROM crdb_internal.kv_protected_ts_records",
	},
	{
		name:  "crdb_internal.table_indexes",
		query: "SELECT * FROM crdb_internal.table_indexes",
		queryRedacted: "SELECT descriptor_id, descriptor_name, index_id, index_name, " +
			"index_type, is_unique, is_inverted, is_sharded, is_visible, " +
			"shard_bucket_count, created_at " +
			"FROM crdb_internal.table_indexes",
	},
	{
		name: "crdb_internal.transaction_contention_events",
		query: `
WITH contention_fingerprints AS (
    SELECT DISTINCT waiting_stmt_fingerprint_id, blocking_txn_fingerprint_id
    FROM crdb_internal.transaction_contention_events
    WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
),
fingerprint_queries AS (
    SELECT DISTINCT fingerprint_id, metadata->>'query' as query
    FROM system.statement_statistics ss
    WHERE EXISTS (
        SELECT 1 FROM contention_fingerprints cf
        WHERE cf.waiting_stmt_fingerprint_id = ss.fingerprint_id
    )
),
transaction_fingerprints AS (
    SELECT DISTINCT fingerprint_id, transaction_fingerprint_id
    FROM system.statement_statistics ss
    WHERE EXISTS (
        SELECT 1 FROM contention_fingerprints cf
        WHERE cf.waiting_stmt_fingerprint_id = ss.fingerprint_id
    )
),
transaction_queries AS (
    SELECT tf.transaction_fingerprint_id, array_agg(fq.query) as queries
    FROM fingerprint_queries fq
    JOIN transaction_fingerprints tf ON tf.fingerprint_id = fq.fingerprint_id
    GROUP BY tf.transaction_fingerprint_id
)
SELECT collection_ts,
       contention_duration,
       waiting_txn_id,
       waiting_txn_fingerprint_id,
       waiting_stmt_fingerprint_id,
       fq.query AS waiting_stmt_query,
       blocking_txn_id,
       blocking_txn_fingerprint_id,
       tq.queries AS blocking_txn_queries_unordered,
       contending_pretty_key,
       index_name,
       table_name,
       database_name
FROM crdb_internal.transaction_contention_events
LEFT JOIN fingerprint_queries fq ON fq.fingerprint_id = waiting_stmt_fingerprint_id
LEFT JOIN transaction_queries tq ON tq.transaction_fingerprint_id = blocking_txn_fingerprint_id
WHERE fq.fingerprint_id != '\x0000000000000000' AND tq.transaction_fingerprint_id != '\x0000000000000000'
`,
		queryFallback: `
SELECT collection_ts,
       contention_duration,
       waiting_txn_id,
       waiting_txn_fingerprint_id,
       waiting_stmt_fingerprint_id,
       blocking_txn_id,
       blocking_txn_fingerprint_id,
       contending_pretty_key,
       index_name,
       table_name,
       database_name
FROM crdb_internal.transaction_contention_events
`,
		queryRedacted: "SELECT collection_ts, blocking_txn_id, " +
			"blocking_txn_fingerprint_id, waiting_txn_id, " +
			"waiting_txn_fingerprint_id, contention_duration, " +
			"IF(crdb_internal.is_system_table_key(contending_key), " +
			"crdb_internal.pretty_key(contending_key, 0), 'redacted') " +
			"as contending_pretty_key, contention_type " +
			"FROM crdb_internal.transaction_contention_events",
	},
	{
		name:  "crdb_internal.zones",
		query: "SELECT * FROM crdb_internal.zones",
		queryRedacted: "SELECT zone_id, subzone_id, target, range_name, " +
			"database_name, schema_name, table_name, index_name, " +
			"partition_name, raw_config_yaml, raw_config_sql, " +
			"raw_config_protobuf, full_config_yaml, full_config_sql " +
			"FROM crdb_internal.zones",
	},
	{
		name: "cluster_settings_history",
		query: `
WITH setting_events AS (
	SELECT
		timestamp,
		info::jsonb AS info_json
	FROM system.eventlog
	WHERE "eventType" = 'set_cluster_setting'
)
SELECT
	info_json ->> 'SettingName' as setting_name,
	CASE
      WHEN cs.sensitive AND info_json ->> 'Value' <> 'DEFAULT' THEN '<redacted>'
      ELSE info_json ->> 'Value'
	END value,
	info_json ->> 'DefaultValue' as default_value,
	cs.default_value as current_default_value,
	info_json ->> 'ApplicationName' as application_name,
	se.timestamp
FROM setting_events se
JOIN crdb_internal.cluster_settings cs on cs.variable = se.info_json ->> 'SettingName'
ORDER BY setting_name, timestamp`,
		queryRedacted: `
WITH setting_events AS (
	SELECT
		timestamp,
		info::jsonb AS info_json
	FROM system.eventlog
	WHERE "eventType" = 'set_cluster_setting'
)
SELECT
	info_json ->> 'SettingName' as setting_name,
	CASE
      WHEN (cs.sensitive OR NOT cs.reportable) AND info_json ->> 'Value' <> 'DEFAULT' THEN '<redacted>'
      ELSE info_json ->> 'Value'
 	END value,
	info_json ->> 'DefaultValue' as default_value,
	cs.default_value as current_default_value,
	info_json ->> 'ApplicationName' as application_name,
	se.timestamp
FROM setting_events se
JOIN crdb_internal.cluster_settings cs on cs.variable = se.info_json ->> 'SettingName'
ORDER BY setting_name, timestamp`,
	},
}

// uploadSystemTables mirrors the system table list from
// pkg/cli/zip_table_registry.go (zipSystemTables). When redaction is
// requested, queryRedacted is used; otherwise query is used.
var uploadSystemTables = []uploadTableDef{
	{
		name:          "system.database_role_settings",
		query:         "SELECT * FROM system.database_role_settings",
		queryRedacted: "SELECT database_id, role_name, settings FROM system.database_role_settings",
	},
	{
		name: "system.descriptor",
		query: `SELECT
				id,
				descriptor
			FROM system.descriptor`,
		queryRedacted: `SELECT
				id,
				crdb_internal.redact_descriptor(descriptor) AS descriptor
			FROM system.descriptor`,
	},
	{
		name:  "system.eventlog",
		query: "SELECT * FROM system.eventlog",
		queryRedacted: `SELECT timestamp, "eventType", "targetID", "reportingID", ` +
			`"uniqueID" FROM system.eventlog`,
	},
	{
		name:          "system.external_connections",
		query:         "SELECT * FROM system.external_connections",
		queryRedacted: "SELECT connection_name, created, updated, connection_type FROM system.external_connections",
	},
	{
		name:  "system.inspect_errors",
		query: "SELECT * FROM system.inspect_errors",
		queryRedacted: "SELECT error_id, job_id, error_type, aost, database_id, " +
			"schema_id, id, details, crdb_internal_expiration " +
			"FROM system.inspect_errors",
	},
	{
		name:  "system.jobs",
		query: "SELECT * FROM system.jobs",
		queryRedacted: `SELECT id,
			status,
			created,
			created_by_type,
			created_by_id,
			claim_session_id,
			claim_instance_id,
			num_runs,
			last_run
			FROM system.jobs`,
	},
	{
		name:  "system.job_info",
		query: "SELECT * FROM system.job_info",
		queryRedacted: `SELECT job_id,
			info_key,
			written,
			'redacted' AS value
			FROM system.job_info`,
	},
	{
		name:          "system.job_progress",
		query:         "SELECT * FROM system.job_progress",
		queryRedacted: "SELECT job_id, written, fraction, resolved FROM system.job_progress",
	},
	{
		name:          "system.job_progress_history",
		query:         "SELECT * FROM system.job_progress_history",
		queryRedacted: "SELECT job_id, written, fraction, resolved FROM system.job_progress_history",
	},
	{
		name:          "system.job_status",
		query:         "SELECT * FROM system.job_status",
		queryRedacted: "SELECT job_id, written, status FROM system.job_status",
	},
	{
		name:          "system.job_message",
		query:         "SELECT * FROM system.job_message",
		queryRedacted: "SELECT job_id, written, kind, message FROM system.job_message",
	},
	{
		name:  "system.lease",
		query: "SELECT * FROM system.lease",
		queryRedacted: "SELECT desc_id, version, sql_instance_id, session_id, " +
			"crdb_region FROM system.lease",
	},
	{
		name:  "system.locations",
		query: "SELECT * FROM system.locations",
		queryRedacted: `SELECT "localityKey", "localityValue", latitude, longitude ` +
			"FROM system.locations",
	},
	{
		name:          "system.migrations",
		query:         "SELECT * FROM system.migrations",
		queryRedacted: "SELECT major, minor, patch, internal, completed_at FROM system.migrations",
	},
	{
		name:  "system.mvcc_statistics",
		query: "SELECT * FROM system.mvcc_statistics",
		queryRedacted: "SELECT created_at, database_id, table_id, index_id, " +
			"statistics FROM system.mvcc_statistics",
	},
	{
		name:  "system.namespace",
		query: "SELECT * FROM system.namespace",
		queryRedacted: `SELECT "parentID", "parentSchemaID", name, id ` +
			"FROM system.namespace",
	},
	{
		name:  "system.prepared_transactions",
		query: "SELECT * FROM system.prepared_transactions",
		queryRedacted: "SELECT global_id, transaction_id, transaction_key, prepared, " +
			"owner, database, heuristic FROM system.prepared_transactions",
	},
	{
		name:          "system.privileges",
		query:         "SELECT * FROM system.privileges",
		queryRedacted: "SELECT username, path, privileges, grant_options FROM system.privileges",
	},
	{
		name:  "system.protected_ts_meta",
		query: "SELECT * FROM system.protected_ts_meta",
		queryRedacted: "SELECT singleton, version, num_records, num_spans, " +
			"total_bytes FROM system.protected_ts_meta",
	},
	{
		name:  "system.protected_ts_records",
		query: "SELECT * FROM system.protected_ts_records",
		queryRedacted: "SELECT id, ts, meta_type, meta, num_spans, spans, " +
			"verified, target FROM system.protected_ts_records",
	},
	{
		name:  "system.rangelog",
		query: "SELECT * FROM system.rangelog",
		queryRedacted: `SELECT timestamp, "rangeID", "storeID", "eventType", ` +
			`"otherRangeID", info, "uniqueID" FROM system.rangelog`,
	},
	{
		name:          "system.region_liveness",
		query:         "SELECT * FROM system.region_liveness",
		queryRedacted: "SELECT crdb_region, unavailable_at FROM system.region_liveness",
	},
	{
		name:  "system.replication_constraint_stats",
		query: "SELECT * FROM system.replication_constraint_stats",
		queryRedacted: "SELECT zone_id, subzone_id, type, config, report_id, " +
			"violation_start, violating_ranges " +
			"FROM system.replication_constraint_stats",
	},
	{
		name:  "system.replication_critical_localities",
		query: "SELECT * FROM system.replication_critical_localities",
		queryRedacted: "SELECT zone_id, subzone_id, locality, report_id, " +
			"at_risk_ranges FROM system.replication_critical_localities",
	},
	{
		name:  "system.replication_stats",
		query: "SELECT * FROM system.replication_stats",
		queryRedacted: "SELECT zone_id, subzone_id, report_id, total_ranges, " +
			"unavailable_ranges, under_replicated_ranges, " +
			"over_replicated_ranges FROM system.replication_stats",
	},
	{
		name:          "system.reports_meta",
		query:         "SELECT * FROM system.reports_meta",
		queryRedacted: "SELECT id, generated FROM system.reports_meta",
	},
	{
		name:          "system.role_id_seq",
		query:         "SELECT * FROM system.role_id_seq",
		queryRedacted: "SELECT last_value, log_cnt, is_called FROM system.role_id_seq",
	},
	{
		name:          "system.role_members",
		query:         "SELECT * FROM system.role_members",
		queryRedacted: `SELECT role, member, "isAdmin" FROM system.role_members`,
	},
	{
		name:          "system.role_options",
		query:         "SELECT * FROM system.role_options",
		queryRedacted: "SELECT username, option, value FROM system.role_options",
	},
	{
		name:  "system.scheduled_jobs",
		query: "SELECT * FROM system.scheduled_jobs",
		queryRedacted: "SELECT schedule_id, schedule_name, created, owner, next_run, " +
			"schedule_state, schedule_expr, schedule_details, executor_type " +
			"FROM system.scheduled_jobs",
	},
	{
		name: "system.settings",
		query: `
SELECT
     name,
     CASE
          WHEN cs.sensitive THEN '<redacted>'
          ELSE s.value
     END value,
	s."lastUpdated",
	s."valueType"
FROM system.settings s
JOIN crdb_internal.cluster_settings cs ON cs.variable = s.name`,
		queryRedacted: `
SELECT
     name,
     CASE
          WHEN cs.sensitive THEN '<redacted>'
          WHEN NOT cs.reportable THEN '<redacted>'
          ELSE s.value
     END value,
	s."lastUpdated",
	s."valueType"
FROM system.settings s
JOIN crdb_internal.cluster_settings cs ON cs.variable = s.name`,
	},
	{
		name:          "system.span_configurations",
		query:         "SELECT * FROM system.span_configurations",
		queryRedacted: "SELECT config, start_key, end_key FROM system.span_configurations",
	},
	{
		name:  "system.sql_instances",
		query: "SELECT * FROM system.sql_instances",
		queryRedacted: `SELECT
			"id",
			'<redacted>' as addr,
			"session_id",
			'<redacted>' as locality,
			'<redacted>' as sql_addr
			FROM system.sql_instances`,
	},
	{
		name: "system.sql_stats_cardinality",
		query: `
			SELECT table_name, aggregated_ts, row_count
			FROM (
					SELECT 'system.statement_statistics' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.statement_statistics
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.transaction_statistics' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.transaction_statistics
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.statement_activity' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.statement_activity
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.transaction_activity' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.transaction_activity
					GROUP BY aggregated_ts
			)
			ORDER BY table_name, aggregated_ts DESC`,
		queryRedacted: `
			SELECT table_name, aggregated_ts, row_count
			FROM (
					SELECT 'system.statement_statistics' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.statement_statistics
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.transaction_statistics' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.transaction_statistics
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.statement_activity' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.statement_activity
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.transaction_activity' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.transaction_activity
					GROUP BY aggregated_ts
			)
			ORDER BY table_name, aggregated_ts DESC`,
	},
	{
		name:          "system.sqlliveness",
		query:         "SELECT * FROM system.sqlliveness",
		queryRedacted: "SELECT session_id, expiration FROM system.sqlliveness",
	},
	{
		name:  "system.statement_diagnostics",
		query: "SELECT * FROM system.statement_diagnostics",
		queryRedacted: "SELECT id, statement_fingerprint, collected_at, statement, " +
			"error FROM system.statement_diagnostics",
	},
	{
		name:  "system.statement_diagnostics_requests",
		query: "SELECT * FROM system.statement_diagnostics_requests",
		queryRedacted: "SELECT id, completed, statement_fingerprint, " +
			"statement_diagnostics_id, requested_at, min_execution_latency, " +
			"expires_at, sampling_probability, plan_gist, anti_plan_gist, " +
			"redacted, username " +
			"FROM system.statement_diagnostics_requests",
	},
	{
		name:          "system.statement_hints",
		query:         "SELECT * FROM system.statement_hints",
		queryRedacted: "SELECT row_id, fingerprint, hint, created_at FROM system.statement_hints",
	},
	{
		name: "system.statement_statistics_limit_5000",
		query: `SELECT max(ss.aggregated_ts),
       ss.fingerprint_id,
       ss.transaction_fingerprint_id,
       ss.plan_hash,
       ss.app_name,
       ss.agg_interval,
       merge_stats_metadata(ss.metadata)    AS metadata,
       merge_statement_stats(ss.statistics) AS statistics,
       ss.plan,
       ss.index_recommendations
     FROM system.public.statement_statistics ss
     WHERE aggregated_ts > (now() - INTERVAL '1 hour') AND
 ( transaction_fingerprint_id in (SELECT DISTINCT(blocking_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      union
                                      SELECT DISTINCT(waiting_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      )
    OR transaction_fingerprint_id in (SELECT ss_cpu.transaction_fingerprint_id
                                      FROM system.public.statement_statistics ss_cpu
                                      group by ss_cpu.transaction_fingerprint_id, ss_cpu.cpu_sql_nanos
                                      ORDER BY ss_cpu.cpu_sql_nanos desc limit 100))
GROUP BY ss.aggregated_ts,
         ss.app_name,
         ss.fingerprint_id,
         ss.transaction_fingerprint_id,
         ss.plan_hash,
         ss.agg_interval,
         ss.plan,
         ss.index_recommendations
limit 5000;`,
		queryRedacted: `SELECT max(ss.aggregated_ts),
       ss.fingerprint_id,
       ss.transaction_fingerprint_id,
       ss.plan_hash,
       ss.app_name,
       ss.agg_interval,
       merge_stats_metadata(ss.metadata)    AS metadata,
       merge_statement_stats(ss.statistics) AS statistics,
       ss.plan,
       ss.index_recommendations
     FROM system.public.statement_statistics ss
     WHERE aggregated_ts > (now() - INTERVAL '1 hour') AND
 ( transaction_fingerprint_id in (SELECT DISTINCT(blocking_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      union
                                      SELECT DISTINCT(waiting_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      )
    OR transaction_fingerprint_id in (SELECT ss_cpu.transaction_fingerprint_id
                                      FROM system.public.statement_statistics ss_cpu
                                      group by ss_cpu.transaction_fingerprint_id, ss_cpu.cpu_sql_nanos
                                      ORDER BY ss_cpu.cpu_sql_nanos desc limit 100))
GROUP BY ss.aggregated_ts,
         ss.app_name,
         ss.fingerprint_id,
         ss.transaction_fingerprint_id,
         ss.plan_hash,
         ss.agg_interval,
         ss.plan,
         ss.index_recommendations
limit 5000;`,
	},
	{
		name:  "system.table_metadata",
		query: "SELECT * FROM system.table_metadata",
		queryRedacted: "SELECT db_id, table_id, db_name, schema_name, table_name, " +
			"total_columns, total_indexes, store_ids, " +
			"replication_size_bytes, total_ranges, total_live_data_bytes, " +
			"total_data_bytes, perc_live_data, last_update_error, " +
			"last_updated, table_type, details " +
			"FROM system.table_metadata",
	},
	{
		name:  "system.table_statistics",
		query: "SELECT * FROM system.table_statistics",
		queryRedacted: `SELECT "tableID", "statisticID", name, "columnIDs", ` +
			`"createdAt", "rowCount", "distinctCount", "nullCount", ` +
			`"avgSize", "partialPredicate", "fullStatisticID", ` +
			`"delayDelete" FROM system.table_statistics`,
	},
	{
		name:          "system.table_statistics_locks",
		query:         "SELECT * FROM system.table_statistics_locks",
		queryRedacted: "SELECT table_id, kind, job_ids FROM system.table_statistics_locks",
	},
	{
		name:  "system.task_payloads",
		query: "SELECT * FROM system.task_payloads",
		queryRedacted: "SELECT id, created, owner, owner_id, min_version, type " +
			"FROM system.task_payloads",
	},
	{
		name:  "system.tenant_tasks",
		query: "SELECT * FROM system.tenant_tasks",
		queryRedacted: "SELECT tenant_id, issuer, task_id, created, payload_id, " +
			"owner, owner_id FROM system.tenant_tasks",
	},
	{
		name:  "system.tenant_settings",
		query: "SELECT * FROM system.tenant_settings",
		queryRedacted: `SELECT * FROM (
			SELECT *
			FROM system.tenant_settings
			WHERE value_type <> 's'
    	) UNION (
			SELECT tenant_id, name, '<redacted>' as value, last_updated, value_type, reason
			FROM system.tenant_settings
			WHERE value_type = 's'
    	)`,
	},
	{
		name:  "system.tenant_usage",
		query: "SELECT * FROM system.tenant_usage",
		queryRedacted: "SELECT tenant_id, instance_id, next_instance_id, last_update, " +
			"ru_burst_limit, ru_refill_rate, ru_current, " +
			"current_share_sum, total_consumption, instance_seq, " +
			"instance_shares, instance_lease " +
			"FROM system.tenant_usage",
	},
	{
		name:          "system.tenants",
		query:         "SELECT * FROM system.tenants",
		queryRedacted: "SELECT id, active, info FROM system.tenants",
	},
	{
		name:  "system.transaction_diagnostics",
		query: "SELECT * FROM system.transaction_diagnostics",
		queryRedacted: "SELECT id, transaction_fingerprint_id, " +
			"statement_fingerprint_ids, transaction_fingerprint, " +
			"collected_at, error " +
			"FROM system.transaction_diagnostics",
	},
	{
		name:  "system.transaction_diagnostics_requests",
		query: "SELECT * FROM system.transaction_diagnostics_requests",
		queryRedacted: "SELECT id, completed, transaction_fingerprint_id, " +
			"statement_fingerprint_ids, transaction_diagnostics_id, " +
			"requested_at, min_execution_latency, expires_at, " +
			"sampling_probability, redacted, username " +
			"FROM system.transaction_diagnostics_requests",
	},
	{
		name: "system.zones",
		query: `SELECT
			"id",
			crdb_internal.pb_to_json('cockroach.config.zonepb.ZoneConfig', config)
			FROM system.zones`,
		queryRedacted: `SELECT "id" FROM system.zones`,
	},
}
