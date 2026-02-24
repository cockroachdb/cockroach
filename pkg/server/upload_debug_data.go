// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		ServerUrl:      req.ServerUrl,
		SessionId:      client.sessionID,
		UploadToken:    client.uploadToken,
		Redact:         req.Redact,
		CpuProfSeconds: req.CpuProfSeconds,
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

	// Use a generous timeout for the fan-out since profile collection
	// can be slow.
	fanoutTimeout := 10 * time.Minute
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

	// Collect stacks.
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

	// Collect stacks with labels.
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

	return &serverpb.UploadNodeDebugDataResponse{
		ArtifactsUploaded: artifacts,
		Errors:            errs,
	}, nil
}
