// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *systemStatusServer) ProblemRanges(
	ctx context.Context, req *serverpb.ProblemRangesRequest,
) (*serverpb.ProblemRangesResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.ProblemRangesResponse{
		NodeID:           roachpb.NodeID(s.serverIterator.getID()),
		ProblemsByNodeID: make(map[roachpb.NodeID]serverpb.ProblemRangesResponse_NodeProblems),
	}

	var isLiveMap map[roachpb.NodeID]livenesspb.NodeVitality
	// If there is a specific nodeID requested, limited the responses to
	// just that node.
	if len(req.NodeID) > 0 {
		requestedNodeID, _, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		isLiveMap = map[roachpb.NodeID]livenesspb.NodeVitality{requestedNodeID: s.nodeLiveness.GetNodeVitalityFromCache(requestedNodeID)}
	} else {
		isLiveMap = s.nodeLiveness.ScanNodeVitalityFromCache()
	}

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.ProblemRangesLocalResponse
		err    error
	}

	responses := make(chan nodeResponse)
	// TODO(bram): consider abstracting out this repeated pattern.
	for nodeID := range isLiveMap {
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			ctx, "server.statusServer: requesting problem ranges",
			func(ctx context.Context) {
				status, err := s.dialNode(ctx, nodeID)
				var problemsResponse *serverpb.ProblemRangesLocalResponse
				if err == nil {
					req := &serverpb.ProblemRangesLocalRequest{}
					problemsResponse, err = status.ProblemRangesLocal(ctx, req)
				}
				response := nodeResponse{
					nodeID: nodeID,
					resp:   problemsResponse,
					err:    err,
				}

				select {
				case responses <- response:
					// Response processed.
				case <-ctx.Done():
					// Context completed, response no longer needed.
				}
			}); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	for remainingResponses := len(isLiveMap); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				response.ProblemsByNodeID[resp.nodeID] = serverpb.ProblemRangesResponse_NodeProblems{
					ErrorMessage: resp.err.Error(),
				}
				continue
			}
			if len(resp.resp.ErrorMessage) != 0 {
				response.ProblemsByNodeID[resp.nodeID] = serverpb.ProblemRangesResponse_NodeProblems{
					ErrorMessage: resp.resp.ErrorMessage,
				}
				continue
			}
			// Copy the problem range IDs from the local response. The IDs are
			// already sorted by ProblemRangesLocal since it visits replicas in order.
			problems := serverpb.ProblemRangesResponse_NodeProblems{
				UnavailableRangeIDs:              resp.resp.UnavailableRangeIDs,
				RaftLeaderNotLeaseHolderRangeIDs: resp.resp.RaftLeaderNotLeaseHolderRangeIDs,
				NoRaftLeaderRangeIDs:             resp.resp.NoRaftLeaderRangeIDs,
				NoLeaseRangeIDs:                  resp.resp.NoLeaseRangeIDs,
				UnderreplicatedRangeIDs:          resp.resp.UnderreplicatedRangeIDs,
				OverreplicatedRangeIDs:           resp.resp.OverreplicatedRangeIDs,
				QuiescentEqualsTickingRangeIDs:   resp.resp.QuiescentEqualsTickingRangeIDs,
				RaftLogTooLargeRangeIDs:          resp.resp.RaftLogTooLargeRangeIDs,
				CircuitBreakerErrorRangeIDs:      resp.resp.CircuitBreakerErrorRangeIDs,
				PausedReplicaIDs:                 resp.resp.PausedReplicaIDs,
				TooLargeRangeIds:                 resp.resp.TooLargeRangeIds,
			}
			response.ProblemsByNodeID[resp.nodeID] = problems
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	return response, nil
}
