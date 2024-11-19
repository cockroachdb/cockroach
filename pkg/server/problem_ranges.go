// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"slices"

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
		resp   *serverpb.RangesResponse
		err    error
	}

	responses := make(chan nodeResponse)
	// TODO(bram): consider abstracting out this repeated pattern.
	for nodeID := range isLiveMap {
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			ctx, "server.statusServer: requesting remote ranges",
			func(ctx context.Context) {
				status, err := s.dialNode(ctx, nodeID)
				var rangesResponse *serverpb.RangesResponse
				if err == nil {
					req := &serverpb.RangesRequest{}
					rangesResponse, err = status.Ranges(ctx, req)
				}
				response := nodeResponse{
					nodeID: nodeID,
					resp:   rangesResponse,
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
			var problems serverpb.ProblemRangesResponse_NodeProblems
			for _, info := range resp.resp.Ranges {
				if len(info.ErrorMessage) != 0 {
					response.ProblemsByNodeID[resp.nodeID] = serverpb.ProblemRangesResponse_NodeProblems{
						ErrorMessage: info.ErrorMessage,
					}
					continue
				}
				if info.Problems.Unavailable {
					problems.UnavailableRangeIDs =
						append(problems.UnavailableRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.LeaderNotLeaseHolder {
					problems.RaftLeaderNotLeaseHolderRangeIDs =
						append(problems.RaftLeaderNotLeaseHolderRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.NoRaftLeader {
					problems.NoRaftLeaderRangeIDs =
						append(problems.NoRaftLeaderRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.Underreplicated {
					problems.UnderreplicatedRangeIDs =
						append(problems.UnderreplicatedRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.Overreplicated {
					problems.OverreplicatedRangeIDs =
						append(problems.OverreplicatedRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.NoLease {
					problems.NoLeaseRangeIDs =
						append(problems.NoLeaseRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.QuiescentEqualsTicking {
					problems.QuiescentEqualsTickingRangeIDs =
						append(problems.QuiescentEqualsTickingRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.RaftLogTooLarge {
					problems.RaftLogTooLargeRangeIDs =
						append(problems.RaftLogTooLargeRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.CircuitBreakerError {
					problems.CircuitBreakerErrorRangeIDs =
						append(problems.CircuitBreakerErrorRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.PausedFollowers {
					problems.PausedReplicaIDs =
						append(problems.PausedReplicaIDs, info.State.Desc.RangeID)
				}
				if info.Problems.RangeTooLarge {
					problems.TooLargeRangeIds =
						append(problems.TooLargeRangeIds, info.State.Desc.RangeID)
				}
			}
			slices.Sort(problems.UnavailableRangeIDs)
			slices.Sort(problems.RaftLeaderNotLeaseHolderRangeIDs)
			slices.Sort(problems.NoRaftLeaderRangeIDs)
			slices.Sort(problems.NoLeaseRangeIDs)
			slices.Sort(problems.UnderreplicatedRangeIDs)
			slices.Sort(problems.OverreplicatedRangeIDs)
			slices.Sort(problems.QuiescentEqualsTickingRangeIDs)
			slices.Sort(problems.RaftLogTooLargeRangeIDs)
			slices.Sort(problems.CircuitBreakerErrorRangeIDs)
			slices.Sort(problems.PausedReplicaIDs)
			slices.Sort(problems.TooLargeRangeIds)
			response.ProblemsByNodeID[resp.nodeID] = problems
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	return response, nil
}
