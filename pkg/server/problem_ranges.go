// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) ProblemRanges(
	ctx context.Context, req *serverpb.ProblemRangesRequest,
) (*serverpb.ProblemRangesResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.ProblemRangesResponse{
		NodeID:           s.gossip.NodeID.Get(),
		ProblemsByNodeID: make(map[roachpb.NodeID]serverpb.ProblemRangesResponse_NodeProblems),
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	// If there is a specific nodeID requested, limited the responses to
	// just that node.
	if len(req.NodeID) > 0 {
		requestedNodeID, _, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		isLiveMap = liveness.IsLiveMap{
			requestedNodeID: liveness.IsLiveMapEntry{IsLive: true},
		}
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
			return nil, status.Errorf(codes.Internal, err.Error())
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
			}
			sort.Sort(roachpb.RangeIDSlice(problems.UnavailableRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.RaftLeaderNotLeaseHolderRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.NoRaftLeaderRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.NoLeaseRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.UnderreplicatedRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.OverreplicatedRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.QuiescentEqualsTickingRangeIDs))
			sort.Sort(roachpb.RangeIDSlice(problems.RaftLogTooLargeRangeIDs))
			response.ProblemsByNodeID[resp.nodeID] = problems
		case <-ctx.Done():
			return nil, status.Errorf(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	return response, nil
}
