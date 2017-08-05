// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"sort"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

func (s *statusServer) ProblemRanges(
	ctx context.Context, req *serverpb.ProblemRangesRequest,
) (*serverpb.ProblemRangesResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.ProblemRangesResponse{}
	if len(req.NodeID) > 0 {
		var err error
		response.NodeID, _, err = s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
		}
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	if response.NodeID != 0 {
		// If there is a specific nodeID requested, limited the responses to
		// just this node.
		if !isLiveMap[response.NodeID] {
			return nil, grpc.Errorf(codes.NotFound, "n%d is not alive", response.NodeID)
		}
		isLiveMap = map[roachpb.NodeID]bool{
			response.NodeID: true,
		}
	}

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.RangesResponse
		err    error
	}
	noRaftLeader := make(map[roachpb.RangeID]struct{})
	numNodes := len(isLiveMap)
	responses := make(chan nodeResponse)
	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()
	for nodeID, alive := range isLiveMap {
		if !alive {
			response.Failures = append(response.Failures, serverpb.Failure{
				NodeID:       nodeID,
				ErrorMessage: "node liveness reports that the node is not alive",
			})
			numNodes--
			continue
		}
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			nodeCtx, "server.statusServer: requesting remote ranges",
			func(ctx context.Context) {
				status, err := s.dialNode(nodeID)
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
			return nil, grpc.Errorf(codes.Internal, err.Error())
		}
	}
	for remainingResponses := numNodes; remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				response.Failures = append(response.Failures, serverpb.Failure{
					NodeID:       resp.nodeID,
					ErrorMessage: resp.err.Error(),
				})
				continue
			}
			for _, info := range resp.resp.Ranges {
				if len(info.ErrorMessage) != 0 {
					response.Failures = append(response.Failures, serverpb.Failure{
						NodeID:       info.SourceNodeID,
						ErrorMessage: info.ErrorMessage,
					})
					continue
				}
				if info.Problems.Unavailable {
					response.UnavailableRangeIDs =
						append(response.UnavailableRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.LeaderNotLeaseHolder {
					response.RaftLeaderNotLeaseHolderRangeIDs =
						append(response.RaftLeaderNotLeaseHolderRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.NoRaftLeader {
					noRaftLeader[info.State.Desc.RangeID] = struct{}{}
				}
				if info.Problems.Underreplicated {
					response.UnderreplicatedRangeIDs =
						append(response.UnderreplicatedRangeIDs, info.State.Desc.RangeID)
				}
				if info.Problems.NoLease {
					response.NoLeaseRangeIDs =
						append(response.NoLeaseRangeIDs, info.State.Desc.RangeID)
				}
			}
		case <-ctx.Done():
			return nil, grpc.Errorf(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	for rangeID := range noRaftLeader {
		response.NoRaftLeaderRangeIDs =
			append(response.NoRaftLeaderRangeIDs, rangeID)
	}

	sort.Sort(roachpb.RangeIDSlice(response.UnavailableRangeIDs))
	sort.Sort(roachpb.RangeIDSlice(response.RaftLeaderNotLeaseHolderRangeIDs))
	sort.Sort(roachpb.RangeIDSlice(response.NoRaftLeaderRangeIDs))
	sort.Sort(roachpb.RangeIDSlice(response.NoLeaseRangeIDs))
	sort.Sort(roachpb.RangeIDSlice(response.UnderreplicatedRangeIDs))

	return response, nil
}
