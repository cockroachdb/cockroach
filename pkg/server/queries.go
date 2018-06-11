// Copyright 2018 The Cockroach Authors.
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
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

func (s *statusServer) Queries(
	ctx context.Context, req *serverpb.QueriesRequest,
) (*serverpb.QueriesResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	response := serverpb.QueriesResponse{
		Queries: make([]roachpb.CollectedStatementStatistics, 0, 10), // TODO(couchand): why?
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	// If there is a specific nodeID requested, limit the responses to
	// just that node.
	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.admin.QueriesLocal(ctx, req)
		}
		isLiveMap = map[roachpb.NodeID]bool{
			requestedNodeID: true,
		}
	}

	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.QueriesResponse
		err    error
	}

	responses := make(chan nodeResponse)
	for nodeID := range isLiveMap {
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			nodeCtx, "server.statusServer: requesting remote queries",
			func(ctx context.Context) {
				status, err := s.dialNode(ctx, nodeID)
				var childResponse *serverpb.QueriesResponse
				if err == nil {
					childReq := &serverpb.QueriesRequest{
						NodeID: "local",
					}
					childResponse, err = status.Queries(ctx, childReq)
				}
				resp := nodeResponse{
					nodeID: nodeID,
					resp:   childResponse,
					err:    err,
				}

				select {
				case responses <- resp:
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
				// TODO(couchand): log maybe?  return the error?
				continue
			}
			response.Queries = append(response.Queries, resp.resp.Queries...)
		case <-ctx.Done():
			return nil, status.Errorf(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	return &response, nil
}

func (s *adminServer) QueriesLocal(
	ctx context.Context, req *serverpb.QueriesRequest,
) (*serverpb.QueriesResponse, error) {
	stmtStats := s.server.pgServer.SQLServer.GetUnscrubbedStmtStats()

	return &serverpb.QueriesResponse{
		Queries: stmtStats,
	}, nil
}
