// Copyright 2014 The Cockroach Authors.
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
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (s *statusServer) Queries(
	ctx context.Context, req *serverpb.QueriesRequest,
) (*serverpb.QueriesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.QueriesResponse{
		Queries:   []serverpb.QueriesResponse_CollectedStatementStatistics{},
		LastReset:  timeutil.Now(),
	}

	localReq := &serverpb.QueriesRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.QueriesLocal(ctx)
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.Queries(ctx, localReq)
	}

	nodeQuery := func(ctx context.Context, status serverpb.StatusClient) (interface{}, error) {
		return status.Queries(ctx, localReq)
	}

	if err := s.iterateNodes(ctx, fmt.Sprintf("statement statistics for node %s", req.NodeID),
		nodeQuery,
		func(nodeID roachpb.NodeID, resp interface{}) {
			queriesResp := resp.(*serverpb.QueriesResponse)
			response.Queries = append(response.Queries, queriesResp.Queries...)
			if response.LastReset.After(queriesResp.LastReset) {
				response.LastReset = queriesResp.LastReset
			}
		},
		func(nodeID roachpb.NodeID, err error) {
			// TODO(couchand): do something here...
		},
	); err != nil {
		return nil, err
	}

	return response, nil
}

func (s *statusServer) QueriesLocal(ctx context.Context) (*serverpb.QueriesResponse, error) {
	stmtStats := s.admin.server.pgServer.SQLServer.GetUnscrubbedStmtStats()
	lastReset := s.admin.server.pgServer.SQLServer.GetStmtStatsLastReset()

	resp := &serverpb.QueriesResponse{
		Queries:   make([]serverpb.QueriesResponse_CollectedStatementStatistics, len(stmtStats)),
		LastReset: lastReset,
	}

	for i, stmt := range stmtStats {
		resp.Queries[i] = serverpb.QueriesResponse_CollectedStatementStatistics{
			Key: serverpb.QueriesResponse_StatementStatisticsKey{
				Query:   stmt.Key.Query,
				App:     stmt.Key.App,
				NodeID:  s.gossip.NodeID.Get(),
				DistSQL: stmt.Key.DistSQL,
				Failed:  stmt.Key.Failed,
			},
			Stats: stmt.Stats,
		}
	}

	return resp, nil
}
