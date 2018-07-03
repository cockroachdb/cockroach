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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (s *statusServer) Statements(
	ctx context.Context, req *serverpb.StatementsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.StatementsResponse{
		Statements: []serverpb.StatementsResponse_CollectedStatementStatistics{},
		LastReset:  timeutil.Now(),
	}

	localReq := &serverpb.StatementsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.StatementsLocal(ctx)
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.Statements(ctx, localReq)
	}

	nodeStatement := func(ctx context.Context, status serverpb.StatusClient) (interface{}, error) {
		return status.Statements(ctx, localReq)
	}

	if err := s.iterateNodes(ctx, fmt.Sprintf("statement statistics for node %s", req.NodeID),
		nodeStatement,
		func(nodeID roachpb.NodeID, resp interface{}) {
			statementsResp := resp.(*serverpb.StatementsResponse)
			response.Statements = append(response.Statements, statementsResp.Statements...)
			if response.LastReset.After(statementsResp.LastReset) {
				response.LastReset = statementsResp.LastReset
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

func (s *statusServer) StatementsLocal(ctx context.Context) (*serverpb.StatementsResponse, error) {
	stmtStats := s.admin.server.pgServer.SQLServer.GetUnscrubbedStmtStats()
	lastReset := s.admin.server.pgServer.SQLServer.GetStmtStatsLastReset()

	resp := &serverpb.StatementsResponse{
		Statements: make([]serverpb.StatementsResponse_CollectedStatementStatistics, len(stmtStats)),
		LastReset:  lastReset,
	}

	for i, stmt := range stmtStats {
		stmts, err := parser.Parse(stmt.Key.Query)
		if err != nil {
			return nil, err
		}
		pretty := tree.PrettyWithOpts(stmts[0], 80, false, 4)

		resp.Statements[i] = serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_StatementStatisticsKey{
				Statement: pretty,
				App:       stmt.Key.App,
				NodeID:    s.gossip.NodeID.Get(),
				DistSQL:   stmt.Key.DistSQL,
				Failed:    stmt.Key.Failed,
			},
			Stats: stmt.Stats,
		}
	}

	return resp, nil
}
