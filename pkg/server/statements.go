// Copyright 2014 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) Statements(
	ctx context.Context, req *serverpb.StatementsRequest,
) (*serverpb.StatementsResponse, error) {
	if req.Combined {
		combinedRequest := serverpb.CombinedStatementsStatsRequest{
			Start: req.Start,
			End:   req.End,
		}
		return s.CombinedStatementStats(ctx, &combinedRequest)
	}

	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if s.serverIterator.getID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "nodeID not set")
	}

	response := &serverpb.StatementsResponse{
		Statements:            []serverpb.StatementsResponse_CollectedStatementStatistics{},
		LastReset:             timeutil.Now(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}

	localReq := &serverpb.StatementsRequest{
		NodeID:    "local",
		FetchMode: req.FetchMode,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return statementsLocal(
				ctx,
				roachpb.NodeID(s.serverIterator.getID()),
				s.sqlServer,
				req.FetchMode)
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.Statements(ctx, localReq)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeStatement := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.Statements(ctx, localReq)
	}

	if err := s.iterateNodes(ctx, "statement statistics",
		dialFn,
		nodeStatement,
		func(nodeID roachpb.NodeID, resp interface{}) {
			statementsResp := resp.(*serverpb.StatementsResponse)
			response.Statements = append(response.Statements, statementsResp.Statements...)
			response.Transactions = append(response.Transactions, statementsResp.Transactions...)
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

func statementsLocal(
	ctx context.Context,
	nodeID roachpb.NodeID,
	sqlServer *SQLServer,
	fetchMode serverpb.StatementsRequest_FetchMode,
) (*serverpb.StatementsResponse, error) {
	var stmtStats []appstatspb.CollectedStatementStatistics
	var txnStats []appstatspb.CollectedTransactionStatistics
	var err error

	if fetchMode != serverpb.StatementsRequest_TxnStatsOnly {
		stmtStats, err = sqlServer.pgServer.SQLServer.GetUnscrubbedStmtStats(ctx)
		if err != nil {
			return nil, err
		}
	}

	if fetchMode != serverpb.StatementsRequest_StmtStatsOnly {
		txnStats, err = sqlServer.pgServer.SQLServer.GetUnscrubbedTxnStats(ctx)
		if err != nil {
			return nil, err
		}
	}

	lastReset := sqlServer.pgServer.SQLServer.GetStmtStatsLastReset()

	resp := &serverpb.StatementsResponse{
		Statements:            make([]serverpb.StatementsResponse_CollectedStatementStatistics, len(stmtStats)),
		LastReset:             lastReset,
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
		Transactions:          make([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, len(txnStats)),
	}

	for i, txn := range txnStats {
		resp.Transactions[i] = serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
			StatsData: txn,
			NodeID:    nodeID,
		}
	}

	for i, stmt := range stmtStats {
		resp.Statements[i] = serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: stmt.Key,
				NodeID:  nodeID,
			},
			ID:    stmt.ID,
			Stats: stmt.Stats,
		}
	}

	return resp, nil
}
