// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
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

	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
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
			return nil, status.Error(codes.InvalidArgument, err.Error())
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

	nodeStatement := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
		return status.Statements(ctx, localReq)
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "statement statistics",
		noTimeout,
		s.dialNode,
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
