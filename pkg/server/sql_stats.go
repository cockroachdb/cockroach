// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) ResetSQLStats(
	ctx context.Context, req *serverpb.ResetSQLStatsRequest,
) (*serverpb.ResetSQLStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireRepairClusterPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.ResetSQLStatsResponse{}
	controller := s.sqlServer.pgServer.SQLServer.GetSQLStatsController()

	// If we need to reset persisted stats, we delegate to SQLStatsController,
	// which will trigger a system table truncation and RPC fanout under the hood.
	if req.ResetPersistedStats {
		if err := controller.ResetClusterSQLStats(ctx); err != nil {
			return nil, err
		}

		return response, nil
	}

	localReq := &serverpb.ResetSQLStatsRequest{
		NodeID: "local",
		// Only the top level RPC handler handles the reset persisted stats.
		ResetPersistedStats: false,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			controller.ResetLocalSQLStats(ctx)
			return response, nil
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.ResetSQLStats(ctx, localReq)
	}

	resetSQLStats := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
		return status.ResetSQLStats(ctx, localReq)
	}

	var fanoutError error
	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "reset SQL statistics",
		noTimeout,
		s.dialNode,
		resetSQLStats,
		func(nodeID roachpb.NodeID, resp interface{}) {
			// Nothing to do here.
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
			if nodeFnError != nil {
				fanoutError = errors.CombineErrors(fanoutError, nodeFnError)
			}
		},
	); err != nil {
		return nil, err
	}

	return response, fanoutError
}

func (s *statusServer) DrainSqlStats(
	ctx context.Context, req *serverpb.DrainSqlStatsRequest,
) (*serverpb.DrainStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	localReq := &serverpb.DrainSqlStatsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.drainSqlStatsLocal(ctx, requestedNodeID)
		}
		statusCli, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return statusCli.DrainSqlStats(ctx, localReq)
	}

	consumeStats := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
		return status.DrainSqlStats(ctx, localReq)
	}

	var fanoutError error
	var fanoutResonses = make([]*serverpb.DrainStatsResponse, 0)
	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "reset SQL statistics",
		noTimeout,
		s.dialNode,
		consumeStats,
		func(nodeID roachpb.NodeID, resp interface{}) {
			fanoutResonses = append(fanoutResonses, resp.(*serverpb.DrainStatsResponse))
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
			if nodeFnError != nil {
				fanoutError = errors.CombineErrors(fanoutError, nodeFnError)
			}
		},
	); err != nil {
		return nil, err
	}
	return combineAllStats(fanoutResonses), fanoutError
}

func (s *statusServer) drainSqlStatsLocal(
	ctx context.Context, nodeID roachpb.NodeID,
) (*serverpb.DrainStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	statsProvider := s.sqlServer.pgServer.SQLServer.GetLocalSQLStatsProvider()
	stmtStats, txnstats, _ := statsProvider.DrainStats(ctx)
	log.VInfof(ctx, 1, "drainSqlStatsLocal: %d statement stats, %d txn stats", len(stmtStats), len(txnstats))
	resp := &serverpb.DrainStatsResponse{
		Statements:   make([]appstatspb.CollectedStatementStatistics, len(stmtStats)),
		Transactions: make([]appstatspb.CollectedTransactionStatistics, len(txnstats)),
	}

	for i, txnstat := range txnstats {
		resp.Transactions[i] = *txnstat
	}

	for i, stmt := range stmtStats {
		resp.Statements[i] = *stmt
	}

	return resp, nil
}

func combineAllStats(resps []*serverpb.DrainStatsResponse) *serverpb.DrainStatsResponse {
	stmtFingerprintCount := make(map[appstatspb.StmtFingerprintID]struct{})
	stmtMap := make(map[appstatspb.StatementStatisticsKey]*appstatspb.CollectedStatementStatistics)
	txnMap := make(map[appstatspb.TransactionFingerprintID]*appstatspb.CollectedTransactionStatistics)

	for _, resp := range resps {
		for _, stmt := range resp.Statements {
			if existingStmt, ok := stmtMap[stmt.Key]; !ok {
				stmtMap[stmt.Key] = &stmt
			} else {
				existingStmt.Stats.Add(&stmt.Stats)
			}

			if _, ok := stmtFingerprintCount[stmt.ID]; !ok {
				stmtFingerprintCount[stmt.ID] = struct{}{}
			}
		}
		for _, txn := range resp.Transactions {
			if existingTx, ok := txnMap[txn.TransactionFingerprintID]; !ok {
				txnMap[txn.TransactionFingerprintID] = &txn
			} else {
				existingTx.Stats.Add(&txn.Stats)
			}
		}
	}

	fingerprintCount := len(stmtFingerprintCount) + len(txnMap)
	response := serverpb.DrainStatsResponse{
		Statements:       make([]appstatspb.CollectedStatementStatistics, 0, len(stmtMap)),
		Transactions:     make([]appstatspb.CollectedTransactionStatistics, 0, len(txnMap)),
		FingerprintCount: int64(fingerprintCount),
	}
	for _, v := range stmtMap {
		response.Statements = append(response.Statements, *v)
	}

	for _, v := range txnMap {
		response.Transactions = append(response.Transactions, *v)
	}
	return &response
}
