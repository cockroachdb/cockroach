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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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

func (s *statusServer) PopAllSqlStats(
	ctx context.Context, req *serverpb.PopAllSqlStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	localReq := &serverpb.PopAllSqlStatsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.popAllSqlStatsLocal(ctx, requestedNodeID)
		}
		statusCli, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return statusCli.PopAllSqlStats(ctx, localReq)
	}

	consumeStats := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
		return status.PopAllSqlStats(ctx, localReq)
	}

	var fanoutError error
	var fanoutResonses = make([]*serverpb.StatementsResponse, 0)
	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "reset SQL statistics",
		noTimeout,
		s.dialNode,
		consumeStats,
		func(nodeID roachpb.NodeID, resp interface{}) {
			fanoutResonses = append(fanoutResonses, resp.(*serverpb.StatementsResponse))
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

func (s *statusServer) popAllSqlStatsLocal(
	ctx context.Context, nodeID roachpb.NodeID,
) (*serverpb.StatementsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	statsProvider := s.sqlServer.pgServer.SQLServer.GetSQLStatsProvider()
	stmtStats, txnstats := statsProvider.PopAllStats(ctx)

	resp := &serverpb.StatementsResponse{
		Statements:            make([]serverpb.StatementsResponse_CollectedStatementStatistics, len(stmtStats)),
		LastReset:             statsProvider.GetLastReset(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
		Transactions:          make([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, len(txnstats)),
	}

	for i, txnstat := range txnstats {
		resp.Transactions[i] = serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
			StatsData: *txnstat,
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

func combineAllStats(resps []*serverpb.StatementsResponse) *serverpb.StatementsResponse {
	stmtMap := make(map[appstatspb.StatementStatisticsKey]serverpb.StatementsResponse_CollectedStatementStatistics)
	txnMap := make(map[appstatspb.TransactionFingerprintID]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics)
	response := serverpb.StatementsResponse{
		Statements:   make([]serverpb.StatementsResponse_CollectedStatementStatistics, 0),
		Transactions: make([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, 0),
	}
	for _, resp := range resps {
		for _, stmt := range resp.Statements {
			if existingStmt, ok := stmtMap[stmt.Key.KeyData]; !ok {
				stmtMap[stmt.Key.KeyData] = stmt
			} else {
				existingStmt.Stats.Add(&stmt.Stats)
			}
		}
		for _, txn := range resp.Transactions {
			if existingTx, ok := txnMap[txn.StatsData.TransactionFingerprintID]; !ok {
				txnMap[txn.StatsData.TransactionFingerprintID] = txn
			} else {
				existingTx.StatsData.Stats.Add(&txn.StatsData.Stats)
			}
		}
	}

	for k, v := range stmtMap {
		response.Statements = append(response.Statements,
			serverpb.StatementsResponse_CollectedStatementStatistics{
				Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
					KeyData: k,
				},
				ID:    v.ID,
				Stats: v.Stats,
			})
	}

	for _, v := range txnMap {
		response.Transactions = append(response.Transactions,
			serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
				StatsData: appstatspb.CollectedTransactionStatistics{
					StatementFingerprintIDs:  v.StatsData.StatementFingerprintIDs,
					TransactionFingerprintID: v.StatsData.TransactionFingerprintID,
					Stats:                    v.StatsData.Stats,
				},
			})
	}
	return &response
}
