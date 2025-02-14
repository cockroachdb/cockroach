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
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DrainSqlStatsRespBuilder is a builder for the DrainSqlStatsResponse. When
// adding StatementStatistics and TransactionStatistics,  it will merge the
// new statistic with an existing one, if it already exists in the map.
type DrainSqlStatsRespBuilder struct {
	stmtFingerprintCount map[appstatspb.StmtFingerprintID]struct{}
	stmtMap              map[appstatspb.StatementStatisticsKey]*appstatspb.CollectedStatementStatistics
	txnMap               map[appstatspb.TransactionFingerprintID]*appstatspb.CollectedTransactionStatistics
}

func NewDrainSqlStatsRespBuilder() *DrainSqlStatsRespBuilder {
	return &DrainSqlStatsRespBuilder{
		stmtFingerprintCount: make(map[appstatspb.StmtFingerprintID]struct{}),
		stmtMap:              make(map[appstatspb.StatementStatisticsKey]*appstatspb.CollectedStatementStatistics),
		txnMap:               make(map[appstatspb.TransactionFingerprintID]*appstatspb.CollectedTransactionStatistics),
	}
}

func (b *DrainSqlStatsRespBuilder) AddStmtStats(
	stmtStats []*appstatspb.CollectedStatementStatistics,
) {
	for _, stmt := range stmtStats {
		if existingStmt, ok := b.stmtMap[stmt.Key]; !ok {
			b.stmtMap[stmt.Key] = stmt
		} else {
			existingStmt.Stats.Add(&stmt.Stats)
		}

		if _, ok := b.stmtFingerprintCount[stmt.ID]; !ok {
			b.stmtFingerprintCount[stmt.ID] = struct{}{}
		}
	}
}

func (b *DrainSqlStatsRespBuilder) AddTxnStats(
	txnStats []*appstatspb.CollectedTransactionStatistics,
) {
	for _, txn := range txnStats {
		if existingTx, ok := b.txnMap[txn.TransactionFingerprintID]; !ok {
			b.txnMap[txn.TransactionFingerprintID] = txn
		} else {
			existingTx.Stats.Add(&txn.Stats)
		}
	}
}

func (b *DrainSqlStatsRespBuilder) Build() *serverpb.DrainStatsResponse {
	fingerprintCount := len(b.stmtFingerprintCount) + len(b.txnMap)
	response := &serverpb.DrainStatsResponse{
		Statements:       make([]*appstatspb.CollectedStatementStatistics, 0, len(b.stmtMap)),
		Transactions:     make([]*appstatspb.CollectedTransactionStatistics, 0, len(b.txnMap)),
		FingerprintCount: int64(fingerprintCount),
	}

	response.Statements = maps.Values(b.stmtMap)
	response.Transactions = maps.Values(b.txnMap)
	return response
}

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

// DrainSqlStats drains the sql stats from all nodes in the cluster and returns
// them. Any statement or transaction stats that exist in multiple nodes will
// be combined into a single entry in the response.
func (s *statusServer) DrainSqlStats(
	ctx context.Context, req *serverpb.DrainSqlStatsRequest,
) (*serverpb.DrainStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	_, isAdmin, err := s.privilegeChecker.GetUserAndRole(ctx)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if !isAdmin {
		return nil, status.Error(codes.PermissionDenied, "user does not have admin role")
	}

	localReq := &serverpb.DrainSqlStatsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.drainSqlStatsLocal(ctx)
		}
		statusCli, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return statusCli.DrainSqlStats(ctx, localReq)
	}

	drainStats := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.DrainStatsResponse, error) {
		return status.DrainSqlStats(ctx, localReq)
	}

	respBuilder := NewDrainSqlStatsRespBuilder()
	var fanOutError error
	if err := iterateNodesExt(ctx, s.serverIterator, s.stopper, "drain SQL statistics",
		s.dialNode,
		drainStats,
		func(nodeID roachpb.NodeID, resp *serverpb.DrainStatsResponse) {
			respBuilder.AddStmtStats(resp.Statements)
			respBuilder.AddTxnStats(resp.Transactions)
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
			if nodeFnError != nil {
				fanOutError = errors.CombineErrors(fanOutError, nodeFnError)
			}
		}, iterateNodesOpts{maxConcurrency: 4},
	); err != nil {
		return nil, err
	}

	// If there is an error in one of the fan out requests, we log it but
	// don't return it. This will allow the caller to still receive the
	// drained sql stats from the other nodes.
	if fanOutError != nil {
		log.Warningf(ctx, "error draining SQL stats from node: %s", fanOutError)
	}
	return respBuilder.Build(), nil
}

func (s *statusServer) drainSqlStatsLocal(
	ctx context.Context,
) (*serverpb.DrainStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	statsProvider := s.sqlServer.pgServer.SQLServer.GetLocalSQLStatsProvider()
	stmtStats, txnstats, fpCount := statsProvider.DrainStats(ctx)
	log.VInfof(ctx,
		1,
		"drainSqlStatsLocal: %d statement stats, %d txn stats, %d fingerprint count",
		len(stmtStats),
		len(txnstats),
		fpCount)
	resp := &serverpb.DrainStatsResponse{FingerprintCount: fpCount}
	resp.Statements = stmtStats
	resp.Transactions = txnstats
	return resp, nil
}
