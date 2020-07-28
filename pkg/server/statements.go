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
	"fmt"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) Statements(
	ctx context.Context, req *serverpb.StatementsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.StatementsResponse{
		Statements:            []serverpb.StatementsResponse_CollectedStatementStatistics{},
		LastReset:             timeutil.Now(),
		InternalAppNamePrefix: sqlbase.InternalAppNamePrefix,
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

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeStatement := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.Statements(ctx, localReq)
	}

	if err := s.iterateNodes(ctx, fmt.Sprintf("statement statistics for node %s", req.NodeID),
		dialFn,
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

	// Assign each statement an ID based on a hash of the query fingerprint.
	for i := range response.Statements {
		h := fnv.New128()
		h.Write([]byte(response.Statements[i].Key.KeyData.Query))
		response.Statements[i].Key.KeyData.Id = fmt.Sprintf("%x", h.Sum(nil))
	}

	// TODO(solon): For now we stub out the transaction field by randomly grouping
	// statements into transactions. This is meant to unblock frontend development
	// while we work on populating this with the actual transaction data.
	maxTransactionSize := len(response.Statements) / 2
	rng, _ := randutil.NewPseudoRand()
	for i := 0; i < len(response.Statements); {
		txnStats := roachpb.TransactionStatistics{}
		numStatements := randutil.RandIntInRange(rng, 1, maxTransactionSize)
		for _, stmt := range response.Statements[i : i+numStatements] {
			txnStats.StatementIds = append(txnStats.StatementIds, stmt.Key.KeyData.Id)
			txnStats.Count++
			if stmt.Stats.MaxRetries > txnStats.MaxRetries {
				txnStats.MaxRetries = stmt.Stats.MaxRetries
			}
			txnStats.NumRows.Mean += stmt.Stats.NumRows.Mean
			txnStats.ServiceLat.Mean += stmt.Stats.ServiceLat.Mean
		}
		response.Transactions = append(response.Transactions, txnStats)
		i += numStatements
	}

	return response, nil
}

func (s *statusServer) StatementsLocal(ctx context.Context) (*serverpb.StatementsResponse, error) {
	stmtStats := s.admin.server.sqlServer.pgServer.SQLServer.GetUnscrubbedStmtStats()
	lastReset := s.admin.server.sqlServer.pgServer.SQLServer.GetStmtStatsLastReset()

	resp := &serverpb.StatementsResponse{
		Statements:            make([]serverpb.StatementsResponse_CollectedStatementStatistics, len(stmtStats)),
		LastReset:             lastReset,
		InternalAppNamePrefix: sqlbase.InternalAppNamePrefix,
	}

	for i, stmt := range stmtStats {
		resp.Statements[i] = serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: stmt.Key,
				NodeID:  s.gossip.NodeID.Get(),
			},
			Stats: stmt.Stats,
		}
	}

	return resp, nil
}
