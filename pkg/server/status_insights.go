// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TransactionExecutionInsights requests transaction insights that satisfy specified
// parameters in request payload if any provided.
func (s *statusServer) TransactionExecutionInsights(
	ctx context.Context, req *serverpb.TransactionExecutionInsightsRequest,
) (*serverpb.TransactionExecutionInsightsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}
	// Validate that either both start and end time are defined or not.
	if (req.StartTime != nil && req.EndTime == nil) || (req.EndTime != nil && req.StartTime == nil) {
		return nil, errors.New("required that both StartTime and EndTime to be set")
	}

	resp := &serverpb.TransactionExecutionInsightsResponse{
		TransactionInsights: make([]*serverpb.TransactionExecutionInsightsResponse_TransactionInsight, 0),
	}

	// Request in-memory statement insights in case persisted data is incomplete.
	inMemoryInsights, err := s.ListExecutionInsights(ctx, &serverpb.ListExecutionInsightsRequest{})
	if err != nil {
		return nil, err
	}
	stmtIDToContentionMap := make(map[clusterunique.ID][]contentionpb.ExtendedContentionEvent)
	txnIDToContentionMap := make(map[uuid.UUID][]contentionpb.ExtendedContentionEvent)
	if req.WithContentionEvents {
		contentionEventsResp, err := s.TransactionContentionEvents(ctx, &serverpb.TransactionContentionEventsRequest{})
		if err != nil {
			return nil, err
		}
		for _, event := range contentionEventsResp.Events {
			// users with VIEWACTIVITYREDACTED role have no access to range keys so
			// it will be empty.
			if event.BlockingEvent.Key.Equal(roachpb.Key{}) {
				continue
			}
			stmtIDToContentionMap[event.WaitingStmtID] = append(stmtIDToContentionMap[event.WaitingStmtID], event)
			txnIDToContentionMap[event.WaitingTxnID] = append(txnIDToContentionMap[event.WaitingTxnID], event)
		}
	}

	for _, insight := range inMemoryInsights.Insights {
		if req.TransactionID != nil && insight.Transaction.ID != *req.TransactionID {
			continue
		}
		if req.StartTime != nil && req.EndTime != nil &&
			!timeutil.IsOverlappingTimeRanges(*req.StartTime, *req.EndTime, insight.Transaction.StartTime, insight.Transaction.EndTime) {
			continue
		}
		if ce, ok := txnIDToContentionMap[insight.Transaction.ID]; ok && insight.Transaction.Contention != nil {
			for _, event := range ce {
				ci, err := s.getContentionEventDetails(&event)
				if err != nil {
					return nil, err
				}
				insight.Transaction.ContentionEvents = append(insight.Transaction.ContentionEvents, ci)
			}
		}
		if !req.WithStatementInsights {
			continue
		}
		for _, stmt := range insight.Statements {
			if events, ok := stmtIDToContentionMap[stmt.ID]; ok && stmt.Contention != nil {
				for _, event := range events {
					ci, err := s.getContentionEventDetails(&event)
					if err != nil {
						return nil, err
					}
					stmt.ContentionEvents = append(stmt.ContentionEvents, ci)
				}
			}
			insight.Statements = append(insight.Statements, stmt)
		}
		resp.TransactionInsights = append(resp.TransactionInsights, &serverpb.TransactionExecutionInsightsResponse_TransactionInsight{
			SessionID:   insight.Session.ID,
			Transaction: insight.Transaction,
		})
	}
	return resp, nil
}
