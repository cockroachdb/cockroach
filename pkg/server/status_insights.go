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
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// StmtInsightsDetails represents the structure of details column
// in system.statement_execution_insights table.
// Reminder for future maintainers: keep this struct backward compatible.
// Do not remove/rename already existing fields.
type StmtInsightsDetails struct {
	RowsRead    int64 `json:"rows_read"`
	RowsWritten int64 `json:"rows_written"`
}

func (d *StmtInsightsDetails) ToJSON() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// StatementExecutionInsights requests statement insights that satisfy specified
// parameters in request payload if any provided.
func (s *statusServer) StatementExecutionInsights(
	ctx context.Context, req *serverpb.StatementExecutionInsightsRequest,
) (*serverpb.StatementExecutionInsightsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	resp := &serverpb.StatementExecutionInsightsResponse{
		Statements: make([]*serverpb.StatementExecutionInsightsResponse_Statement, 0),
	}

	// TODO (koorosh): async requests for ListExecutionInsights and TransactionContentionEvents
	// Request in-memory statement insights in case persisted data is incomplete.
	inMemoryInsights, err := s.ListExecutionInsights(ctx, &serverpb.ListExecutionInsightsRequest{})
	if err != nil {
		return nil, err
	}
	contentionEventsResp, err := s.TransactionContentionEventsWithDetails(ctx, &serverpb.TransactionContentionEventsWithDetailsRequest{})
	if err != nil {
		return nil, err
	}
	stmtIdToContentionMap := make(map[clusterunique.ID][]*serverpb.ContentionEvent)
	for _, event := range contentionEventsResp.Events {
		stmtIdToContentionMap[*event.WaitingStmtID] = append(stmtIdToContentionMap[*event.WaitingStmtID], event)
	}
	for _, insight := range inMemoryInsights.Insights {
		for _, stmt := range insight.Statements {
			if req.StatementID != nil && stmt.ID != *req.StatementID {
				continue
			}
			if req.StmtFingerprintID != appstatspb.StmtFingerprintID(0) && stmt.FingerprintID != req.StmtFingerprintID {
				continue
			}
			if req.StartTime != nil && req.EndTime != nil &&
				!timeutil.IsOverlappingTimeRanges(*req.StartTime, *req.EndTime, stmt.StartTime, stmt.EndTime) {
				continue
			}
			// Case when only req.EndTime is Nil which means to get all insights up to now.
			if req.StartTime != nil && req.StartTime.After(stmt.EndTime) {
				continue
			}
			// Case when only req.StartTime is Nil.
			if req.EndTime != nil && req.EndTime.Before(stmt.StartTime) {
				continue
			}
			contentionEvents := stmtIdToContentionMap[stmt.ID]
			var lastErrMsg string
			if redactErr := s.privilegeChecker.RequireViewActivityRedactedPermission(ctx); redactErr == nil {
				lastErrMsg = string(stmt.ErrorMsg.Redact())
			} else {
				lastErrMsg = string(stmt.ErrorMsg)
			}
			resp.Statements = append(resp.Statements, &serverpb.StatementExecutionInsightsResponse_Statement{
				ID:                   &stmt.ID,
				FingerprintID:        stmt.FingerprintID,
				TransactionID:        &insight.Transaction.ID,
				TxnFingerprintID:     insight.Transaction.FingerprintID,
				SessionID:            &insight.Session.ID,
				Query:                stmt.Query,
				Status:               stmt.Status,
				StartTime:            &stmt.StartTime,
				EndTime:              &stmt.EndTime,
				FullScan:             stmt.FullScan,
				ImplicitTxn:          insight.Transaction.ImplicitTxn,
				User:                 insight.Transaction.User,
				UserPriority:         insight.Transaction.UserPriority,
				ApplicationName:      insight.Transaction.ApplicationName,
				Database:             stmt.Database,
				PlanGist:             stmt.PlanGist,
				RowsRead:             stmt.RowsRead,
				RowsWritten:          stmt.RowsWritten,
				Retries:              stmt.Retries,
				AutoRetryReason:      stmt.AutoRetryReason,
				Nodes:                stmt.Nodes,
				Contention:           stmt.Contention,
				IndexRecommendations: stmt.IndexRecommendations,
				Problem:              stmt.Problem,
				Causes:               stmt.Causes,
				CPUSQLNanos:          stmt.CPUSQLNanos,
				ErrorCode:            stmt.ErrorCode,
				LastErrorMsg:         lastErrMsg,
				ServiceLatSeconds:    stmt.LatencyInSeconds,
				ContentionEvents:     contentionEvents,
			})
		}
	}
	return resp, nil
}
