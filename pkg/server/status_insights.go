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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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
	// Validate that either both start and end time are defined or not.
	if (req.StartTime != nil && req.EndTime == nil) || (req.EndTime != nil && req.StartTime == nil) {
		return nil, errors.New("required that both StartTime and EndTime to be set")
	}

	resp := &serverpb.StatementExecutionInsightsResponse{
		Statements: make([]*serverpb.StatementExecutionInsightsResponse_Statement, 0),
	}

	// Request in-memory statement insights in case persisted data is incomplete.
	inMemoryInsights, err := s.ListExecutionInsights(ctx, &serverpb.ListExecutionInsightsRequest{})
	if err != nil {
		return nil, err
	}
	contentionEventsResp, err := s.TransactionContentionEvents(ctx, &serverpb.TransactionContentionEventsRequest{})
	if err != nil {
		return nil, err
	}
	stmtIdToContentionMap := make(map[clusterunique.ID][]contentionpb.ExtendedContentionEvent)
	for _, event := range contentionEventsResp.Events {
		// users with VIEWACTIVITYREDACTED role have no access to range keys so
		// it will be empty.
		if event.BlockingEvent.Key.Equal(roachpb.Key{}) {
			continue
		}
		stmtIdToContentionMap[event.WaitingStmtID] = append(stmtIdToContentionMap[event.WaitingStmtID], event)
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

			var contentionEvents []*serverpb.ContentionEvent
			if events, ok := stmtIdToContentionMap[stmt.ID]; ok {
				for _, event := range events {
					ci, err := s.getContentionEventDetails(ctx, &event)
					if err != nil {
						return nil, err
					}
					contentionEvents = append(contentionEvents, ci)
				}
			}
			statement := convertToStmtInsight(
				stmt,
				insight.Session.ID,
				insight.Transaction.ID,
				insight.Transaction.FingerprintID,
				insight.Transaction.ImplicitTxn,
				insight.Transaction.User,
				insight.Transaction.UserPriority,
				insight.Transaction.ApplicationName,
				contentionEvents,
			)
			resp.Statements = append(resp.Statements, statement)
		}
	}
	return resp, nil
}

func convertToStmtInsight(
	stmt *insights.Statement,
	sessionID clusterunique.ID,
	transactionID uuid.UUID,
	txnFingerprintID appstatspb.TransactionFingerprintID,
	implicitTxn bool,
	user string,
	userPriority string,
	applicationName string,
	contentionEvents []*serverpb.ContentionEvent,
) *serverpb.StatementExecutionInsightsResponse_Statement {
	return &serverpb.StatementExecutionInsightsResponse_Statement{
		ID:                   &stmt.ID,
		FingerprintID:        stmt.FingerprintID,
		TransactionID:        &transactionID,
		TxnFingerprintID:     txnFingerprintID,
		SessionID:            &sessionID,
		Query:                stmt.Query,
		Status:               stmt.Status,
		StartTime:            &stmt.StartTime,
		EndTime:              &stmt.EndTime,
		FullScan:             stmt.FullScan,
		ImplicitTxn:          implicitTxn,
		User:                 user,
		UserPriority:         userPriority,
		ApplicationName:      applicationName,
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
		ServiceLatSeconds:    stmt.LatencyInSeconds,
		ContentionEvents:     contentionEvents,
	}
}

func (b *baseStatusServer) getContentionEventDetails(
	ctx context.Context, event *contentionpb.ExtendedContentionEvent,
) (*serverpb.ContentionEvent, error) {
	if err := b.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	hasViewRedacted := false
	if err := b.privilegeChecker.RequireViewActivityRedactedPermission(ctx); err == nil {
		hasViewRedacted = true
	}
	var key roachpb.Key
	var prettyKey string
	if hasViewRedacted {
		key = event.BlockingEvent.Key
		prettyKey = keys.PrettyPrint(nil /* valDirs */, event.BlockingEvent.Key)
	}
	var tableName, indexName, dbName, schemaName string
	err := b.sqlServer.internalDB.Txn(b.rpcCtx.MasterCtx, func(ctx context.Context, txn isql.Txn) error {
		_, tableID, err := b.sqlServer.execCfg.Codec.DecodeTablePrefix(event.BlockingEvent.Key)
		if err != nil {
			return err
		}
		_, _, indexID, err := b.sqlServer.execCfg.Codec.DecodeIndexPrefix(event.BlockingEvent.Key)
		if err != nil {
			return err
		}
		desc := descs.FromTxn(txn)
		var tableDesc catalog.TableDescriptor
		tableDesc, err = desc.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
		if err != nil {
			return nil //nolint:returnerrcheck
		}
		tableName = tableDesc.GetName()

		idxDesc, err := catalog.MustFindIndexByID(tableDesc, descpb.IndexID(indexID))
		if err != nil {
			indexName = fmt.Sprintf("[dropped index id: %d]", indexID)
		}
		if idxDesc != nil {
			indexName = idxDesc.GetName()
		}

		dbDesc, err := desc.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, tableDesc.GetParentID())
		if err != nil {
			dbName = "[dropped database]"
		}
		if dbDesc != nil {
			dbName = dbDesc.GetName()
		}

		schemaDesc, err := desc.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, tableDesc.GetParentSchemaID())
		if err != nil {
			schemaName = "[dropped schema]"
		}
		if schemaDesc != nil {
			schemaName = schemaDesc.GetName()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &serverpb.ContentionEvent{
		Key:                      key,
		PrettyKey:                prettyKey,
		BlockingTxnID:            event.BlockingEvent.TxnMeta.ID,
		BlockingTxnFingerprintID: event.BlockingTxnFingerprintID,
		Duration:                 &event.BlockingEvent.Duration,
		WaitingTxnID:             &event.WaitingTxnID,
		WaitingTxnFingerprintID:  event.WaitingTxnFingerprintID,
		CollectionTs:             &event.CollectionTs,
		WaitingStmtFingerprintID: event.WaitingStmtFingerprintID,
		WaitingStmtID:            &event.WaitingStmtID,
		DatabaseName:             dbName,
		SchemaName:               schemaName,
		IndexName:                indexName,
		TableName:                tableName,
	}, nil
}

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
		Transactions: make([]*serverpb.TransactionExecutionInsightsResponse_Transaction, 0),
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
		// Filter by TransactionID.
		if req.TransactionID != nil && insight.Transaction.ID != *req.TransactionID {
			continue
		}
		// Filter by TxnFingerprintID.
		if req.TxnFingerprintID > 0 && insight.Transaction.FingerprintID != req.TxnFingerprintID {
			continue
		}
		// Filter by start and end time.
		if req.StartTime != nil && req.EndTime != nil &&
			!timeutil.IsOverlappingTimeRanges(*req.StartTime, *req.EndTime, insight.Transaction.StartTime, insight.Transaction.EndTime) {
			continue
		}
		transaction := &serverpb.TransactionExecutionInsightsResponse_Transaction{
			ID:               &insight.Transaction.ID,
			FingerprintID:    insight.Transaction.FingerprintID,
			SessionID:        &insight.Session.ID,
			UserPriority:     insight.Transaction.UserPriority,
			ImplicitTxn:      insight.Transaction.ImplicitTxn,
			Contention:       insight.Transaction.Contention,
			StartTime:        &insight.Transaction.StartTime,
			EndTime:          &insight.Transaction.EndTime,
			User:             insight.Transaction.User,
			ApplicationName:  insight.Transaction.ApplicationName,
			RowsRead:         insight.Transaction.RowsRead,
			RowsWritten:      insight.Transaction.RowsWritten,
			RetryCount:       insight.Transaction.RetryCount,
			AutoRetryReason:  insight.Transaction.AutoRetryReason,
			Problems:         insight.Transaction.Problems,
			Causes:           insight.Transaction.Causes,
			StmtExecutionIDs: insight.Transaction.StmtExecutionIDs,
			CPUSQLNanos:      insight.Transaction.CPUSQLNanos,
			LastErrorCode:    insight.Transaction.LastErrorCode,
			LastErrorMsg:     insight.Transaction.LastErrorMsg,
			Status:           insight.Transaction.Status,
		}
		resp.Transactions = append(resp.Transactions, transaction)
		if ce, ok := txnIDToContentionMap[insight.Transaction.ID]; ok && req.WithContentionEvents {
			for _, event := range ce {
				ci, err := s.getContentionEventDetails(ctx, &event)
				if err != nil {
					return nil, err
				}
				transaction.ContentionEvents = append(transaction.ContentionEvents, ci)
			}
		}
		if !req.WithStatementInsights {
			continue
		}
		for _, stmt := range insight.Statements {
			var contentionEvents []*serverpb.ContentionEvent
			if events, ok := stmtIDToContentionMap[stmt.ID]; ok && req.WithContentionEvents {
				for _, event := range events {
					ci, err := s.getContentionEventDetails(ctx, &event)
					if err != nil {
						return nil, err
					}
					contentionEvents = append(contentionEvents, ci)
				}
			}
			si := convertToStmtInsight(
				stmt,
				insight.Session.ID,
				insight.Transaction.ID,
				insight.Transaction.FingerprintID,
				insight.Transaction.ImplicitTxn,
				insight.Transaction.User,
				insight.Transaction.UserPriority,
				insight.Transaction.ApplicationName,
				contentionEvents,
			)
			transaction.Statements = append(transaction.Statements, si)
		}
	}
	return resp, nil
}
