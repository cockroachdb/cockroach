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
		StatementInsights: make([]*serverpb.StatementExecutionInsightsResponse_StatementInsight, 0),
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

			if events, ok := stmtIdToContentionMap[stmt.ID]; ok && stmt.Contention != nil {
				for _, event := range events {
					ci, err := s.getContentionEventDetails(&event)
					if err != nil {
						return nil, err
					}
					stmt.ContentionEvents = append(stmt.ContentionEvents, ci)
				}
			}

			resp.StatementInsights = append(resp.StatementInsights, &serverpb.StatementExecutionInsightsResponse_StatementInsight{
				TransactionID: insight.Transaction.ID,
				ID:            insight.Session.ID,
				Statement:     stmt,
			})
		}
	}
	return resp, nil
}

func (b *baseStatusServer) getContentionEventDetails(
	event *contentionpb.ExtendedContentionEvent,
) (*insights.ContentionEvent, error) {
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

	return &insights.ContentionEvent{
		Event:        event,
		DatabaseName: dbName,
		SchemaName:   schemaName,
		IndexName:    indexName,
		TableName:    tableName,
	}, nil
}
