// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package process

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type StmtInsightsProcessor struct {
	SinkPGURL string
}

func (t *StmtInsightsProcessor) Process(
	ctx context.Context, stmtInsight *obspb.StatementInsightsStatistics,
) error {
	db, err := OpenDBSync(ctx, t.SinkPGURL)
	// TODO(maryliag): create a pool for the connections
	if err != nil {
		return err
	}
	defer db.Close()

	insertStmt := `INSERT INTO obsservice.statement_execution_insights (
									 timestamp,
									 org_id,
									 cluster_id,
									 tenant_id,
									 event_id,
									 session_id,
									 transaction_id,
									 transaction_fingerprint_id,
									 statement_id,
									 statement_fingerprint_id,
									 problem,
									 causes,
									 query,
									 status,
									 start_time,
									 end_time,
									 full_scan,
									 user_name,
									 app_name,
									 user_priority,
									 database_name,
									 plan_gist,
									 retries,
									 last_retry_reason,
									 execution_node_ids,
									 index_recommendations,
									 implicit_txn,
									 cpu_sql_nanos,
									 error_code,
									 contention_time,
									 details
									 ) VALUES (
											 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
											 $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, 
											 $25, $26, $27, $28, $29, $30, $31
								   )`

	if _, err := db.ExecContext(
		ctx,
		insertStmt,
		stmtInsight.EventInfo.Timestamp,
		stmtInsight.EventInfo.OrgID,
		stmtInsight.EventInfo.ClusterID,
		stmtInsight.EventInfo.TenantID,
		stmtInsight.EventInfo.EventID,
		stmtInsight.SessionID,
		stmtInsight.TransactionID,
		fmt.Sprint(stmtInsight.TxnFingerprintID),
		stmtInsight.ID,
		fmt.Sprint(stmtInsight.FingerprintID),
		stmtInsight.Problem,
		stmtInsight.Causes,
		stmtInsight.Query,
		stmtInsight.Status,
		stmtInsight.StartTime,
		stmtInsight.EndTime,
		stmtInsight.FullScan,
		stmtInsight.User,
		stmtInsight.ApplicationName,
		stmtInsight.UserPriority,
		stmtInsight.Database,
		stmtInsight.PlanGist,
		stmtInsight.Retries,
		stmtInsight.AutoRetryReason,
		stmtInsight.Nodes,
		stmtInsight.IndexRecommendations,
		stmtInsight.ImplicitTxn,
		stmtInsight.CPUSQLNanos,
		stmtInsight.ErrorCode,
		stmtInsight.Contention,
		// Details column. This column is null for now, but exists in case there is extra information
		// we might want to add in the future, this would be the place to easily add without the need for a creation
		// of a new column.
		tree.DNull,
	); err != nil {
		return err
	}

	return nil
}

var _ EventProcessor[*obspb.StatementInsightsStatistics] = (*StmtInsightsProcessor)(nil)
