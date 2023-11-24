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
	if err != nil {
		return err
	}
	defer db.Close()

	insertStmt := `INSERT INTO obsservice.statement_execution_insights VALUES 
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
     $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)`

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
		tree.DNull, // details
	); err != nil {
		return err
	}

	return nil
}

var _ EventProcessor[*obspb.StatementInsightsStatistics] = (*StmtInsightsProcessor)(nil)
