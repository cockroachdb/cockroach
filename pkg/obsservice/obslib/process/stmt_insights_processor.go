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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type StmtInsightsProcessor struct {
	SinkPGURL string

	mu struct {
		syncutil.RWMutex

		lastExportTs time.Time
		insights     []*obspb.StatementInsightsStatistics
	}
}

const InsightsBatchMax = 100

func NewStmtInsightsProcessor(sinkPGURL string) *StmtInsightsProcessor {
	p := StmtInsightsProcessor{SinkPGURL: sinkPGURL}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.lastExportTs = timeutil.Now()
	return &p
}

func (p *StmtInsightsProcessor) Process(
	ctx context.Context, stmtInsight *obspb.StatementInsightsStatistics,
) error {
	p.addInsight(stmtInsight)
	insightsSize, lastExportTs := p.getInsightsInfo()

	if insightsSize >= InsightsBatchMax || lastExportTs > time.Second*30 {
		insertStmt, args := p.prepareInsightExport()
		err := p.exportInsights(ctx, insertStmt, args)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *StmtInsightsProcessor) addInsight(stmtInsight *obspb.StatementInsightsStatistics) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.insights = append(p.mu.insights, stmtInsight)
}

func (p *StmtInsightsProcessor) getInsightsInfo() (int, time.Duration) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	insightsSize := len(p.mu.insights)
	lastExportTs := timeutil.Since(p.mu.lastExportTs)

	return insightsSize, lastExportTs
}

// prepareInsightExport creates the insert and args for the INSERT and clears the insights mutex.
func (p *StmtInsightsProcessor) prepareInsightExport() (string, []interface{}) {
	columnCount := 34
	var rows []string
	var args []interface{}

	p.mu.Lock()
	defer p.mu.Unlock()
	for insightIdx, stmtInsight := range p.mu.insights {
		var causes []string
		for _, cause := range stmtInsight.Causes {
			causes = append(causes, cause.String())
		}

		args = append(args,
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
			stmtInsight.Problem.String(),
			causes,
			stmtInsight.Query,
			stmtInsight.Status.String(),
			stmtInsight.StartTime,
			stmtInsight.EndTime,
			stmtInsight.FullScan,
			stmtInsight.User,
			stmtInsight.ApplicationName,
			stmtInsight.UserPriority,
			stmtInsight.Database,
			stmtInsight.PlanGist,
			stmtInsight.RowsRead,
			stmtInsight.RowsWritten,
			stmtInsight.Retries,
			stmtInsight.AutoRetryReason,
			stmtInsight.Nodes,
			stmtInsight.IndexRecommendations,
			stmtInsight.ImplicitTxn,
			stmtInsight.CPUSQLNanos,
			stmtInsight.ErrorCode,
			stmtInsight.LastErrorRedactable,
			stmtInsight.Contention,
			// Details column. This column is null for now, but exists in case there is extra information
			// we might want to add in the future, this would be the place to easily add without the need for a creation
			// of a new column.
			tree.DNull,
		)
		var placeholders []any
		for i := 1; i <= columnCount; i++ {
			placeholders = append(placeholders, columnCount*insightIdx+i)
		}

		rows = append(rows, fmt.Sprintf(`($%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, 
			$%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v)`,
			placeholders...))
	}

	insertStmt := fmt.Sprintf(`INSERT INTO obsservice.statement_execution_insights (
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
									 rows_read,
									 rows_written,
									 retries,
									 last_retry_reason,
									 execution_node_ids,
									 index_recommendations,
									 implicit_txn,
									 cpu_sql_nanos,
									 error_code,
									 last_error_redactable,
									 contention_time,
									 details
									 ) VALUES %s ON CONFLICT DO NOTHING`, strings.Join(rows, ", "))

	p.mu.lastExportTs = timeutil.Now()
	p.mu.insights = nil

	return insertStmt, args
}

func (p *StmtInsightsProcessor) exportInsights(
	ctx context.Context, insertStmt string, args []interface{},
) error {
	db, err := OpenDBSync(ctx, p.SinkPGURL)
	// TODO(maryliag): create a pool for the connections
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err = db.ExecContext(ctx, insertStmt, args...); err != nil {
		return err
	}

	return nil
}

var _ EventProcessor[*obspb.StatementInsightsStatistics] = (*StmtInsightsProcessor)(nil)
