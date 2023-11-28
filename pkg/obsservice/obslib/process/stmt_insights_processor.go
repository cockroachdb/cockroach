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
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type StmtInsightsProcessor struct {
	SinkPGURL string

	mu struct {
		syncutil.RWMutex

		lastExportTs time.Time
		insights     *cache.UnorderedCache
	}
}

const InsightsBatchMax = 100

func NewStmtInsightsProcessor(sinkPGURL string) *StmtInsightsProcessor {
	p := StmtInsightsProcessor{SinkPGURL: sinkPGURL}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.insights = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, _, _ interface{}) bool {
			return size > InsightsBatchMax
		},
	})
	p.mu.lastExportTs = timeutil.Now()

	return &p
}

func (p *StmtInsightsProcessor) Process(
	ctx context.Context, stmtInsight *obspb.StatementInsightsStatistics,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.mu.insights.Add(string(stmtInsight.ID), stmtInsight)
	if p.mu.insights.Len() >= InsightsBatchMax || timeutil.Since(p.mu.lastExportTs) > time.Minute {
		err := p.exportInsights(ctx)
		if err != nil {
			return err
		}
		p.mu.lastExportTs = timeutil.Now()
		p.mu.insights.Clear()
	}

	return nil
}

func (p *StmtInsightsProcessor) exportInsights(ctx context.Context) error {
	db, err := OpenDBSync(ctx, p.SinkPGURL)
	// TODO(maryliag): create a pool for the connections
	if err != nil {
		return err
	}
	defer db.Close()

	rows := ""
	placeholder := 1
	comma := ""
	var args []interface{}

	p.mu.insights.Do(func(entry *cache.Entry) {
		stmtInsight := entry.Value.(*obspb.StatementInsightsStatistics)
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
		)
		rows = rows + fmt.Sprintf(`%s ($%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, 
			$%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v)`, comma,
			placeholder, placeholder+1, placeholder+2, placeholder+3, placeholder+4, placeholder+5, placeholder+6,
			placeholder+7, placeholder+8, placeholder+9, placeholder+10, placeholder+11, placeholder+12, placeholder+13,
			placeholder+14, placeholder+15, placeholder+16, placeholder+17, placeholder+18, placeholder+19, placeholder+20,
			placeholder+21, placeholder+22, placeholder+23, placeholder+24, placeholder+25, placeholder+26, placeholder+27,
			placeholder+28, placeholder+29, placeholder+30)
		comma = ","
		placeholder = placeholder + 31
	})

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
									 retries,
									 last_retry_reason,
									 execution_node_ids,
									 index_recommendations,
									 implicit_txn,
									 cpu_sql_nanos,
									 error_code,
									 contention_time,
									 details
									 ) VALUES %s`, rows)

	if _, err = db.ExecContext(ctx, insertStmt, args...); err != nil {
		return err
	}

	return nil
}

var _ EventProcessor[*obspb.StatementInsightsStatistics] = (*StmtInsightsProcessor)(nil)
