// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssremote

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type SQLStats struct {
	execConfig *sql.ExecutorConfig
}

func New(execCfg *sql.ExecutorConfig) *SQLStats {
	return &SQLStats{execConfig: execCfg}
}

func (ss *SQLStats) PopAllStats(
	ctx context.Context,
) ([]*appstatspb.CollectedStatementStatistics, []*appstatspb.CollectedTransactionStatistics) {
	resp, err := ss.execConfig.SQLStatusServer.PopAllSqlStats(ctx, &serverpb.PopAllSqlStatsRequest{})
	if err != nil {
		log.Warning(ctx, "Something went wrong")
		return nil, nil
	}
	stmtStats := make([]*appstatspb.CollectedStatementStatistics, 0, len(resp.Statements))
	txnStats := make([]*appstatspb.CollectedTransactionStatistics, 0, len(resp.Transactions))
	for _, stmt := range resp.Statements {
		stmtStats = append(stmtStats, &appstatspb.CollectedStatementStatistics{
			ID:    stmt.ID,
			Key:   stmt.Key.KeyData,
			Stats: stmt.Stats,
		})
	}

	for _, txnstmt := range resp.Transactions {
		txnStats = append(txnStats, &appstatspb.CollectedTransactionStatistics{
			StatementFingerprintIDs:  txnstmt.StatsData.StatementFingerprintIDs,
			App:                      "",
			Stats:                    txnstmt.StatsData.Stats,
			TransactionFingerprintID: txnstmt.StatsData.TransactionFingerprintID,
		})
	}
	return stmtStats, txnStats
}

var _ sqlstats.SSProvider = &SQLStats{}
