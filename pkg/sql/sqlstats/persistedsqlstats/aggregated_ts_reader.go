// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

func (s *PersistedSQLStats) GetEarliestStatementAggregatedTs(
	ctx context.Context,
) (time.Time, error) {
	return s.getEarliestAggregatedTs(ctx, "system.statement_statistics", systemschema.StmtStatsHashColumnName)
}

func (s *PersistedSQLStats) GetEarliestTransactionAggregatedTs(
	ctx context.Context,
) (time.Time, error) {
	return s.getEarliestAggregatedTs(ctx, "system.transaction_statistics", systemschema.TxnStatsHashColumnName)
}

func (s *PersistedSQLStats) getEarliestAggregatedTs(
	ctx context.Context, tableName, hashColumnName string,
) (time.Time, error) {
	var earliestAggregatedTs time.Time

	for shardIdx := int64(0); shardIdx < systemschema.SQLStatsHashShardBucketCount; shardIdx++ {
		stmt := s.getStatementForEarliestAggregatedTs(tableName, hashColumnName)
		row, err := s.cfg.InternalExecutor.QueryRowEx(ctx, "get-earliest-aggregated-ts", nil,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			stmt,
			shardIdx,
		)
		if err != nil {
			return time.Time{}, err
		}
		if row != nil {
			aggregatedTs := tree.MustBeDTimestampTZ(row[0]).Time
			if !aggregatedTs.IsZero() &&
				(earliestAggregatedTs.IsZero() || aggregatedTs.Before(earliestAggregatedTs)) {
				earliestAggregatedTs = aggregatedTs
			}
		}
	}

	if earliestAggregatedTs.IsZero() {
		// When the table is empty, return what the next aggregatedTs would be.
		earliestAggregatedTs = s.computeAggregatedTs()
	}

	return earliestAggregatedTs, nil
}

func (s *PersistedSQLStats) getStatementForEarliestAggregatedTs(
	tableName, hashColumnName string,
) string {
	// [1]: table name
	// [2]: AOST clause
	// [3]: hash column name
	const stmt = `
		SELECT aggregated_ts
		FROM %[1]s %[2]s
		WHERE %[3]s = $1
		ORDER BY aggregated_ts
		LIMIT 1;
	`
	followerReadClause := "AS OF SYSTEM TIME follower_read_timestamp()"
	if s.cfg.Knobs != nil {
		followerReadClause = s.cfg.Knobs.AOSTClause
	}

	return fmt.Sprintf(stmt,
		tableName,
		followerReadClause,
		hashColumnName,
	)
}
