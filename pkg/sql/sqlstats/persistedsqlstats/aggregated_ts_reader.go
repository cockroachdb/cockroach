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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

func (s *PersistedSQLStats) ScanEarliestAggregatedTs(
	ctx context.Context, ex sqlutil.InternalExecutor, tableName string,
) (time.Time, error) {
	var hashColumnName string
	switch tableName {
	case "system.statement_statistics":
		hashColumnName = systemschema.StmtStatsHashColumnName
	case "system.transaction_statistics":
		hashColumnName = systemschema.TxnStatsHashColumnName
	default:
		return time.Time{}, errors.Newf(`Unexpected table name %s`, tableName)
	}

	earliestAggregatedTsPerShard := make([]time.Time, systemschema.SQLStatsHashShardBucketCount)
	for shardIdx := int64(0); shardIdx < systemschema.SQLStatsHashShardBucketCount; shardIdx++ {
		stmt := s.getStatementForEarliestAggregatedTs(tableName, hashColumnName)
		row, err := ex.QueryRowEx(ctx, "scan-earliest-aggregated-ts", nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			stmt,
			shardIdx,
		)
		if err != nil {
			return time.Time{}, err
		}
		if row == nil {
			earliestAggregatedTsPerShard[shardIdx] = time.Time{}
		} else {
			shardEarliestAggregatedTs := tree.MustBeDTimestampTZ(row[0]).Time
			earliestAggregatedTsPerShard[shardIdx] = shardEarliestAggregatedTs
		}

	}
	var earliestAggregatedTs time.Time
	for _, shardEarliestAggregatedTs := range earliestAggregatedTsPerShard {
		if !shardEarliestAggregatedTs.IsZero() &&
			(earliestAggregatedTs.IsZero() || shardEarliestAggregatedTs.Before(earliestAggregatedTs)) {
			earliestAggregatedTs = shardEarliestAggregatedTs
		}
	}

	if earliestAggregatedTs.IsZero() {
		// if we are flushing every 15 seconds, the table unlikely to be empty
		// but in this unlikely event, return what the next aggregatedTs would be
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
