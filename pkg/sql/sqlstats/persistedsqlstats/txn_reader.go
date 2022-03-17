// Copyright 2021 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// IterateTransactionStats implements sqlstats.Provider interface.
func (s *PersistedSQLStats) IterateTransactionStats(
	ctx context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) (err error) {
	// We override the sorting options since otherwise we would need to implement
	// sorted and unsorted merge separately. We can revisit this decision if
	// there's a good reason that we want the performance optimization from
	// unsorted merge.
	options.SortedKey = true
	options.SortedAppNames = true

	// We compute the current aggregated_ts so that the in-memory stats can be
	// merged with the persisted stats.
	curAggTs := s.ComputeAggregatedTs()
	aggInterval := s.GetAggregationInterval()
	memIter := newMemTxnStatsIterator(s.SQLStats, options, curAggTs, aggInterval)

	var persistedIter sqlutil.InternalRows
	persistedIter, err = s.persistedTxnStatsIter(ctx, options)
	if err != nil {
		return err
	}
	combinedIter := NewCombinedTxnStatsIterator(memIter, &txnStatsWrapper{rows: persistedIter})
	defer func() {
		err = errors.CombineErrors(err, combinedIter.Close())
	}()

	for {
		var ok bool
		ok, err = combinedIter.Next(ctx)
		if err != nil {
			return err
		}

		if !ok {
			break
		}

		stats := combinedIter.Cur()
		if err = visitor(ctx, stats); err != nil {
			return err
		}
	}

	return nil
}

func (s *PersistedSQLStats) persistedTxnStatsIter(
	ctx context.Context, options *sqlstats.IteratorOptions,
) (sqlutil.InternalRows, error) {

	persistedIter, err := s.cfg.InternalExecutor.QueryIteratorEx(
		ctx,
		"read-txn-stats",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		s.getFetchQueryForTxnStatsTable(options),
	)

	if err != nil {
		return nil, err
	}

	return persistedIter, err
}

func (s *PersistedSQLStats) getFetchQueryForTxnStatsTable(
	options *sqlstats.IteratorOptions,
) (query string) {
	selectedColumns := []string{
		"aggregated_ts",
		"fingerprint_id",
		"app_name",
		"metadata",
		"statistics",
		"agg_interval",
	}

	// [1]: selection columns
	// [2]: AOST clause
	query = `
SELECT 
  %[1]s
FROM
	system.transaction_statistics
%[2]s`

	followerReadClause := s.cfg.Knobs.GetAOSTClause()

	query = fmt.Sprintf(query, strings.Join(selectedColumns, ","), followerReadClause)

	orderByColumns := []string{"aggregated_ts"}
	if options.SortedAppNames {
		orderByColumns = append(orderByColumns, "app_name")
	}

	if options.SortedKey {
		orderByColumns = append(orderByColumns, "fingerprint_id")
	}

	query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(orderByColumns, ","))

	return query
}

func rowToTxnStats(row tree.Datums) (*roachpb.CollectedTransactionStatistics, error) {
	var stats roachpb.CollectedTransactionStatistics
	var err error

	stats.AggregatedTs = tree.MustBeDTimestampTZ(row[0]).Time

	value, err := sqlstatsutil.DatumToUint64(row[1])
	if err != nil {
		return nil, err
	}
	stats.TransactionFingerprintID = roachpb.TransactionFingerprintID(value)

	stats.App = string(tree.MustBeDString(row[2]))

	metadata := tree.MustBeDJSON(row[3]).JSON
	if err = sqlstatsutil.DecodeTxnStatsMetadataJSON(metadata, &stats); err != nil {
		return nil, err
	}

	statistics := tree.MustBeDJSON(row[4]).JSON
	if err = sqlstatsutil.DecodeTxnStatsStatisticsJSON(statistics, &stats.Stats); err != nil {
		return nil, err
	}

	aggInterval := tree.MustBeDInterval(row[5]).Duration
	stats.AggregationInterval = time.Duration(aggInterval.Nanos())

	return &stats, nil
}
