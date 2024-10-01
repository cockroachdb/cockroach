// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/errors"
)

// IterateTransactionStats implements sqlstats.Provider interface.
func (s *PersistedSQLStats) IterateTransactionStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
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

	var persistedIter isql.Rows
	var colCnt int
	persistedIter, colCnt, err = s.persistedTxnStatsIter(ctx, options)
	if err != nil {
		return err
	}
	defer func() {
		closeError := persistedIter.Close()
		if closeError != nil {
			err = errors.CombineErrors(err, closeError)
		}
	}()

	combinedIter := NewCombinedTxnStatsIterator(memIter, persistedIter, colCnt)

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
	ctx context.Context, options sqlstats.IteratorOptions,
) (iter isql.Rows, expectedColCnt int, err error) {
	query, expectedColCnt := s.getFetchQueryForTxnStatsTable(options)
	exec := s.cfg.DB.Executor()
	if iter, err = exec.QueryIteratorEx(
		ctx,
		"read-txn-stats",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		query,
	); err != nil {
		return nil /* iter */, 0 /* expectedColCnt */, err
	}

	return iter, expectedColCnt, err
}

func (s *PersistedSQLStats) getFetchQueryForTxnStatsTable(
	options sqlstats.IteratorOptions,
) (query string, colCnt int) {
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

	return query, len(selectedColumns)
}

func rowToTxnStats(row tree.Datums) (*appstatspb.CollectedTransactionStatistics, error) {
	var stats appstatspb.CollectedTransactionStatistics
	var err error

	stats.AggregatedTs = tree.MustBeDTimestampTZ(row[0]).Time

	value, err := sqlstatsutil.DatumToUint64(row[1])
	if err != nil {
		return nil, err
	}
	stats.TransactionFingerprintID = appstatspb.TransactionFingerprintID(value)

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
