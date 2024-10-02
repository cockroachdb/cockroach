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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// IterateStatementStats implements sqlstats.Provider interface.
func (s *PersistedSQLStats) IterateStatementStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
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
	memIter := newMemStmtStatsIterator(s.SQLStats, options, curAggTs, aggInterval)

	var persistedIter isql.Rows
	var colCnt int
	persistedIter, colCnt, err = s.persistedStmtStatsIter(ctx, options)
	if err != nil {
		return err
	}
	defer func() {
		closeError := persistedIter.Close()
		if closeError != nil {
			err = errors.CombineErrors(err, closeError)
		}
	}()

	combinedIter := NewCombinedStmtStatsIterator(memIter, persistedIter, colCnt)

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

func (s *PersistedSQLStats) persistedStmtStatsIter(
	ctx context.Context, options sqlstats.IteratorOptions,
) (iter isql.Rows, expectedColCnt int, err error) {
	query, expectedColCnt := s.getFetchQueryForStmtStatsTable(ctx, options)

	persistedIter, err := s.cfg.DB.Executor().QueryIteratorEx(
		ctx,
		"read-stmt-stats",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		query,
	)

	if err != nil {
		return nil /* iter */, 0 /* expectedColCnt */, err
	}

	return persistedIter, expectedColCnt, err
}

func (s *PersistedSQLStats) getFetchQueryForStmtStatsTable(
	ctx context.Context, options sqlstats.IteratorOptions,
) (query string, colCnt int) {
	selectedColumns := []string{
		"aggregated_ts",
		"fingerprint_id",
		"transaction_fingerprint_id",
		"plan_hash",
		"app_name",
		"metadata",
		"statistics",
		"plan",
		"agg_interval",
		"index_recommendations",
	}

	// [1]: selection columns
	// [2]: AOST clause
	query = `
SELECT 
  %[1]s
FROM
	system.statement_statistics
%[2]s`

	followerReadClause := s.cfg.Knobs.GetAOSTClause()

	query = fmt.Sprintf(query, strings.Join(selectedColumns, ","), followerReadClause)

	orderByColumns := []string{"aggregated_ts"}
	if options.SortedAppNames {
		orderByColumns = append(orderByColumns, "app_name")
	}

	// TODO(azhng): what we should really be sorting here is fingerprint_id
	//  column. This is so that we are backward compatible with the way
	//  we are ordering the in-memory stats.
	if options.SortedKey {
		orderByColumns = append(orderByColumns, "metadata ->> 'query'")
	}

	orderByColumns = append(orderByColumns, "transaction_fingerprint_id")
	query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(orderByColumns, ","))

	return query, len(selectedColumns)
}

func rowToStmtStats(row tree.Datums) (*appstatspb.CollectedStatementStatistics, error) {
	var stats appstatspb.CollectedStatementStatistics
	stats.AggregatedTs = tree.MustBeDTimestampTZ(row[0]).Time

	stmtFingerprintID, err := sqlstatsutil.DatumToUint64(row[1])
	if err != nil {
		return nil, err
	}
	stats.ID = appstatspb.StmtFingerprintID(stmtFingerprintID)

	transactionFingerprintID, err := sqlstatsutil.DatumToUint64(row[2])
	if err != nil {
		return nil, err
	}
	stats.Key.TransactionFingerprintID =
		appstatspb.TransactionFingerprintID(transactionFingerprintID)

	stats.Key.PlanHash, err = sqlstatsutil.DatumToUint64(row[3])
	if err != nil {
		return nil, err
	}

	stats.Key.App = string(tree.MustBeDString(row[4]))

	metadata := tree.MustBeDJSON(row[5]).JSON
	if err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadata, &stats); err != nil {
		return nil, err
	}

	statistics := tree.MustBeDJSON(row[6]).JSON
	if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statistics, &stats.Stats); err != nil {
		return nil, err
	}

	jsonPlan := tree.MustBeDJSON(row[7]).JSON
	plan, err := sqlstatsutil.JSONToExplainTreePlanNode(jsonPlan)
	if err != nil {
		return nil, err
	}
	stats.Stats.SensitiveInfo.MostRecentPlanDescription = *plan

	aggInterval := tree.MustBeDInterval(row[8]).Duration
	stats.AggregationInterval = time.Duration(aggInterval.Nanos())

	recommendations := tree.MustBeDArray(row[9])
	var indexRecommendations []string
	for _, s := range recommendations.Array {
		indexRecommendations = util.CombineUnique(indexRecommendations, []string{string(tree.MustBeDString(s))})
	}
	stats.Stats.IndexRecommendations = indexRecommendations

	return &stats, nil
}
