// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stats

import (
	"fmt"
	"math/rand"
	"time"

	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Constants for automatic statistics collection.
// TODO(rytaft): Should these constants be configurable?
const (
	// targetFractionOfRowsUpdatedBeforeRefresh indicates the target fraction
	// of rows in a table that should be updated before statistics on that table
	// are refreshed.
	targetFractionOfRowsUpdatedBeforeRefresh = 0.05

	// minTimeBetweenRefreshes indicates the minimum amount of time between
	// automated statistics refreshes on the same table.
	minTimeBetweenRefreshes = time.Hour

	// defaultAverageTimeBetweenRefreshes is the default time to use as the
	// "average" time between refreshes when there is no information for a given
	// table.
	defaultAverageTimeBetweenRefreshes = 12 * time.Hour

	// autoStatsName is the name to use for statistics created automatically.
	// The name is chosen to be something that users are unlikely to choose when
	// running CREATE STATISTICS manually.
	autoStatsName = "__auto__"

	// autoStatsAsOfTime is a duration which is used to define the AS OF time for
	// automatic runs of CREATE STATISTICS.
	autoStatsAsOfTime = 1 * time.Minute
)

// MaybeRefreshStats is used to schedule a CREATE STATISTICS refresh job after
// approximately X% of total rows have been updated/inserted/deleted in a given
// table. Currently, X is hardcoded to be 5%.
//
// The decision to refresh is based on a percentage rather than a fixed number
// of rows because if a table is huge and rarely updated, we don't want to
// waste time frequently refreshing stats. Likewise, if it's small and rapidly
// updated, we want to update stats more often.
//
// To avoid contention on row update counters, we use a statistical approach.
// For example, suppose we want to refresh stats after 5% of rows are updated
// and there are currently 1M rows in the table. If a user updates 10 rows,
// we use random number generation to refresh stats with probability
// 10/(1M * 0.05) = 0.0002. The general formula is:
//
//                            # rows updated/inserted/deleted
//    p =  --------------------------------------------------------------------
//         (# rows in table) * (target fraction of rows updated before refresh)
//
// The existing statistics in the stats cache are used to get the number of
// rows in the table.
//
// MaybeRefreshStats also implements some heuristic limits designed to corral
// statistical outliers. If we haven't refreshed stats in 2x the average time
// between the last few refreshes, we automatically trigger a refresh. We also
// avoid updating stats too often by disallowing automatic refreshes if the
// last one happened too recently. (Currently "too recently" is defined as less
// than 1 hour ago). The existing statistics in the stats cache are used to
// calculate the average time between refreshes as well as to determine when
// the stats were last updated.
//
// If the decision is made to continue with the refresh, MaybeRefreshStats runs
// CREATE STATISTICS on the given table with the default set of column
// statistics. See comments in sql/create_stats.go for details about which
// default columns are chosen. MaybeRefreshStats runs CREATE STATISTICS with
// AS OF SYSTEM TIME ‘-1m’ to minimize performance impact on running
// transactions.
//
// MaybeRefreshStats should be called after any successful insert, update,
// upsert or delete. rowsAffected refers to the number of rows written as part
// of the mutation operation. It should be an asynchronous call in a separate
// goroutine to avoid adding latency to the calling transaction.
//
func MaybeRefreshStats(
	evalCtx *tree.EvalContext,
	cache *TableStatisticsCache,
	executor sqlutil.InternalExecutor,
	tableID sqlbase.ID,
	rowsAffected int,
) {
	asOf := autoStatsAsOfTime
	if evalCtx.TestingKnobs.AutomaticStatisticsAsOfTime != 0 {
		asOf = evalCtx.TestingKnobs.AutomaticStatisticsAsOfTime
	}
	maybeRefreshStats(evalCtx, cache, executor, tableID, rowsAffected, asOf)
}

// maybeRefreshStats implements MaybeRefreshStats and enables use of an
// alternate AS OF time for testing purposes.
func maybeRefreshStats(
	evalCtx *tree.EvalContext,
	cache *TableStatisticsCache,
	executor sqlutil.InternalExecutor,
	tableID sqlbase.ID,
	rowsAffected int,
	asOf time.Duration,
) {
	if !evalCtx.SessionData.AutomaticStatistics {
		return
	}

	// The context in evalCtx will have been canceled already.
	ctx := context.TODO()
	tableStats, err := cache.GetTableStats(ctx, tableID)
	if err != nil {
		log.Errorf(ctx, "failed to get table statistics: %v", err)
		return
	}

	var rowCount float64
	mustRefresh := false
	if stat := mostRecentAutomaticStat(tableStats); stat != nil {
		if time.Now().Before(stat.CreatedAt.Add(minTimeBetweenRefreshes)) {
			// Insufficient time has passed since the last refresh.
			return
		}
		if time.Now().After(stat.CreatedAt.Add(2 * averageRefreshTime(tableStats))) {
			// Too much time has passed since the last refresh.
			mustRefresh = true
		}
		rowCount = float64(stat.RowCount)
	} else {
		// If there are no statistics available on this table, we must perform a
		// refresh.
		mustRefresh = true
	}

	probability := 1.0
	if rowCount > 0 {
		probability = float64(rowsAffected) / (rowCount * targetFractionOfRowsUpdatedBeforeRefresh)
	}
	if !mustRefresh && rand.Float64() > probability {
		// No refresh is happening this time.
		return
	}

	// TODO(rytaft): Add logic to create a Job ID for this refresh and use the
	// Job ID to lock automatic stats creation for this table with a lock
	// manager. If the lock succeeds, check the stats cache one more time to
	// make sure a new statistic was not just added. If not, proceed to the next
	// step.

	// Sleep so that the latest changes will be reflected according to the
	// AS OF time.
	time.Sleep(asOf)

	// Create statistics for all default column sets on the given table.
	if _ /* rows */, err := executor.Exec(
		ctx,
		"create-stats",
		nil, /* txn */
		fmt.Sprintf("CREATE STATISTICS %s FROM [%d] AS OF SYSTEM TIME '-%s';",
			autoStatsName, tableID, asOf.String(),
		),
	); err != nil {
		log.Errorf(ctx, "failed to create statistics: %v", err)
		return
	}
}

// mostRecentAutomaticStat finds the most recent automatic statistic
// (identified by the name autoStatsName).
func mostRecentAutomaticStat(tableStats []*TableStatistic) *TableStatistic {
	// Stats are sorted with the most recent first.
	for _, stat := range tableStats {
		if stat.Name == autoStatsName {
			return stat
		}
	}
	return nil
}

// averageRefreshTime returns the average time between automatic statistics
// refreshes given a list of tableStats from one table. It does so by finding
// the most recent automatically generated statistic (identified by the name
// autoStatsName), and then finds all previously generated automatic stats on
// those same columns. The average is calculated as the average time between
// each consecutive stat.
//
// If there are not at least two automatically generated statistics on the same
// columns, the default value defaultAverageTimeBetweenRefreshes is returned.
func averageRefreshTime(tableStats []*TableStatistic) time.Duration {
	var reference *TableStatistic
	var sum time.Duration
	var count int
	for _, stat := range tableStats {
		if stat.Name != autoStatsName {
			continue
		}
		if reference == nil {
			reference = stat
			continue
		}
		if !areEqual(stat.ColumnIDs, reference.ColumnIDs) {
			continue
		}
		// Stats are sorted with the most recent first.
		sum += reference.CreatedAt.Sub(stat.CreatedAt)
		count++
		reference = stat
	}
	if count == 0 {
		return defaultAverageTimeBetweenRefreshes
	}
	return sum / time.Duration(count)
}

func areEqual(a, b []sqlbase.ColumnID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
