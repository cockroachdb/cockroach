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
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// AutomaticStatisticsClusterMode controls the cluster default for when
// automatic table statistics collection is enabled.
var AutomaticStatisticsClusterMode = settings.RegisterBoolSetting(
	"sql.defaults.experimental_automatic_statistics",
	"default experimental automatic statistics mode",
	false,
)

// AutoStatsCtx stores any context used by automatic statistics code.
type AutoStatsCtx struct {
	randGen autoStatsRand
}

// MakeAutoStatsCtx creates a new AutoStatsCtx.
func MakeAutoStatsCtx() AutoStatsCtx {
	randSource := rand.NewSource(rand.Int63())
	return AutoStatsCtx{
		randGen: makeAutoStatsRand(randSource),
	}
}

// Constants for automatic statistics collection.
// TODO(rytaft): Should these constants be configurable?
const (
	// targetFractionOfRowsUpdatedBeforeRefresh indicates the target fraction
	// of rows in a table that should be updated before statistics on that table
	// are refreshed.
	targetFractionOfRowsUpdatedBeforeRefresh = 0.05

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
	autoStatsAsOfTime = 30 * time.Second
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
// of the mutation operation.
//
func MaybeRefreshStats(
	evalCtx *tree.EvalContext,
	autoStatsCtx *AutoStatsCtx,
	cache *TableStatisticsCache,
	executor sqlutil.InternalExecutor,
	tableID sqlbase.ID,
	rowsAffected int,
) {
	if !AutomaticStatisticsClusterMode.Get(&evalCtx.Settings.SV) {
		// Automatic stats are disabled.
		return
	}

	if sqlbase.IsReservedID(tableID) {
		// Don't try to create statistics for system tables (most importantly,
		// for table_statistics itself).
		return
	}
	if tableID == keys.VirtualDescriptorID {
		// Don't try to create statistics for virtual tables.
		return
	}

	asOf := autoStatsAsOfTime
	if evalCtx.TestingKnobs.AutomaticStatisticsAsOfTime != 0 {
		asOf = evalCtx.TestingKnobs.AutomaticStatisticsAsOfTime
	}

	// Refresh stats asynchronously to avoid adding latency to the calling
	// transaction.
	go maybeRefreshStats(evalCtx, autoStatsCtx, cache, executor, tableID, rowsAffected, asOf)
}

// maybeRefreshStats implements MaybeRefreshStats and enables use of an
// alternate AS OF time for testing purposes.
func maybeRefreshStats(
	_ *tree.EvalContext,
	autoStatsCtx *AutoStatsCtx,
	cache *TableStatisticsCache,
	executor sqlutil.InternalExecutor,
	tableID sqlbase.ID,
	rowsAffected int,
	asOf time.Duration,
) {
	// The context in evalCtx will have been canceled already.
	ctx := context.TODO()
	tableStats, err := cache.GetTableStats(ctx, tableID)
	if err != nil {
		log.Errorf(ctx, "failed to get table statistics: %v", err)
		return
	}

	r := autoStatsCtx.randGen
	var rowCount float64
	mustRefresh := false
	if stat := mostRecentAutomaticStat(tableStats); stat != nil {
		// Randomly add some extra time to the limit check to avoid having
		// multiple threads trying to create stats at the same time.
		extraTime := time.Duration(r.randInt(int64(time.Hour)))
		maxTimeBetweenRefreshes := stat.CreatedAt.Add(2*averageRefreshTime(tableStats) + extraTime)
		if timeutil.Now().After(maxTimeBetweenRefreshes) {
			// Too much time has passed since the last refresh.
			//
			// Note that this can cause some unnecessary runs of CREATE STATISTICS
			// in the case where there is a heavy write load followed by a very light
			// load. For example, suppose the average refresh time is 1 hour during
			// the period of heavy writes, and the average refresh time should be 1
			// week during the period of light load. It could take ~16 refreshes over
			// 3-4 weeks before the average settles at around 1 week. (Assuming the
			// refresh happens at exactly 2x the current average, and the average
			// refresh time is calculated from the most recent 4 refreshes. See the
			// comment in stats/delete_stats.go.)
			mustRefresh = true
		}

		rowCount = float64(stat.RowCount)
	} else {
		// If there are no statistics available on this table, we must perform a
		// refresh.
		mustRefresh = true
	}

	targetRows := int64(rowCount*targetFractionOfRowsUpdatedBeforeRefresh) + 1
	if !mustRefresh && r.randInt(targetRows) >= int64(rowsAffected) {
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

// autoStatsRand pairs a rand.Rand with a mutex.
type autoStatsRand struct {
	*syncutil.Mutex
	*rand.Rand
}

func makeAutoStatsRand(source rand.Source) autoStatsRand {
	return autoStatsRand{
		Mutex: &syncutil.Mutex{},
		Rand:  rand.New(source),
	}
}

func (r autoStatsRand) randInt(n int64) int64 {
	r.Lock()
	defer r.Unlock()
	return r.Int63n(n)
}
