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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// AutomaticStatisticsClusterMode controls the cluster default for when
// automatic table statistics collection is enabled.
var AutomaticStatisticsClusterMode = settings.RegisterBoolSetting(
	"sql.defaults.experimental_automatic_statistics",
	"default experimental automatic statistics mode",
	false,
)

// DefaultRefreshInterval is the frequency at which the Refresher will check if
// the stats for each table should be refreshed. It is mutable for testing.
// NB: Updates to this value after Refresher.Start has been called will not
// have any effect.
var DefaultRefreshInterval = time.Second

// DefaultAsOfTime is a duration which is used to define the AS OF time for
// automatic runs of CREATE STATISTICS. It is mutable for testing.
// NB: Updates to this value after MakeRefresher has been called will not have
// any effect.
var DefaultAsOfTime = 30 * time.Second

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

	// refreshChanBufferLen is the length of the buffered channel used by the
	// automatic statistics refresher. If the channel overflows, all SQL mutations
	// will be ignored by the refresher until it processes some existing mutations
	// in the buffer and makes space for new ones. SQL mutations will never block
	// waiting on the refresher.
	refreshChanBufferLen = 256
)

// Refresher is responsible for automatically refreshing the table statistics
// that are used by the cost-based optimizer. It is necessary to periodically
// refresh the statistics to prevent them from becoming stale as data in the
// database changes.
//
// The Refresher is designed to schedule a CREATE STATISTICS refresh job after
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
// Refresher also implements some heuristic limits designed to corral
// statistical outliers. If we haven't refreshed stats in 2x the average time
// between the last few refreshes, we automatically trigger a refresh. The
// existing statistics in the stats cache are used to calculate the average
// time between refreshes as well as to determine when the stats were last
// updated.
//
// If the decision is made to continue with the refresh, Refresher runs
// CREATE STATISTICS on the given table with the default set of column
// statistics. See comments in sql/create_stats.go for details about which
// default columns are chosen. Refresher runs CREATE STATISTICS with
// AS OF SYSTEM TIME ‘-30s’ to minimize performance impact on running
// transactions.
//
// To avoid adding latency to SQL mutation operations, the Refresher is run
// in one separate background thread per Server. SQL mutation operations signal
// to the Refresher thread by calling NotifyMutation, which sends mutation
// metadata to the Refresher thread over a non-blocking buffered channel. The
// signaling is best-effort; if the channel is full, the metadata will not be
// sent.
//
type Refresher struct {
	ex      sqlutil.InternalExecutor
	cache   *TableStatisticsCache
	randGen autoStatsRand

	// mutations is the buffered channel used to pass messages containing
	// metadata about SQL mutations to the background Refresher thread.
	mutations chan mutation

	// asOfTime is a duration which is used to define the AS OF time for
	// runs of CREATE STATISTICS by the Refresher.
	asOfTime time.Duration

	// extraTime is a small, random amount of extra time to add to the check for
	// whether too much time has passed since the last statistics refresh. It is
	// used to avoid having multiple nodes trying to create stats at the same
	// time.
	extraTime time.Duration

	// mutationCounts contains aggregated mutation counts for each table that
	// have yet to be processed by the refresher.
	mutationCounts map[sqlbase.ID]int64
}

// mutation contains metadata about a SQL mutation and is the message passed to
// the background refresher thread to (possibly) trigger a statistics refresh.
type mutation struct {
	tableID      sqlbase.ID
	rowsAffected int
}

// MakeRefresher creates a new Refresher.
func MakeRefresher(
	ex sqlutil.InternalExecutor, cache *TableStatisticsCache, asOfTime time.Duration,
) *Refresher {
	randSource := rand.NewSource(rand.Int63())

	return &Refresher{
		ex:             ex,
		cache:          cache,
		randGen:        makeAutoStatsRand(randSource),
		mutations:      make(chan mutation, refreshChanBufferLen),
		asOfTime:       asOfTime,
		extraTime:      time.Duration(rand.Int63n(int64(time.Hour))),
		mutationCounts: make(map[sqlbase.ID]int64, 16),
	}
}

// Start starts the stats refresher thread, which polls for messages about
// new SQL mutations and refreshes the table statistics with probability
// proportional to the percentage of rows affected.
func (r *Refresher) Start(
	ctx context.Context, stopper *stop.Stopper, refreshInterval time.Duration,
) error {
	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		timer := time.NewTimer(refreshInterval)

		for {
			select {
			case <-timer.C:
				mutationCounts := r.mutationCounts
				if err := stopper.RunAsyncTask(
					ctx, "stats.Refresher: maybeRefreshStats", func(ctx context.Context) {
						for tableID, rowsAffected := range mutationCounts {
							r.maybeRefreshStats(ctx, tableID, rowsAffected, r.asOfTime)
						}
						timer.Reset(refreshInterval)
					}); err != nil {
					log.Errorf(ctx, "failed to refresh stats: %v", err)
				}
				r.mutationCounts = make(map[sqlbase.ID]int64, len(r.mutationCounts))

			case mut := <-r.mutations:
				r.mutationCounts[mut.tableID] += int64(mut.rowsAffected)

			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

// NotifyMutation is called by SQL mutation operations to signal to the
// Refresher that a table has been mutated. It should be called after any
// successful insert, update, upsert or delete. rowsAffected refers to the
// number of rows written as part of the mutation operation.
func (r *Refresher) NotifyMutation(
	settings *settings.Values, tableID sqlbase.ID, rowsAffected int,
) {
	if !AutomaticStatisticsClusterMode.Get(settings) {
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

	// Send mutation info to the refresher thread to avoid adding latency to
	// the calling transaction.
	select {
	case r.mutations <- mutation{tableID: tableID, rowsAffected: rowsAffected}:
	default:
		// Don't block if there is no room in the buffered channel.
		log.Warningf(context.TODO(),
			"buffered channel is full. Unable to refresh stats for table %d with %d rows affected",
			tableID, rowsAffected)
	}
}

// maybeRefreshStats implements the core logic described in the comment for
// Refresher. It is called by the background Refresher thread.
func (r *Refresher) maybeRefreshStats(
	ctx context.Context, tableID sqlbase.ID, rowsAffected int64, asOf time.Duration,
) {
	tableStats, err := r.cache.GetTableStats(ctx, tableID)
	if err != nil {
		log.Errorf(ctx, "failed to get table statistics: %v", err)
		return
	}

	var rowCount float64
	mustRefresh := false
	if stat := mostRecentAutomaticStat(tableStats); stat != nil {
		// Check if too much time has passed since the last refresh.
		// This check is in place to corral statistical outliers and avoid a
		// case where a significant portion of the data in a table has changed but
		// the stats haven't been refreshed. Randomly add some extra time to the
		// limit check to avoid having multiple nodes trying to create stats at
		// the same time.
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
		maxTimeBetweenRefreshes := stat.CreatedAt.Add(2*avgRefreshTime(tableStats) + r.extraTime)
		if timeutil.Now().After(maxTimeBetweenRefreshes) {
			mustRefresh = true
		}
		rowCount = float64(stat.RowCount)
	} else {
		// If there are no statistics available on this table, we must perform a
		// refresh.
		mustRefresh = true
	}

	targetRows := int64(rowCount*targetFractionOfRowsUpdatedBeforeRefresh) + 1
	if !mustRefresh && r.randGen.randInt(targetRows) >= rowsAffected {
		// No refresh is happening this time.
		return
	}

	// TODO(rytaft): Add logic to create a Job ID for this refresh and use the
	// Job ID to lock automatic stats creation for this table with a lock
	// manager. If the lock succeeds, check the stats cache one more time to
	// make sure a new statistic was not just added. If not, proceed to the next
	// step.

	if err := r.refreshStats(ctx, tableID, asOf); err != nil {
		pgerr, ok := errors.Cause(err).(*pgerror.Error)
		if ok && pgerr.Code == pgerror.CodeUndefinedTableError {
			// Sleep so that the latest changes will be reflected according to the
			// AS OF time, then try again.
			time.Sleep(asOf)
			err = r.refreshStats(ctx, tableID, asOf)
		}
		if err != nil {
			log.Errorf(ctx, "failed to create statistics: %v", err)
			return
		}
	}
}

func (r *Refresher) refreshStats(
	ctx context.Context, tableID sqlbase.ID, asOf time.Duration,
) error {
	// Create statistics for all default column sets on the given table.
	_ /* rows */, err := r.ex.Exec(
		ctx,
		"create-stats",
		nil, /* txn */
		fmt.Sprintf("CREATE STATISTICS %s FROM [%d] AS OF SYSTEM TIME '-%s';",
			autoStatsName, tableID, asOf.String(),
		),
	)
	return err
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

// avgRefreshTime returns the average time between automatic statistics
// refreshes given a list of tableStats from one table. It does so by finding
// the most recent automatically generated statistic (identified by the name
// autoStatsName), and then finds all previously generated automatic stats on
// those same columns. The average is calculated as the average time between
// each consecutive stat.
//
// If there are not at least two automatically generated statistics on the same
// columns, the default value defaultAverageTimeBetweenRefreshes is returned.
func avgRefreshTime(tableStats []*TableStatistic) time.Duration {
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
