// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// AutoStatsClusterSettingName is the name of the automatic stats collection
// cluster setting.
const AutoStatsClusterSettingName = "sql.stats.automatic_collection.enabled"

// AutomaticStatisticsClusterMode controls the cluster setting for enabling
// automatic table statistics collection.
var AutomaticStatisticsClusterMode = settings.RegisterBoolSetting(
	AutoStatsClusterSettingName,
	"automatic statistics collection mode",
	true,
).WithPublic()

// MultiColumnStatisticsClusterMode controls the cluster setting for enabling
// automatic collection of multi-column statistics.
var MultiColumnStatisticsClusterMode = settings.RegisterBoolSetting(
	"sql.stats.multi_column_collection.enabled",
	"multi-column statistics collection mode",
	true,
).WithPublic()

// AutomaticStatisticsMaxIdleTime controls the maximum fraction of time that
// the sampler processors will be idle when scanning large tables for automatic
// statistics (in high load scenarios). This value can be tuned to trade off
// the runtime vs performance impact of automatic stats.
var AutomaticStatisticsMaxIdleTime = settings.RegisterFloatSetting(
	"sql.stats.automatic_collection.max_fraction_idle",
	"maximum fraction of time that automatic statistics sampler processors are idle",
	0.9,
	func(val float64) error {
		if val < 0 || val >= 1 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"sql.stats.automatic_collection.max_fraction_idle must be >= 0 and < 1 but found: %v", val)
		}
		return nil
	},
)

// AutomaticStatisticsFractionStaleRows controls the cluster setting for
// the target fraction of rows in a table that should be stale before
// statistics on that table are refreshed, in addition to the constant value
// AutomaticStatisticsMinStaleRows.
var AutomaticStatisticsFractionStaleRows = func() *settings.FloatSetting {
	s := settings.RegisterFloatSetting(
		"sql.stats.automatic_collection.fraction_stale_rows",
		"target fraction of stale rows per table that will trigger a statistics refresh",
		0.2,
		settings.NonNegativeFloat,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// AutomaticStatisticsMinStaleRows controls the cluster setting for the target
// number of rows that should be updated before a table is refreshed, in
// addition to the fraction AutomaticStatisticsFractionStaleRows.
var AutomaticStatisticsMinStaleRows = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"sql.stats.automatic_collection.min_stale_rows",
		"target minimum number of stale rows per table that will trigger a statistics refresh",
		500,
		settings.NonNegativeInt,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// DefaultRefreshInterval is the frequency at which the Refresher will check if
// the stats for each table should be refreshed. It is mutable for testing.
// NB: Updates to this value after Refresher.Start has been called will not
// have any effect.
var DefaultRefreshInterval = time.Minute

// DefaultAsOfTime is a duration which is used to define the AS OF time for
// automatic runs of CREATE STATISTICS. It is mutable for testing.
// NB: Updates to this value after MakeRefresher has been called will not have
// any effect.
var DefaultAsOfTime = 30 * time.Second

// bufferedChanFullLogLimiter is used to minimize spamming the log with
// "buffered channel is full" errors.
var bufferedChanFullLogLimiter = log.Every(time.Second)

// Constants for automatic statistics collection.
// TODO(rytaft): Should these constants be configurable?
const (
	// defaultAverageTimeBetweenRefreshes is the default time to use as the
	// "average" time between refreshes when there is no information for a given
	// table.
	defaultAverageTimeBetweenRefreshes = 12 * time.Hour

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
// table. Currently, X is hardcoded to be 20%.
//
// The decision to refresh is based on a percentage rather than a fixed number
// of rows because if a table is huge and rarely updated, we don't want to
// waste time frequently refreshing stats. Likewise, if it's small and rapidly
// updated, we want to update stats more often.
//
// To avoid contention on row update counters, we use a statistical approach.
// For example, suppose we want to refresh stats after 20% of rows are updated
// and there are currently 1M rows in the table. If a user updates 10 rows,
// we use random number generation to refresh stats with probability
// 10/(1M * 0.2) = 0.00005. The general formula is:
//
//                            # rows updated/inserted/deleted
//    p =  --------------------------------------------------------------------
//         (# rows in table) * (target fraction of rows updated before refresh)
//
// The existing statistics in the stats cache are used to get the number of
// rows in the table.
//
// In order to prevent small tables from being constantly refreshed, we also
// require that approximately 500 rows have changed in addition to the 20%.
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
	st      *cluster.Settings
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
	mutationCounts map[descpb.ID]int64
}

// mutation contains metadata about a SQL mutation and is the message passed to
// the background refresher thread to (possibly) trigger a statistics refresh.
type mutation struct {
	tableID      descpb.ID
	rowsAffected int
}

// MakeRefresher creates a new Refresher.
func MakeRefresher(
	st *cluster.Settings,
	ex sqlutil.InternalExecutor,
	cache *TableStatisticsCache,
	asOfTime time.Duration,
) *Refresher {
	randSource := rand.NewSource(rand.Int63())

	return &Refresher{
		st:             st,
		ex:             ex,
		cache:          cache,
		randGen:        makeAutoStatsRand(randSource),
		mutations:      make(chan mutation, refreshChanBufferLen),
		asOfTime:       asOfTime,
		extraTime:      time.Duration(rand.Int63n(int64(time.Hour))),
		mutationCounts: make(map[descpb.ID]int64, 16),
	}
}

// Start starts the stats refresher thread, which polls for messages about
// new SQL mutations and refreshes the table statistics with probability
// proportional to the percentage of rows affected.
func (r *Refresher) Start(
	ctx context.Context, stopper *stop.Stopper, refreshInterval time.Duration,
) error {
	_ = stopper.RunAsyncTask(context.Background(), "refresher", func(ctx context.Context) {
		// We always sleep for r.asOfTime at the beginning of each refresh, so
		// subtract it from the refreshInterval.
		refreshInterval -= r.asOfTime
		if refreshInterval < 0 {
			refreshInterval = 0
		}

		timer := time.NewTimer(refreshInterval)
		defer timer.Stop()

		// Ensure that read-only tables will have stats created at least
		// once on startup.
		const initialTableCollectionDelay = time.Second
		initialTableCollection := time.After(initialTableCollectionDelay)

		for {
			select {
			case <-initialTableCollection:
				r.ensureAllTables(ctx, &r.st.SV, initialTableCollectionDelay)

			case <-timer.C:
				mutationCounts := r.mutationCounts
				if err := stopper.RunAsyncTask(
					ctx, "stats.Refresher: maybeRefreshStats", func(ctx context.Context) {
						// Wait so that the latest changes will be reflected according to the
						// AS OF time.
						timerAsOf := time.NewTimer(r.asOfTime)
						defer timerAsOf.Stop()
						select {
						case <-timerAsOf.C:
							break
						case <-stopper.ShouldQuiesce():
							return
						}

						for tableID, rowsAffected := range mutationCounts {
							// Check the cluster setting before each refresh in case it was
							// disabled recently.
							if !AutomaticStatisticsClusterMode.Get(&r.st.SV) {
								break
							}

							r.maybeRefreshStats(ctx, stopper, tableID, rowsAffected, r.asOfTime)

							select {
							case <-stopper.ShouldQuiesce():
								// Don't bother trying to refresh the remaining tables if we
								// are shutting down.
								return
							default:
							}
						}
						timer.Reset(refreshInterval)
					}); err != nil {
					log.Errorf(ctx, "failed to refresh stats: %v", err)
				}
				r.mutationCounts = make(map[descpb.ID]int64, len(r.mutationCounts))

			case mut := <-r.mutations:
				r.mutationCounts[mut.tableID] += int64(mut.rowsAffected)

			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
	return nil
}

// ensureAllTables ensures that an entry exists in r.mutationCounts for each
// table in the database.
func (r *Refresher) ensureAllTables(
	ctx context.Context, settings *settings.Values, initialTableCollectionDelay time.Duration,
) {
	if !AutomaticStatisticsClusterMode.Get(settings) {
		// Automatic stats are disabled.
		return
	}

	// Use a historical read so as to disable txn contention resolution.
	getAllTablesQuery := fmt.Sprintf(
		`
SELECT table_id FROM crdb_internal.tables AS OF SYSTEM TIME '-%s'
WHERE database_name IS NOT NULL
AND drop_time IS NULL
`,
		initialTableCollectionDelay)

	it, err := r.ex.QueryIterator(
		ctx,
		"get-tables",
		nil, /* txn */
		getAllTablesQuery,
	)
	if err == nil {
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			tableID := descpb.ID(*row[0].(*tree.DInt))
			// Don't create statistics for system tables or virtual tables.
			// TODO(rytaft): Don't add views here either. Unfortunately views are not
			// identified differently from tables in crdb_internal.tables.
			if !descpb.IsReservedID(tableID) && !descpb.IsVirtualTable(tableID) {
				r.mutationCounts[tableID] += 0
			}
		}
	}
	if err != nil {
		// Note that it is ok if the iterator returned partial results before
		// encountering an error - in that case we added entries to
		// r.mutationCounts for some of the tables and operation of adding an
		// entry is idempotent (i.e. we didn't mess up anything for the next
		// call to this method).
		log.Errorf(ctx, "failed to get tables for automatic stats: %v", err)
		return
	}
}

// NotifyMutation is called by SQL mutation operations to signal to the
// Refresher that a table has been mutated. It should be called after any
// successful insert, update, upsert or delete. rowsAffected refers to the
// number of rows written as part of the mutation operation.
func (r *Refresher) NotifyMutation(tableID descpb.ID, rowsAffected int) {
	if !AutomaticStatisticsClusterMode.Get(&r.st.SV) {
		// Automatic stats are disabled.
		return
	}

	if descpb.IsReservedID(tableID) {
		// Don't try to create statistics for system tables (most importantly,
		// for table_statistics itself).
		return
	}
	if descpb.IsVirtualTable(tableID) {
		// Don't try to create statistics for virtual tables.
		return
	}

	// Send mutation info to the refresher thread to avoid adding latency to
	// the calling transaction.
	select {
	case r.mutations <- mutation{tableID: tableID, rowsAffected: rowsAffected}:
	default:
		// Don't block if there is no room in the buffered channel.
		if bufferedChanFullLogLimiter.ShouldLog() {
			log.Warningf(context.TODO(),
				"buffered channel is full. Unable to refresh stats for table %d with %d rows affected",
				tableID, rowsAffected)
		}
	}
}

// maybeRefreshStats implements the core logic described in the comment for
// Refresher. It is called by the background Refresher thread.
func (r *Refresher) maybeRefreshStats(
	ctx context.Context,
	stopper *stop.Stopper,
	tableID descpb.ID,
	rowsAffected int64,
	asOf time.Duration,
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

	targetRows := int64(rowCount*AutomaticStatisticsFractionStaleRows.Get(&r.st.SV)) +
		AutomaticStatisticsMinStaleRows.Get(&r.st.SV)
	if !mustRefresh && rowsAffected < math.MaxInt32 && r.randGen.randInt(targetRows) >= rowsAffected {
		// No refresh is happening this time.
		return
	}

	if err := r.refreshStats(ctx, tableID, asOf); err != nil {
		if errors.Is(err, ConcurrentCreateStatsError) {
			// Another stats job was already running. Attempt to reschedule this
			// refresh.
			if mustRefresh {
				// For the cases where mustRefresh=true (stats don't yet exist or it
				// has been 2x the average time since a refresh), we want to make sure
				// that maybeRefreshStats is called on this table during the next
				// cycle so that we have another chance to trigger a refresh. We pass
				// rowsAffected=0 so that we don't force a refresh if another node has
				// already done it.
				r.mutations <- mutation{tableID: tableID, rowsAffected: 0}
			} else {
				// If this refresh was caused by a "dice roll", we want to make sure
				// that the refresh is rescheduled so that we adhere to the
				// AutomaticStatisticsFractionStaleRows statistical ideal. We
				// ensure that the refresh is triggered during the next cycle by
				// passing a very large number for rowsAffected.
				r.mutations <- mutation{tableID: tableID, rowsAffected: math.MaxInt32}
			}
			return
		}

		// Log other errors but don't automatically reschedule the refresh, since
		// that could lead to endless retries.
		log.Warningf(ctx, "failed to create statistics on table %d: %v", tableID, err)
		return
	}
}

func (r *Refresher) refreshStats(ctx context.Context, tableID descpb.ID, asOf time.Duration) error {
	// Create statistics for all default column sets on the given table.
	_ /* rows */, err := r.ex.Exec(
		ctx,
		"create-stats",
		nil, /* txn */
		fmt.Sprintf(
			"CREATE STATISTICS %s FROM [%d] WITH OPTIONS THROTTLING %g AS OF SYSTEM TIME '-%s'",
			jobspb.AutoStatsName,
			tableID,
			AutomaticStatisticsMaxIdleTime.Get(&r.st.SV),
			asOf.String(),
		),
	)
	return err
}

// mostRecentAutomaticStat finds the most recent automatic statistic
// (identified by the name AutoStatsName).
func mostRecentAutomaticStat(tableStats []*TableStatistic) *TableStatistic {
	// Stats are sorted with the most recent first.
	for _, stat := range tableStats {
		if stat.Name == jobspb.AutoStatsName {
			return stat
		}
	}
	return nil
}

// avgRefreshTime returns the average time between automatic statistics
// refreshes given a list of tableStats from one table. It does so by finding
// the most recent automatically generated statistic (identified by the name
// AutoStatsName), and then finds all previously generated automatic stats on
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
		if stat.Name != jobspb.AutoStatsName {
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

func areEqual(a, b []descpb.ColumnID) bool {
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

type concurrentCreateStatisticsError struct{}

var _ error = concurrentCreateStatisticsError{}

func (concurrentCreateStatisticsError) Error() string {
	return "another CREATE STATISTICS job is already running"
}

// ConcurrentCreateStatsError is reported when two CREATE STATISTICS jobs
// are issued concurrently. This is a sentinel error.
var ConcurrentCreateStatsError error = concurrentCreateStatisticsError{}
