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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// AutomaticStatisticsClusterMode controls the cluster setting for enabling
// automatic table statistics collection.
var AutomaticStatisticsClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	catpb.AutoStatsEnabledSettingName,
	"automatic statistics collection mode",
	true,
).WithPublic()

// UseStatisticsOnSystemTables controls the cluster setting for enabling
// statistics usage by the optimizer for planning queries involving system
// tables.
var UseStatisticsOnSystemTables = settings.RegisterBoolSetting(
	settings.TenantWritable,
	catpb.UseStatsOnSystemTables,
	"when true, enables use of statistics on system tables by the query optimizer",
	true,
).WithPublic()

// AutomaticStatisticsOnSystemTables controls the cluster setting for enabling
// automatic statistics collection on system tables. Auto stats must be enabled
// via a true setting of sql.stats.automatic_collection.enabled for this flag to
// have any effect.
var AutomaticStatisticsOnSystemTables = settings.RegisterBoolSetting(
	settings.TenantWritable,
	catpb.AutoStatsOnSystemTables,
	"when true, enables automatic collection of statistics on system tables",
	true,
).WithPublic()

// MultiColumnStatisticsClusterMode controls the cluster setting for enabling
// automatic collection of multi-column statistics.
var MultiColumnStatisticsClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.multi_column_collection.enabled",
	"multi-column statistics collection mode",
	true,
).WithPublic()

// AutomaticStatisticsMaxIdleTime controls the maximum fraction of time that
// the sampler processors will be idle when scanning large tables for automatic
// statistics (in high load scenarios). This value can be tuned to trade off
// the runtime vs performance impact of automatic stats.
var AutomaticStatisticsMaxIdleTime = settings.RegisterFloatSetting(
	settings.TenantWritable,
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
		settings.TenantWritable,
		catpb.AutoStatsFractionStaleSettingName,
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
		settings.TenantWritable,
		catpb.AutoStatsMinStaleSettingName,
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
//	                        # rows updated/inserted/deleted
//	p =  --------------------------------------------------------------------
//	     (# rows in table) * (target fraction of rows updated before refresh)
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
type Refresher struct {
	log.AmbientContext
	st      *cluster.Settings
	ex      isql.Executor
	cache   *TableStatisticsCache
	randGen autoStatsRand

	// mutations is the buffered channel used to pass messages containing
	// metadata about SQL mutations to the background Refresher thread.
	mutations chan mutation

	// settings is the buffered channel used to pass messages containing
	// autostats setting override information to the background Refresher thread.
	settings chan settingOverride

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

	// settingOverrides holds any autostats cluster setting overrides for each
	// table.
	settingOverrides map[descpb.ID]catpb.AutoStatsSettings

	// numTablesEnsured is an internal counter for testing ensureAllTables.
	numTablesEnsured int
}

// mutation contains metadata about a SQL mutation and is the message passed to
// the background refresher thread to (possibly) trigger a statistics refresh.
type mutation struct {
	tableID      descpb.ID
	rowsAffected int
	// removeSettingOverrides, when true, removes any pre-existing auto stats
	// cluster setting overrides for the table with the above tableID.
	// The default value of false is a no-Op.
	removeSettingOverrides bool
}

// settingOverride specifies the autostats setting override values to use in
// place of the cluster settings.
type settingOverride struct {
	tableID  descpb.ID
	settings catpb.AutoStatsSettings
}

// MakeRefresher creates a new Refresher.
func MakeRefresher(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	ex isql.Executor,
	cache *TableStatisticsCache,
	asOfTime time.Duration,
) *Refresher {
	randSource := rand.NewSource(rand.Int63())

	return &Refresher{
		AmbientContext:   ambientCtx,
		st:               st,
		ex:               ex,
		cache:            cache,
		randGen:          makeAutoStatsRand(randSource),
		mutations:        make(chan mutation, refreshChanBufferLen),
		settings:         make(chan settingOverride, refreshChanBufferLen),
		asOfTime:         asOfTime,
		extraTime:        time.Duration(rand.Int63n(int64(time.Hour))),
		mutationCounts:   make(map[descpb.ID]int64, 16),
		settingOverrides: make(map[descpb.ID]catpb.AutoStatsSettings),
	}
}

func (r *Refresher) getNumTablesEnsured() int {
	return r.numTablesEnsured
}

func (r *Refresher) autoStatsEnabled(desc catalog.TableDescriptor) bool {
	if desc == nil {
		// If the descriptor could not be accessed, defer to the cluster setting.
		return AutomaticStatisticsClusterMode.Get(&r.st.SV)
	}
	enabledForTable := desc.AutoStatsCollectionEnabled()
	// The table-level setting of sql_stats_automatic_collection_enabled takes
	// precedence over the cluster setting.
	if enabledForTable == catpb.AutoStatsCollectionNotSet {
		return AutomaticStatisticsClusterMode.Get(&r.st.SV)
	}
	return enabledForTable == catpb.AutoStatsCollectionEnabled
}

func (r *Refresher) autoStatsEnabledForTableID(
	tableID descpb.ID, settingOverrides map[descpb.ID]catpb.AutoStatsSettings,
) bool {
	var setting catpb.AutoStatsSettings
	var ok bool
	if settingOverrides == nil {
		// If the setting overrides map doesn't exist, defer to the cluster setting.
		return AutomaticStatisticsClusterMode.Get(&r.st.SV)
	}
	if setting, ok = settingOverrides[tableID]; !ok {
		// If there are no setting overrides, defer to the cluster setting.
		return AutomaticStatisticsClusterMode.Get(&r.st.SV)
	}
	autoStatsSettingValue := setting.AutoStatsCollectionEnabled()
	if autoStatsSettingValue == catpb.AutoStatsCollectionNotSet {
		return AutomaticStatisticsClusterMode.Get(&r.st.SV)
	}
	// The table-level setting of sql_stats_automatic_collection_enabled takes
	// precedence over the cluster setting.
	return autoStatsSettingValue == catpb.AutoStatsCollectionEnabled
}

func (r *Refresher) autoStatsMinStaleRows(explicitSettings *catpb.AutoStatsSettings) int64 {
	if explicitSettings == nil {
		return AutomaticStatisticsMinStaleRows.Get(&r.st.SV)
	}
	if minStaleRows, ok := explicitSettings.AutoStatsMinStaleRows(); ok {
		return minStaleRows
	}
	return AutomaticStatisticsMinStaleRows.Get(&r.st.SV)
}

func (r *Refresher) autoStatsFractionStaleRows(explicitSettings *catpb.AutoStatsSettings) float64 {
	if explicitSettings == nil {
		return AutomaticStatisticsFractionStaleRows.Get(&r.st.SV)
	}
	if fractionStaleRows, ok := explicitSettings.AutoStatsFractionStaleRows(); ok {
		return fractionStaleRows
	}
	return AutomaticStatisticsFractionStaleRows.Get(&r.st.SV)
}

func (r *Refresher) getTableDescriptor(
	ctx context.Context, tableID descpb.ID,
) (desc catalog.TableDescriptor) {
	if err := r.cache.db.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) (err error) {
		if desc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID); err != nil {
			err = errors.Wrapf(err,
				"failed to get table descriptor for automatic stats on table id: %d", tableID)
		}
		return err
	}); err != nil {
		log.Errorf(ctx, "%v", err)
	}
	return desc
}

// Start starts the stats refresher thread, which polls for messages about
// new SQL mutations and refreshes the table statistics with probability
// proportional to the percentage of rows affected.
func (r *Refresher) Start(
	ctx context.Context, stopper *stop.Stopper, refreshInterval time.Duration,
) error {
	bgCtx := r.AnnotateCtx(context.Background())
	_ = stopper.RunAsyncTask(bgCtx, "refresher", func(ctx context.Context) {
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
		var ensuringAllTables bool

		for {
			select {
			case <-initialTableCollection:
				r.ensureAllTables(ctx, &r.st.SV, initialTableCollectionDelay)
				if len(r.mutationCounts) > 0 {
					ensuringAllTables = true
				}

			case <-timer.C:
				mutationCounts := r.mutationCounts
				refreshingAllTables := ensuringAllTables
				ensuringAllTables = false

				var settingOverrides map[descpb.ID]catpb.AutoStatsSettings
				// For each mutation count, look up auto stats setting overrides using
				// the associated table ID. r.settingOverrides is never rebuilt. It is
				// always added to or deleted from. We could just copy the entire hash
				// map here, but maybe it is quicker and causes less memory pressure to
				// just create a map with entries for the tables we're processing.
				for tableID := range mutationCounts {
					if settings, ok := r.settingOverrides[tableID]; ok {
						if settingOverrides == nil {
							settingOverrides = make(map[descpb.ID]catpb.AutoStatsSettings)
						}
						settingOverrides[tableID] = settings
					}
				}

				if err := stopper.RunAsyncTask(
					ctx, "stats.Refresher: maybeRefreshStats", func(ctx context.Context) {
						// Record the start time of processing this batch of tables.
						start := timeutil.Now()

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
							var desc catalog.TableDescriptor
							now := timeutil.Now()
							elapsed := now.Sub(start)
							// If a long-running stats collection caused a delay in
							// processing the current table longer than the refresh
							// interval, look up the table descriptor to ensure we don't
							// have stale table settings.
							if elapsed > DefaultRefreshInterval || refreshingAllTables {
								desc = r.getTableDescriptor(ctx, tableID)
								if desc != nil {
									if !r.autoStatsEnabled(desc) {
										continue
									}
									if settingOverrides == nil {
										settingOverrides = make(map[descpb.ID]catpb.AutoStatsSettings)
									}
									autoStatsSettings := desc.GetAutoStatsSettings()
									if autoStatsSettings == nil {
										delete(settingOverrides, tableID)
									} else {
										settingOverrides[tableID] = *autoStatsSettings
									}
								}
							}
							if desc == nil {
								// Check the cluster setting and table setting before each
								// refresh in case they were disabled recently.
								if !r.autoStatsEnabledForTableID(tableID, settingOverrides) {
									continue
								}
							}
							var explicitSettings *catpb.AutoStatsSettings
							if settingOverrides != nil {
								if settings, ok := settingOverrides[tableID]; ok {
									explicitSettings = &settings
								}
							}
							r.maybeRefreshStats(ctx, tableID, explicitSettings, rowsAffected, r.asOfTime)

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
				// This clears out any tables that may have been added to the
				// mutationCounts map by ensureAllTables and any mutation counts that
				// have been added since the last call to maybeRefreshStats.
				// This is by design. We don't want to constantly refresh tables that
				// are read-only.
				r.mutationCounts = make(map[descpb.ID]int64, len(r.mutationCounts))

			case mut := <-r.mutations:
				r.mutationCounts[mut.tableID] += int64(mut.rowsAffected)
				// The mutations channel also handles resetting of cluster setting
				// overrides when none exist (so that we don't have to pass two messages
				// when nothing is overridden).
				if mut.removeSettingOverrides {
					delete(r.settingOverrides, mut.tableID)
				}

			case clusterSettingOverride := <-r.settings:
				r.settingOverrides[clusterSettingOverride.tableID] = clusterSettingOverride.settings

			case <-stopper.ShouldQuiesce():
				log.Info(ctx, "quiescing auto stats refresher")
				return
			}
		}
	})
	return nil
}

const (
	getAllTablesTemplateSQL = `
SELECT
	tbl.table_id
FROM
	crdb_internal.tables AS tbl
	INNER JOIN system.descriptor AS d ON d.id = tbl.table_id
		AS OF SYSTEM TIME '-%s'
WHERE
	tbl.database_name IS NOT NULL
	AND tbl.table_id NOT IN (%d, %d, %d, %d)  -- excluded system tables
	AND tbl.drop_time IS NULL
	AND (
			crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', d.descriptor, false)->'table'->>'viewQuery'
		) IS NULL
	%s`

	explicitlyEnabledTablesPredicate = `AND
	(crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor',
		d.descriptor, false)->'table'->'autoStatsSettings' ->> 'enabled' = 'true'
	)`

	autoStatsEnabledOrNotSpecifiedPredicate = `AND
	(crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor',
		d.descriptor, false)->'table'->'autoStatsSettings'->'enabled' IS NULL
	 OR crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor',
		d.descriptor, false)->'table'->'autoStatsSettings' ->> 'enabled' = 'true'
	)`
)

func (r *Refresher) getApplicableTables(
	ctx context.Context, stmt string, opname string, forTesting bool,
) {
	if forTesting {
		r.numTablesEnsured = 0
	}
	it, err := r.ex.QueryIterator(
		ctx,
		opname,
		nil, /* txn */
		stmt,
	)
	if err == nil {
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			tableID := descpb.ID(*row[0].(*tree.DInt))
			// Don't create statistics for virtual tables.
			// The query already excludes views and system tables.
			if !descpb.IsVirtualTable(tableID) {
				r.mutationCounts[tableID] += 0
				if forTesting {
					r.numTablesEnsured++
				}
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
	}
}

// ensureAllTables ensures that an entry exists in r.mutationCounts for each
// table in the database which has auto stats enabled, either explicitly via
// a table-level setting, or implicitly via the cluster setting.
func (r *Refresher) ensureAllTables(
	ctx context.Context, settings *settings.Values, initialTableCollectionDelay time.Duration,
) {
	if !AutomaticStatisticsClusterMode.Get(settings) {
		// Use a historical read so as to disable txn contention resolution.
		// A table-level setting of sql_stats_automatic_collection_enabled=true is
		// checked and only those tables are included in this scan.
		getTablesWithAutoStatsExplicitlyEnabledQuery := fmt.Sprintf(
			getAllTablesTemplateSQL,
			initialTableCollectionDelay,
			keys.TableStatisticsTableID, keys.LeaseTableID, keys.JobsTableID, keys.ScheduledJobsTableID,
			explicitlyEnabledTablesPredicate,
		)
		r.getApplicableTables(ctx, getTablesWithAutoStatsExplicitlyEnabledQuery,
			"get-tables-with-autostats-explicitly-enabled", false)
		return
	}

	// Use a historical read so as to disable txn contention resolution.
	// A table-level setting of sql_stats_automatic_collection_enabled of null,
	// meaning not set, or true qualifies rows we're interested in.
	getAllTablesQuery := fmt.Sprintf(
		getAllTablesTemplateSQL,
		initialTableCollectionDelay,
		keys.TableStatisticsTableID, keys.LeaseTableID, keys.JobsTableID, keys.ScheduledJobsTableID,
		autoStatsEnabledOrNotSpecifiedPredicate,
	)
	r.getApplicableTables(ctx, getAllTablesQuery,
		"get-tables", false)
}

// NotifyMutation is called by SQL mutation operations to signal to the
// Refresher that a table has been mutated. It should be called after any
// successful insert, update, upsert or delete. rowsAffected refers to the
// number of rows written as part of the mutation operation.
func (r *Refresher) NotifyMutation(table catalog.TableDescriptor, rowsAffected int) {
	if !r.autoStatsEnabled(table) {
		return
	}
	if !autostatsCollectionAllowed(table, r.st) {
		// Don't collect stats for virtual tables or views. System tables may be
		// allowed if enabled in cluster settings.
		return
	}

	noSettingOverrides := table.NoAutoStatsSettingsOverrides()
	var autoStatsSettings *catpb.AutoStatsSettings
	if !noSettingOverrides {
		autoStatsSettings = table.GetAutoStatsSettings()
	}

	// Send setting override information over first, so it could take effect
	// before the mutation is processed.
	if autoStatsSettings != nil {
		autoStatsOverrides := *protoutil.Clone(autoStatsSettings).(*catpb.AutoStatsSettings)
		select {
		case r.settings <- settingOverride{
			tableID:  table.GetID(),
			settings: autoStatsOverrides,
		}:
		default:
			// Don't block if there is no room in the buffered channel.
			if bufferedChanFullLogLimiter.ShouldLog() {
				log.Warningf(context.TODO(),
					"buffered channel is full. Unable to update settings for table %q (%d) during auto stats refreshing",
					table.GetName(), table.GetID())
			}
		}
	}

	// Send mutation info to the refresher thread to avoid adding latency to
	// the calling transaction.
	select {
	case r.mutations <- mutation{
		tableID:                table.GetID(),
		rowsAffected:           rowsAffected,
		removeSettingOverrides: noSettingOverrides,
	}:
	default:
		// Don't block if there is no room in the buffered channel.
		if bufferedChanFullLogLimiter.ShouldLog() {
			log.Warningf(context.TODO(),
				"buffered channel is full. Unable to refresh stats for table %q (%d) with %d rows affected",
				table.GetName(), table.GetID(), rowsAffected)
		}
	}
}

// maybeRefreshStats implements the core logic described in the comment for
// Refresher. It is called by the background Refresher thread.
// explicitSettings, if non-nil, holds any autostats cluster setting overrides
// for this table.
func (r *Refresher) maybeRefreshStats(
	ctx context.Context,
	tableID descpb.ID,
	explicitSettings *catpb.AutoStatsSettings,
	rowsAffected int64,
	asOf time.Duration,
) {
	tableStats, err := r.cache.getTableStatsFromCache(ctx, tableID, nil /* forecast */)
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
		maxTimeBetweenRefreshes := stat.CreatedAt.Add(2*avgFullRefreshTime(tableStats) + r.extraTime)
		if timeutil.Now().After(maxTimeBetweenRefreshes) {
			mustRefresh = true
		}
		rowCount = float64(stat.RowCount)
	} else {
		// If there are no statistics available on this table, we must perform a
		// refresh.
		mustRefresh = true
	}

	statsFractionStaleRows := r.autoStatsFractionStaleRows(explicitSettings)
	statsMinStaleRows := r.autoStatsMinStaleRows(explicitSettings)
	targetRows := int64(rowCount*statsFractionStaleRows) + statsMinStaleRows
	// randInt will panic if we pass it a value of 0.
	randomTargetRows := int64(0)
	if targetRows > 0 {
		randomTargetRows = r.randGen.randInt(targetRows)
	}
	if !mustRefresh && rowsAffected < math.MaxInt32 && randomTargetRows >= rowsAffected {
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

// avgFullRefreshTime returns the average time between automatic full statistics
// refreshes given a list of tableStats from one table. It does so by finding
// the most recent automatically generated statistic and then finds all
// previously generated automatic stats on those same columns. The average is
// calculated as the average time between each consecutive stat.
//
// If there are not at least two automatically generated statistics on the same
// columns, the default value defaultAverageTimeBetweenRefreshes is returned.
func avgFullRefreshTime(tableStats []*TableStatistic) time.Duration {
	var reference *TableStatistic
	var sum time.Duration
	var count int
	for _, stat := range tableStats {
		if !stat.IsAuto() || stat.IsPartial() {
			continue
		}
		if reference == nil {
			reference = stat
			continue
		}
		if !areEqual(stat.ColumnIDs, reference.ColumnIDs) {
			continue
		}
		if stat.CreatedAt.Equal(reference.CreatedAt) {
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
