// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Enabled enables the stats activity flush job.
var enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"sql.stats.activity.flush.enabled",
	"enable the flush to the system statement and transaction activity tables", true)

// FlushInterval defines the interval to flush the stats
// to the activity tables.
var flushInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"sql.stats.activity.flush.interval",
	"the frequency at which the statistics are flushed to activity tables",
	15*time.Minute,
	settings.NonNegativeDurationWithMaximum(24*time.Hour),
)

// SQLStatsActivityAggregationInterval is the cluster setting that controls the aggregation
// interval for stats when we flush to disk.
var sqlStatsActivityAggregationInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.activity.aggregation.interval",
	"the interval at which we aggregate SQL activity statistics upon flush, "+
		"this value must be greater than or equal to sql.stats.activity.flush.interval",
	time.Hour,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
)

// SQLStatsActivityTopLimit is the cluster setting that controls the number of
// rows selected to be inserted into the activity tables
var sqlStatsActivityTopLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.stats.activity.top.limit",
	"the limit per a column for the top number of statistics to be flushed "+
		"to the, the activity tables.",
	500,
	settings.NonNegativeInt,
)

// SQLStatsActivityMaxPersistedRows specifies maximum number of rows that will be
// retained in system.statement_statistics and system.transaction_statistics.
// Defaults computed 500(top limit)*6(num columns)*24(hrs)*3(days)=216000
// to give a minimum of 3 days of history. It was rounded down to 200k to
// give an even number. The top 500(controlled by sql.stats.activity.top.limit)
// are likely the same for several columns ,so it should still give 3 days
// of history for the default settings
var sqlStatsActivityMaxPersistedRows = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.stats.activity.persisted_rows.max",
	"maximum number of rows of statement and transaction"+
		" activity that will be persisted in the system tables.",
	200000, /* defaultValue*/
	settings.NonNegativeInt,
).WithPublic()

const numberOfTopColumns = 6

type sqlActivityUpdateJob struct {
	job *jobs.Job
}

// Resume implements the jobs.sqlActivityUpdateJob interface.
// The SQL activity job runs AS a forever-running background job.
// and runs the SqlActivityUpdater according to sql.stats.activity.flush.interval.
func (j *sqlActivityUpdateJob) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	log.Infof(ctx, "starting sql stats activity flush job")
	// The sql activity update job is a forever running background job.
	// It's always safe to wind the SQL pod down whenever it's
	// running, something we indicate through the job's idle
	// status.
	j.job.MarkIdle(true)

	execCtx := execCtxI.(JobExecContext)
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper
	settings := execCtx.ExecCfg().Settings

	for {
		sampleInterval := flushInterval.Get(&settings.SV)
		select {
		case <-time.After(sampleInterval):
			if enabled.Get(&settings.SV) {
				updater := NewSqlActivityUpdater(settings, execCtx.ExecCfg().InternalDB)
				if err := updater.TransferStatsToActivity(ctx); err != nil {
					log.Warningf(ctx, "error running sql activity updater job: %v", err)
				}
			}
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		}
	}
}

// OnFailOrCancel implements the jobs.sqlActivityUpdateJob interface.
// No action needs to be taken on our part. There's no state to clean up.
func (r *sqlActivityUpdateJob) OnFailOrCancel(
	ctx context.Context, _ interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"sql activity is not cancelable")
		log.Infof(ctx, "%v", err)
	}
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoUpdateSQLActivity,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &sqlActivityUpdateJob{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}

// NewSqlActivityUpdater returns a new instance of SqlActivityUpdater.
func NewSqlActivityUpdater(setting *cluster.Settings, db isql.DB) *SqlActivityUpdater {
	return &SqlActivityUpdater{
		st: setting,
		db: db,
	}
}

type SqlActivityUpdater struct {
	st *cluster.Settings
	db isql.DB
}

func (u *SqlActivityUpdater) TransferStatsToActivity(ctx context.Context) error {
	// Get the config and pass it around to avoid any issue of it changing
	// in the middle of the execution.
	maxRowPersistedRows := sqlStatsActivityMaxPersistedRows.Get(&u.st.SV)
	topLimit := sqlStatsActivityTopLimit.Get(&u.st.SV)
	aggTs := u.ComputeAggregatedTs(ctx, &u.st.SV)

	// The counts are using AS OF SYSTEM TIME so the values may be slightly
	// off. This is acceptable to increase the performance.
	stmtRowCount, txnRowCount, totalStmtClusterExecCount, totalTxnClusterExecCount, err := u.getAostExecutionCount(ctx, aggTs)
	if err != nil {
		return err
	}

	// No need to continue since there are no rows to transfer
	if stmtRowCount == 0 && txnRowCount == 0 {
		log.Infof(ctx, "sql stats activity found no rows at %s", aggTs)
		return nil
	}

	// Create space on the table before adding new rows to avoid
	// going OVER the count. If the compaction fails it will not
	// add any new rows.
	err = u.compactActivityTables(ctx, maxRowPersistedRows-stmtRowCount)
	if err != nil {
		return err
	}

	// There are fewer rows than filtered top would return.
	// Just transfer all the stats to avoid overhead of getting
	// the tops.
	if stmtRowCount < (topLimit*numberOfTopColumns) && txnRowCount < (topLimit*numberOfTopColumns) {
		return u.transferAllStats(ctx, aggTs, totalStmtClusterExecCount, totalTxnClusterExecCount)
	}

	// Only transfer the top sql.stats.activity.top.limit for each of
	// the 6 most popular columns
	err = u.transferTopStats(ctx, aggTs, topLimit, totalStmtClusterExecCount, totalTxnClusterExecCount)
	return err
}

// transferAllStats is used to transfer all the stats FROM
// system.statement_statistics and system.transaction_statistics
// to system.statement_activity and system.transaction_activity
func (u *SqlActivityUpdater) transferAllStats(
	ctx context.Context,
	aggTs time.Time,
	totalStmtClusterExecCount int64,
	totalTxnClusterExecCount int64,
) error {
	_, err := u.db.Executor().ExecEx(ctx,
		"activity-flush-txn-transfer-all",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
			UPSERT INTO system.public.transaction_activity 
(aggregated_ts, fingerprint_id, app_name, agg_interval, metadata,
 statistics, query, execution_count, execution_total_seconds,
 execution_total_cluster_seconds, contention_time_avg_seconds, 
 cpu_sql_avg_nanos, service_latency_avg_seconds, service_latency_p99_seconds)
    (SELECT aggregated_ts,
            fingerprint_id,
            app_name,
            agg_interval,
            metadata,
            statistics,
            '' AS query,
            (statistics->'execution_statistics'->>'cnt')::int,
            ((statistics->'execution_statistics'->>'cnt')::float)*((statistics->'statistics'->'svcLat'->>'mean')::float),
            $2 AS execution_total_cluster_seconds,
            COALESCE((statistics->'execution_statistics'->'contentionTime'->>'mean')::float,0),
            COALESCE((statistics->'execution_statistics'->'cpu_sql_nanos'->>'mean')::float,0),
            (statistics->'statistics'->'svcLat'->>'mean')::float,
            COALESCE((statistics->'statistics'->'latencyInfo'->>'p99')::float, 0)
     FROM (SELECT
                  max(aggregated_ts) AS aggregated_ts,
                  app_name,
                  fingerprint_id,
                  agg_interval,
                  max(metadata) AS metadata,
                  crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics
           FROM system.public.transaction_statistics
           WHERE aggregated_ts = $1
             and app_name not like '$ internal%'
           GROUP BY app_name,
                    fingerprint_id,
                    agg_interval));
`,
		aggTs,
		totalTxnClusterExecCount,
	)

	if err != nil {
		return err
	}

	_, err = u.db.Executor().ExecEx(ctx,
		"activity-flush-stmt-transfer-all",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
			UPSERT
INTO system.public.statement_activity (aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name,
                                       agg_interval, metadata, statistics, plan, index_recommendations, execution_count,
                                       execution_total_seconds, execution_total_cluster_seconds,
                                       contention_time_avg_seconds,
                                       cpu_sql_avg_nanos,
                                       service_latency_avg_seconds, service_latency_p99_seconds)
    (SELECT aggregated_ts,
            fingerprint_id,
            transaction_fingerprint_id,
            plan_hash,
            app_name,
            agg_interval,
            metadata,
            statistics,
            plan,
            index_recommendations,
            (statistics -> 'execution_statistics' ->> 'cnt')::int,
            ((statistics -> 'execution_statistics' ->> 'cnt')::float) *
            ((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float),
            $2 AS execution_total_cluster_seconds,
            COALESCE((statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
            COALESCE((statistics -> 'execution_statistics' -> 'cpu_sql_nanos' ->> 'mean')::float, 0),
            (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float,
            COALESCE((statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
     FROM (SELECT max(aggregated_ts)                                           AS aggregated_ts,
                  fingerprint_id,
                  transaction_fingerprint_id,
                  plan_hash,
                  app_name,
                  agg_interval,
                  max(metadata)                                                AS metadata,
                  crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics,
                  plan,
                  index_recommendations
           FROM system.public.statement_statistics
           WHERE aggregated_ts = $1
             and app_name not like '$ internal%'
           GROUP BY app_name,
                    fingerprint_id,
                    transaction_fingerprint_id,
                    plan_hash,
                    agg_interval,
                    plan,
                    index_recommendations));
`,
		aggTs,
		totalStmtClusterExecCount,
	)

	return err
}

// transferTopStats is used to transfer top N stats FROM
// system.statement_statistics and system.transaction_statistics
// to system.statement_activity and system.transaction_activity
func (u *SqlActivityUpdater) transferTopStats(
	ctx context.Context,
	aggTs time.Time,
	topLimit int64,
	totalStmtClusterExecCount int64,
	totalTxnClusterExecCount int64,
) (retErr error) {
	// Select the top 500 (controlled by sql.stats.activity.top.limit) for
	// execution_count, service_latency,cpu_sql_nanos, contention_time,
	// p99_latency and insert into transaction_activity table.
	_, err := u.db.Executor().ExecEx(ctx,
		"activity-flush-txn-transfer-tops",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
UPSERT
INTO system.public.transaction_activity
(aggregated_ts, fingerprint_id, app_name, agg_interval, metadata,
 statistics, query, execution_count, execution_total_seconds,
 execution_total_cluster_seconds, contention_time_avg_seconds,
 cpu_sql_avg_nanos, service_latency_avg_seconds, service_latency_p99_seconds)
    (SELECT aggregated_ts,
            fingerprint_id,
            app_name,
            agg_interval,
            metadata,
            statistics,
            ''  AS query,
            (statistics -> 'execution_statistics' ->> 'cnt')::int,
            ((statistics -> 'execution_statistics' ->> 'cnt')::float) *
            ((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float),
            $2 AS execution_total_cluster_seconds,
            COALESCE((statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
            COALESCE((statistics -> 'execution_statistics' -> 'cpu_sql_nanos' ->> 'mean')::float, 0),
            (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float,
            COALESCE((statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
     FROM (SELECT max(ts.aggregated_ts)                                        AS aggregated_ts,
                  ts.app_name,
                  ts.fingerprint_id,
                  ts.agg_interval,
                  max(ts.metadata)                                             AS metadata,
                  crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics
           FROM system.public.transaction_statistics ts
                    inner join (SELECT fingerprint_id, app_name, agg_interval
                                FROM (SELECT fingerprint_id, app_name, agg_interval,
                                             row_number()
                                             OVER (ORDER BY (statistics -> 'execution_statistics' ->> 'cnt')::int desc)        AS ePos,
                                             row_number()
                                             OVER (ORDER BY (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float desc)   AS sPos,
                                             row_number()
                                             OVER (ORDER BY ((statistics -> 'execution_statistics' ->> 'cnt')::float) *
                                                            ((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float) desc) AS tPos,
                                             row_number() OVER (ORDER BY COALESCE(
                                                     (statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float,
                                                     0) desc)                                                                  AS cPos,
                                             row_number() OVER (ORDER BY COALESCE(
                                                     (statistics -> 'execution_statistics' -> 'cpu_sql_nanos' ->> 'mean')::float,
                                                     0) desc)                                                                  AS uPos,
                                             row_number() OVER (ORDER BY COALESCE(
                                                     (statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::float,
                                                     0) desc)                                                                  AS lPos
                                      FROM (SELECT fingerprint_id, app_name, agg_interval,
                                                   crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics
                                            FROM system.public.transaction_statistics
                                            WHERE aggregated_ts = $1 and
                                                  app_name not like '$ internal%'
                                            GROUP BY app_name,
                                                     fingerprint_id,
																										agg_interval))
                                WHERE ePos < $3
                                   or sPos < $3
                                   or tPos < $3
                                   or cPos < $3
                                   or uPos < $3
                                   or lPos < $3) agg
                               on agg.app_name = ts.app_name and agg.fingerprint_id = ts.fingerprint_id and
                                  agg.agg_interval = ts.agg_interval
           GROUP BY ts.app_name,
                    ts.fingerprint_id,
                    ts.agg_interval));
`,
		aggTs,
		totalStmtClusterExecCount,
		topLimit,
	)

	if err != nil {
		return err
	}

	// Select the top 500 (controlled by sql.stats.activity.top.limit) for
	// execution_count, service_latency, cpu_sql_nanos, contention_time,
	// p99_latency, and any statement that was in the top N transactions and
	// insert into statement_activity table.
	_, err = u.db.Executor().ExecEx(ctx,
		"activity-flush-stmt-transfer-tops",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
UPSERT
INTO system.public.statement_activity
(aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name,
 agg_interval, metadata, statistics, plan, index_recommendations, execution_count,
 execution_total_seconds, execution_total_cluster_seconds,
 contention_time_avg_seconds,
 cpu_sql_avg_nanos,
 service_latency_avg_seconds, service_latency_p99_seconds)
    (SELECT aggregated_ts,
            fingerprint_id,
            transaction_fingerprint_id,
            plan_hash,
            app_name,
            agg_interval,
            metadata,
            statistics,
            plan,
            index_recommendations,
            (statistics -> 'execution_statistics' ->> 'cnt')::int,
            ((statistics -> 'execution_statistics' ->> 'cnt')::float) *
            ((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float),
            $2 AS execution_total_cluster_seconds,
            COALESCE((statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
            COALESCE((statistics -> 'execution_statistics' -> 'cpu_sql_nanos' ->> 'mean')::float, 0),
            (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float,
            COALESCE((statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
     FROM (SELECT max(ss.aggregated_ts)                                           AS aggregated_ts,
                  ss.fingerprint_id,
                  ss.transaction_fingerprint_id,
                  ss.plan_hash,
                  ss.app_name,
                  ss.agg_interval,
                  max(ss.metadata)                                                AS metadata,
                  crdb_internal.merge_transaction_stats(array_agg(ss.statistics)) AS statistics,
                  ss.plan,
                  ss.index_recommendations
           FROM system.public.statement_statistics ss
           inner join (SELECT fingerprint_id, app_name
                                    FROM (SELECT fingerprint_id, app_name,
                                                 row_number()
                                                 OVER (ORDER BY (statistics -> 'execution_statistics' ->> 'cnt')::int desc)      AS ePos,
                                                 row_number()
                                                 OVER (ORDER BY (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float desc) AS sPos,
                                                 row_number() OVER (ORDER BY
                                                         ((statistics -> 'execution_statistics' ->> 'cnt')::float) *
                                                         ((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::float) desc)      AS tPos,
                                                 row_number() OVER (ORDER BY COALESCE(
                                                         (statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float,
                                                         0) desc)                                                                AS cPos,
                                                 row_number() OVER (ORDER BY COALESCE(
                                                         (statistics -> 'execution_statistics' -> 'cpu_sql_nanos' ->> 'mean')::float,
                                                         0) desc)                                                                AS uPos,
                                                 row_number() OVER (ORDER BY COALESCE(
                                                         (statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::float,
                                                         0) desc)                                                                AS lPos
                                          FROM (SELECT fingerprint_id,
                                                       app_name,
                                                       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
                                                FROM system.public.statement_statistics
                                                WHERE aggregated_ts = $1 and
                                                      app_name not like '$ internal%'
                                                GROUP BY app_name,
                                                         fingerprint_id))
                                    WHERE ePos < $3
                                       or sPos < $3
                                       or tPos < $3
                                       or cPos < $3
                                       or uPos < $3
                                       or lPos < $3) agg on agg.app_name = ss.app_name and agg.fingerprint_id = ss.fingerprint_id
           WHERE aggregated_ts = $1
           GROUP BY ss.app_name,
                    ss.fingerprint_id,
                    ss.transaction_fingerprint_id,
                    ss.plan_hash,
                    ss.agg_interval,
                    ss.plan,
                    ss.index_recommendations));
`,
		aggTs,
		totalTxnClusterExecCount,
		topLimit,
	)

	return err
}

// getAosExecutionCount is used to get the row counts of both the
// system.statement_statistics and system.transaction_statistics.
// It also gets the total execution count for the specified aggregated
// timestamp.
func (u *SqlActivityUpdater) getAostExecutionCount(
	ctx context.Context, aggTs time.Time,
) (
	stmtRowCount int64,
	txnRowCount int64,
	totalStmtClusterExecCount int64,
	totalTxnClusterExecCount int64,
	retErr error,
) {
	it, err := u.db.Executor().QueryIteratorEx(ctx,
		"activity-flush-count",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
				SELECT row_count, ex_sum FROM (SELECT
					count_rows():::int AS row_count,
					COALESCE(sum(execution_count)::int, 0) AS ex_sum
				FROM system.statement_statistics AS OF SYSTEM TIME follower_read_timestamp()
				WHERE  app_name not like '$ internal%' and aggregated_ts = $1
			union all
				SELECT
					count_rows():::int AS row_count,
					COALESCE(sum(execution_count)::int, 0) AS ex_sum
				FROM system.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp()
				WHERE app_name not like '$ internal%' and  aggregated_ts = $1) AS OF SYSTEM TIME follower_read_timestamp()`,
		aggTs,
	)

	if err != nil {
		return -1, -1, -1, -1, err
	}

	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	stmtRowCount, totalStmtClusterExecCount, err = u.getExecutionCountFromRow(ctx, it)
	if err != nil {
		return -1, -1, -1, -1, err
	}

	txnRowCount, totalTxnClusterExecCount, err = u.getExecutionCountFromRow(ctx, it)
	return stmtRowCount, txnRowCount, totalStmtClusterExecCount, totalTxnClusterExecCount, err
}

func (u *SqlActivityUpdater) getExecutionCountFromRow(
	ctx context.Context, iter isql.Rows,
) (rowCount int64, totalExecutionCount int64, err error) {
	ok, err := iter.Next(ctx)
	if err != nil {
		return -1, -1, err
	}

	if !ok {
		return -1, -1, fmt.Errorf("no rows in activity-flush-count")
	}

	row := iter.Cur()
	if row[0] == tree.DNull || row[1] == tree.DNull {
		return 0, 0, nil
	}

	return int64(tree.MustBeDInt(row[0])), int64(tree.MustBeDInt(row[1])), nil
}

// ComputeAggregatedTs returns the aggregation timestamp to assign
// in-memory SQL stats during storage or aggregation.
func (u *SqlActivityUpdater) ComputeAggregatedTs(
	ctx context.Context, sv *settings.Values,
) time.Time {
	interval := sqlStatsActivityAggregationInterval.Get(sv)
	intervalSqlStats := persistedsqlstats.SQLStatsAggregationInterval.Get(sv)

	// system.{statement/transaction}_activity stats are based on the values in
	// system.{statement/transaction}_statistics, so the interval must be equal
	// to or greater than the statistics table.
	if interval < intervalSqlStats {
		log.Warningf(ctx, "sql.stats.activity.aggregation.interval: %s must be greater than or equal to sql.stats.aggregation.interval: %s", interval, intervalSqlStats)
		interval = intervalSqlStats
	}
	now := timeutil.Now()

	aggTs := now.Truncate(interval)

	return aggTs
}

// compactActivityTables is used delete rows FROM the activity tables
// to keep the tables under the specified config limit.
func (u *SqlActivityUpdater) compactActivityTables(ctx context.Context, maxRowCount int64) error {
	rowCount, err := u.getTableRowCount(ctx, "system.statement_activity")
	if err != nil {
		return err
	}

	if rowCount < maxRowCount {
		return nil
	}

	// Delete all the rows FROM the aggregated_ts to avoid
	// showing partial data for a time range.
	_, err = u.db.Executor().ExecEx(ctx,
		"activity-stmt-compaction",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
				delete
FROM system.statement_activity
WHERE aggregated_ts in (SELECT DISTINCT aggregated_ts FROM system.statement_activity ORDER BY aggregated_ts limit $1);`,
		rowCount-maxRowCount,
	)

	if err != nil {
		return err
	}

	rowCount, err = u.getTableRowCount(ctx, "system.transaction_activity")
	if err != nil {
		return err
	}

	if rowCount < maxRowCount {
		return nil
	}

	// Delete all the rows FROM the aggregated_ts to avoid
	// showing partial data for a time range.
	_, err = u.db.Executor().ExecEx(ctx,
		"activity-txn-compaction",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
				delete
FROM system.transaction_activity
WHERE aggregated_ts in (SELECT DISTINCT aggregated_ts FROM system.transaction_activity ORDER BY aggregated_ts limit $1);`,
		rowCount-maxRowCount,
	)

	return err
}

// getTableRowCount is used to get the row counts of both the
// system.statement_statistics and system.transaction_statistics.
// It also gets the total execution count for the specified aggregated
// timestamp.
func (u *SqlActivityUpdater) getTableRowCount(
	ctx context.Context, tableName string,
) (rowCount int64, retErr error) {
	query := fmt.Sprintf(`
				SELECT
					count_rows()::int
				FROM %s AS OF SYSTEM TIME follower_read_timestamp()`, tableName)
	datums, err := u.db.Executor().QueryRowEx(ctx,
		"activity-total-count",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		query,
	)

	if err != nil {
		return 0, err
	}

	if datums == nil {
		return 0, nil
	}

	if datums[0] == tree.DNull {
		return 0, nil
	}

	return int64(tree.MustBeDInt(datums[0])), nil
}
