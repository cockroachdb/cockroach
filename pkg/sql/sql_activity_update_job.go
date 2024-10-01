// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// sqlStatsActivityFlushEnabled the stats activity flush job.
var sqlStatsActivityFlushEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.activity.flush.enabled",
	"enable the flush to the system statement and transaction activity tables",
	true)

// sqlStatsActivityTopCount is the cluster setting that controls the number of
// rows selected to be inserted into the activity tables
var sqlStatsActivityTopCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.activity.top.max",
	"the limit per column for the top number of statistics to be flushed "+
		"to the activity tables",
	500,
	settings.NonNegativeInt,
)

// sqlStatsActivityMaxPersistedRows specifies maximum number of rows that will be
// retained in system.statement_activity and system.transaction_activity.
// Defaults computed 500(top limit)*6(num columns)*24(hrs)*3(days)=216000
// to give a minimum of 3 days of history. It was rounded down to 200k to
// give an even number. The top 500(controlled by sql.stats.activity.top.max)
// are likely the same for several columns, so it should still give 3 days
// of history for the default settings
var sqlStatsActivityMaxPersistedRows = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.activity.persisted_rows.max",
	"maximum number of rows of statement and transaction"+
		" activity that will be persisted in the system tables",
	200000, /* defaultValue*/
	settings.NonNegativeInt,
	settings.WithPublic)

const numberOfStmtTopColumns = 6
const numberOfTxnTopColumns = 5

// sqlActivityUpdateJob is responsible for translating the data in the
// statement/txn statistics tables into the statement/txn _activity_
// tables.
type sqlActivityUpdateJob struct {
	job *jobs.Job
}

// Resume implements the jobs.sqlActivityUpdateJob interface.
// The SQL activity job runs AS a forever-running background job
// and runs the sqlActivityUpdater according to sql.stats.activity.flush.interval.
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
	statsFlush := execCtx.ExecCfg().InternalDB.server.sqlStats
	metrics := execCtx.ExecCfg().JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeAutoUpdateSQLActivity].(ActivityUpdaterMetrics)

	flushDoneSignal := make(chan struct{})
	defer func() {
		statsFlush.SetFlushDoneSignalCh(nil)
	}()

	statsFlush.SetFlushDoneSignalCh(flushDoneSignal)
	for {
		select {
		case <-flushDoneSignal:
			// A flush was done. Set the timer and wait for it to complete.
			if sqlStatsActivityFlushEnabled.Get(&settings.SV) {
				startTime := timeutil.Now().UnixNano()
				updater := newSqlActivityUpdater(settings, execCtx.ExecCfg().InternalDB, nil)
				if err := updater.TransferStatsToActivity(ctx); err != nil {
					log.Warningf(ctx, "error running sql activity updater job: %v", err)
					metrics.NumFailedUpdates.Inc(1)
				} else {
					metrics.NumSuccessfulUpdates.Inc(1)
				}
				metrics.UpdateLatency.RecordValue(timeutil.Now().UnixNano() - startTime)
			}
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		}
	}
}

// ActivityUpdaterMetrics must be public for metrics to get
// registered
type ActivityUpdaterMetrics struct {
	NumFailedUpdates     *metric.Counter
	NumSuccessfulUpdates *metric.Counter
	UpdateLatency        metric.IHistogram
}

func (m ActivityUpdaterMetrics) MetricStruct() {}

func newActivityUpdaterMetrics() metric.Struct {
	return ActivityUpdaterMetrics{
		NumFailedUpdates: metric.NewCounter(metric.Metadata{
			Name:        "sql.stats.activity.updates.failed",
			Help:        "Number of update attempts made by the SQL activity updater job that failed with errors",
			Measurement: "failed updates",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		NumSuccessfulUpdates: metric.NewCounter(metric.Metadata{
			Name:        "sql.stats.activity.updates.successful",
			Help:        "Number of successful updates made by the SQL activity updater job",
			Measurement: "successful updates",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		UpdateLatency: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "sql.stats.activity.update.latency",
				Help:        "The latency of updates made by the SQL activity updater job. Includes failed update attempts",
				Measurement: "Nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.LongRunning60mLatencyBuckets,
			Mode:         metric.HistogramModePrometheus,
		}),
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
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *sqlActivityUpdateJob) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoUpdateSQLActivity,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &sqlActivityUpdateJob{job: job}
		},
		jobs.DisablesTenantCostControl,
		jobs.WithJobMetrics(newActivityUpdaterMetrics()),
	)
}

// newSqlActivityUpdater returns a new instance of sqlActivityUpdater.
func newSqlActivityUpdater(
	setting *cluster.Settings, db isql.DB, testingKnobs *sqlstats.TestingKnobs,
) *sqlActivityUpdater {
	return &sqlActivityUpdater{
		st:           setting,
		db:           db,
		testingKnobs: testingKnobs,
	}
}

type sqlActivityUpdater struct {
	st           *cluster.Settings
	testingKnobs *sqlstats.TestingKnobs
	db           isql.DB
}

// TransferStatsToActivity call upsert stats function to current and prior hour.
func (u *sqlActivityUpdater) TransferStatsToActivity(ctx context.Context) error {
	// To improve performance we don't recalculate the entire top activity table everytime.
	// Only update the latest hour and the one prior.
	// We still need to recalculate the prior hour because the flush could happen during the switch of hours, and
	// we could miss some executions completed on the prior hour.
	aggTs := u.computeAggregatedTs(&u.st.SV)

	err := u.upsertStatsForAggregatedTs(ctx, aggTs)
	if err != nil {
		return err
	}
	return u.upsertStatsForAggregatedTs(ctx, aggTs.Add(-time.Hour))
}

// upsertStatsForAggregatedTs calculates the top sql stats and update the activity table accordingly.
func (u *sqlActivityUpdater) upsertStatsForAggregatedTs(
	ctx context.Context, aggTs time.Time,
) error {
	// Get the config and pass it around to avoid any issue of it changing
	// in the middle of the execution.
	maxRowPersistedRows := sqlStatsActivityMaxPersistedRows.Get(&u.st.SV)
	topLimit := sqlStatsActivityTopCount.Get(&u.st.SV)

	// The counts are using AS OF SYSTEM TIME so the values may be slightly
	// off. This is acceptable to increase the performance.
	stmtRowCount, txnRowCount, totalEstimatedStmtClusterExecSeconds, totalEstimatedTxnClusterExecSeconds,
		err := u.getAostRowCountAndTotalClusterExecSeconds(ctx, aggTs)
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
	if stmtRowCount < (topLimit*numberOfStmtTopColumns) && txnRowCount < (topLimit*numberOfTxnTopColumns) {
		return u.transferAllStats(ctx, aggTs, totalEstimatedStmtClusterExecSeconds, totalEstimatedTxnClusterExecSeconds)
	}

	// Only transfer the top sql.stats.activity.top.max for each of
	// the 6 most popular columns
	err = u.transferTopStats(ctx, aggTs, topLimit, totalEstimatedStmtClusterExecSeconds, totalEstimatedTxnClusterExecSeconds)
	return err
}

// transferAllStats is used to transfer all the stats FROM
// system.statement_statistics and system.transaction_statistics
// to system.statement_activity and system.transaction_activity
func (u *sqlActivityUpdater) transferAllStats(
	ctx context.Context,
	aggTs time.Time,
	totalEstimatedStmtClusterExecSeconds float64,
	totalEstimatedTxnClusterExecSeconds float64,
) error {
	// Any change should update cockroach/pkg/sql/opt/exec/execbuilder/testdata/observability
	_, err := u.db.Executor().ExecEx(ctx,
		"activity-flush-txn-transfer-all",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		`
			UPSERT INTO system.public.transaction_activity 
(aggregated_ts, fingerprint_id, app_name, agg_interval, metadata,
 statistics, query, execution_count, execution_total_seconds,
 execution_total_cluster_seconds, contention_time_avg_seconds, 
 cpu_sql_avg_nanos, service_latency_avg_seconds, service_latency_p99_seconds)
    (SELECT max_aggregated_ts,
            fingerprint_id,
            app_name,
            max_agg_interval,
            metadata,
            statistics,
            '' AS query,
            (statistics->'statistics'->>'cnt')::int,
            ((statistics->'statistics'->>'cnt')::float)*((statistics->'statistics'->'svcLat'->>'mean')::float),
            $1 AS execution_total_cluster_seconds,
            COALESCE((statistics->'execution_statistics'->'contentionTime'->>'mean')::float,0),
            COALESCE((statistics->'execution_statistics'->'cpuSQLNanos'->>'mean')::float,0),
            (statistics->'statistics'->'svcLat'->>'mean')::float,
            0 as service_latency_p99_seconds
     FROM (SELECT
                  max(aggregated_ts) AS max_aggregated_ts,
                  app_name,
                  fingerprint_id,
                  max(agg_interval) as max_agg_interval,
                  max(metadata) as metadata,
                  merge_transaction_stats(statistics) AS statistics
           FROM system.public.transaction_statistics
           WHERE aggregated_ts = $2
             and app_name not like '$ internal%'
           GROUP BY app_name,
                    fingerprint_id));
`,
		totalEstimatedTxnClusterExecSeconds,
		aggTs,
	)

	if err != nil {
		return err
	}

	// Any change should update cockroach/pkg/sql/opt/exec/execbuilder/testdata/observability
	_, err = u.db.Executor().ExecEx(ctx,
		"activity-flush-stmt-transfer-all",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
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
            '0x0000000000000000'::bytes,
            plan_hash,
            app_name,
            max_agg_interval,
            merged_metadata,
            merged_stats,
            max_plan,
            jsonb_array_to_string_array(merged_stats -> 'index_recommendations') as idx_rec,
            (merged_stats -> 'statistics' ->> 'cnt')::int,
            ((merged_stats -> 'statistics' ->> 'cnt')::float) *
            ((merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float),
            $1 AS execution_total_cluster_seconds,
            COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
            COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0),
            (merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float,
            COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
     FROM (SELECT max(aggregated_ts)                                           AS aggregated_ts,
                  fingerprint_id,
                  plan_hash,
                  app_name,
                  max(agg_interval) as max_agg_interval,
                  merge_stats_metadata(metadata)    AS merged_metadata,
                  merge_statement_stats(statistics) AS merged_stats,
                  max(plan) AS max_plan
           FROM system.public.statement_statistics
           WHERE aggregated_ts = $2
             and app_name not like '$ internal%'
           GROUP BY app_name,
                    fingerprint_id,
                    plan_hash));
`,
		totalEstimatedStmtClusterExecSeconds,
		aggTs,
	)

	return err
}

// transferTopStats is used to transfer top N stats FROM
// system.statement_statistics and system.transaction_statistics
// to system.statement_activity and system.transaction_activity
func (u *sqlActivityUpdater) transferTopStats(
	ctx context.Context,
	aggTs time.Time,
	topLimit int64,
	totalEstimatedStmtClusterExecSeconds float64,
	totalEstimatedTxnClusterExecSeconds float64,
) (retErr error) {

	// Deleting and inserting the activity tables needs to be done in the same
	// transaction. A user could try to access the table during the update. If
	// delete was done in a separate txn the user would get no results.
	errTxn := u.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {

		// Delete all the rows of the old data from the table for the current
		// aggregated timestamp. This is necessary because if a customer generates
		// a lot of fingerprints each time the upsert runs it will add all new rows
		// instead of updating the existing one. This causes the
		// transaction_activity to grow too large causing the UI to be slow.
		_, err := txn.ExecEx(ctx,
			"activity-flush-txn-transfer-tops",
			txn.KV(), /* txn */
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
			`DELETE FROM system.public.transaction_activity WHERE aggregated_ts = $1;`,
			aggTs)

		if err != nil {
			return err
		}

		// Select the top 500 (controlled by sql.stats.activity.top.max) for
		// each of execution_count, total execution time, service_latency, cpu_sql_nanos,
		// contention_time and insert into transaction_activity table.
		// Up to 2500 rows (sql.stats.activity.top.max * 5) may be added to
		// transaction_activity.
		// Any change should update cockroach/pkg/sql/opt/exec/execbuilder/testdata/observability
		_, err = txn.ExecEx(ctx,
			"activity-flush-txn-transfer-tops",
			txn.KV(), /* txn */
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
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
            merge_stats,
            ''  AS query,
            (merge_stats -> 'statistics' ->> 'cnt')::int,
            ((merge_stats -> 'statistics' ->> 'cnt')::float) *
            ((merge_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float),
            $1 AS execution_total_cluster_seconds,
            COALESCE ((merge_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
            COALESCE ((merge_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0),
            (merge_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float,
            0 as service_latency_p99_seconds
     FROM (SELECT ts.aggregated_ts                                       AS aggregated_ts,
                  ts.app_name,
                  ts.fingerprint_id,
                  max(ts.agg_interval) as agg_interval,
                  max(ts.metadata) AS metadata,
                  merge_transaction_stats(statistics) AS merge_stats
           FROM system.public.transaction_statistics ts
                    inner join (SELECT fingerprint_id, app_name
                                FROM (SELECT fingerprint_id, app_name,
                                           contentionTime, cpuTime,
                                            row_number() OVER (ORDER BY (merge_stats -> 'statistics' ->> 'cnt')::int desc) AS ePos,
                                            row_number() OVER (ORDER BY (merge_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float desc) AS sPos,
                                            row_number() OVER (ORDER BY ((merge_stats -> 'statistics' ->> 'cnt')::float) *
                                                ((merge_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float) desc) AS tPos,
                                            row_number() OVER (ORDER BY COALESCE((merge_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0) desc) AS cPos,
                                            row_number() OVER (ORDER BY COALESCE((merge_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0) desc) AS uPos
                                      FROM (SELECT fingerprint_id, app_name, merge_stats,
                                            (merge_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float as contentionTime,
                                            (merge_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float as cpuTime
                                            FROM (SELECT fingerprint_id, app_name,
                                                   merge_transaction_stats(statistics) AS merge_stats
                                            			FROM system.public.transaction_statistics
                                            			WHERE aggregated_ts = $2 and
                                                  	app_name not like '$ internal%'
                                            			GROUP BY app_name, fingerprint_id
																						)
																			)
																)
                                WHERE ePos < $3
                                   or sPos < $3
                                   or tPos < $3
                                   or (cPos < $3 AND contentionTime > 0)
                                   or (uPos < $3 AND cpuTime > 0)) agg
                               on agg.app_name = ts.app_name and agg.fingerprint_id = ts.fingerprint_id
           WHERE aggregated_ts = $2
           GROUP BY ts.aggregated_ts,
                    ts.app_name,
                    ts.fingerprint_id));;
`,
			totalEstimatedTxnClusterExecSeconds,
			aggTs,
			topLimit,
		)

		return err
	}, isql.WithPriority(admissionpb.UserLowPri))

	if errTxn != nil {
		return errTxn
	}

	errTxn = u.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {

		// Delete all the rows of the old data from the table for the current
		// aggregated timestamp. This is necessary because if a customer generates
		// a lot of fingerprints each time the upsert runs it will add all new rows
		// instead of updating the existing one. This causes the
		// transaction_activity to grow too large causing the UI to be slow.
		_, err := txn.ExecEx(ctx,
			"activity-flush-txn-transfer-tops",
			txn.KV(), /* txn */
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
			`DELETE FROM system.public.statement_activity WHERE aggregated_ts = $1;`,
			aggTs)

		if err != nil {
			return err
		}

		// Select the top 500 (controlled by sql.stats.activity.top.max) for each of
		// execution_count, total execution time, service_latency, cpu_sql_nanos,
		// contention_time, p99_latency. Also include all statements that are in the
		// top N transactions. This is needed so the statement information is
		// available for the ui so a user can see what is in the transaction.
		// Any change should update cockroach/pkg/sql/opt/exec/execbuilder/testdata/observability
		_, err = txn.ExecEx(ctx,
			"activity-flush-stmt-transfer-tops",
			txn.KV(), /* txn */
			sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
			`
WITH agg_stmt_stats AS (SELECT aggregated_ts,
                               fingerprint_id,
                               app_name,
                               merge_statement_stats(statistics) AS merged_stats
                        FROM system.public.statement_statistics
                        WHERE aggregated_ts = $2
                          and app_name not like '$ internal%'
                        GROUP BY aggregated_ts,
                                 app_name,
                                 fingerprint_id),
     limit_stmt_stats AS (SELECT aggregated_ts,
                                 fingerprint_id,
                                 app_name
                          FROM (SELECT aggregated_ts,
                                       fingerprint_id,
                                       app_name,
                                       merged_stats,
                                       row_number() OVER (ORDER BY (merged_stats -> 'statistics' ->> 'cnt')::int desc)                AS ePos,
                                       row_number() OVER (ORDER BY (merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float desc) AS sPos,
                                       row_number() OVER (ORDER BY
                                               ((merged_stats -> 'statistics' ->> 'cnt')::float) *
                                               ((merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float) desc)      AS tPos,
                                       row_number() OVER (ORDER BY COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0) desc) AS cPos,
                                       row_number() OVER (ORDER BY COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0) desc) AS uPos,
                                       row_number() OVER (ORDER BY COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0) desc) AS lPos
                                FROM agg_stmt_stats)
                          WHERE ePos < $3
                             or sPos < $3
                             or tPos < $3
														 or (cPos < $3 AND ((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float > 0))
														 or (uPos < $3 AND  ((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float > 0))
														 or (lPos < $3 AND ((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float > 0)))
UPSERT INTO system.public.statement_activity
(aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name,
 agg_interval, metadata, statistics, plan, index_recommendations, execution_count,
 execution_total_seconds, execution_total_cluster_seconds,
 contention_time_avg_seconds,
 cpu_sql_avg_nanos,
 service_latency_avg_seconds, service_latency_p99_seconds)(
SELECT aggregated_ts,
       fingerprint_id,
       '0x0000000000000000'::bytes,
       plan_hash,
       app_name,
       max_agg_interval,
       metadata,
       merged_stats,
       max_plan,
       jsonb_array_to_string_array(merged_stats -> 'index_recommendations') as idx_rec,
       (merged_stats -> 'statistics' ->> 'cnt')::int,
       ((merged_stats -> 'statistics' ->> 'cnt')::float) *
       ((merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float),
       $1 AS execution_total_cluster_seconds,
       COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
       COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0),
       (merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float,
       COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
FROM (SELECT ss.aggregated_ts AS aggregated_ts,
             ss.fingerprint_id,
             ss.plan_hash,
             ss.app_name,
             max(ss.agg_interval) AS max_agg_interval,
             max(ss.plan) AS max_plan,
             merge_stats_metadata(ss.metadata) AS metadata,
             merge_statement_stats(ss.statistics) AS merged_stats
      FROM system.statement_statistics ss
      INNER JOIN limit_stmt_stats using (aggregated_ts, fingerprint_id, app_name)
      GROUP BY aggregated_ts, fingerprint_id, plan_hash, app_name));
`,
			totalEstimatedStmtClusterExecSeconds,
			aggTs,
			topLimit,
		)

		return err
	}, isql.WithPriority(admissionpb.UserLowPri))

	return errTxn
}

// getAostRowCountAndTotalClusterExecSeconds is used to get the row counts of
// both the system.statement_statistics and system.transaction_statistics.
// It also gets the total execution seconds for all the stmts/txn for the
// specified aggregated timestamp.
func (u *sqlActivityUpdater) getAostRowCountAndTotalClusterExecSeconds(
	ctx context.Context, aggTs time.Time,
) (
	stmtRowCount int64,
	txnRowCount int64,
	totalEstimatedStmtClusterExecSeconds float64,
	totalEstimatedTxnClusterExecSeconds float64,
	retErr error,
) {
	aost := "AS OF SYSTEM TIME follower_read_timestamp()"
	if u.testingKnobs != nil {
		aost = u.testingKnobs.GetAOSTClause()
	}

	query := fmt.Sprintf(`
SELECT row_count,
       ex_sum
FROM (SELECT count_rows():::int                     AS row_count,
             COALESCE(sum(total_estimated_execution_time), 0) AS ex_sum
      FROM system.statement_statistics %[1]s
      WHERE aggregated_ts = $1
      union all
      SELECT
          count_rows():::int AS row_count, 
          COALESCE (sum(total_estimated_execution_time), 0) AS ex_sum
      FROM system.transaction_statistics %[1]s
      WHERE aggregated_ts = $1) %[1]s`, aost)

	it, err := u.db.Executor().QueryIteratorEx(ctx,
		"activity-flush-count",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		query,
		aggTs,
	)

	if err != nil {
		return -1, -1, -1, -1, err
	}

	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	stmtRowCount, totalEstimatedStmtClusterExecSeconds, err = u.getExecutionCountFromRow(ctx, it)
	if err != nil {
		return -1, -1, -1, -1, err
	}

	txnRowCount, totalEstimatedTxnClusterExecSeconds, err = u.getExecutionCountFromRow(ctx, it)
	return stmtRowCount, txnRowCount, totalEstimatedStmtClusterExecSeconds, totalEstimatedTxnClusterExecSeconds, err
}

func (u *sqlActivityUpdater) getExecutionCountFromRow(
	ctx context.Context, iter isql.Rows,
) (rowCount int64, totalEstimatedClusterExecSeconds float64, err error) {
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

	return int64(tree.MustBeDInt(row[0])), float64(tree.MustBeDFloat(row[1])), nil
}

func (u *sqlActivityUpdater) getTimeNow() time.Time {
	if u.testingKnobs != nil && u.testingKnobs.StubTimeNow != nil {
		return u.testingKnobs.StubTimeNow()
	}
	return timeutil.Now()
}

// ComputeAggregatedTs returns the aggregation timestamp to assign
// in-memory SQL stats during storage or aggregation.
func (u *sqlActivityUpdater) computeAggregatedTs(sv *settings.Values) time.Time {
	interval := persistedsqlstats.SQLStatsAggregationInterval.Get(sv)
	now := u.getTimeNow()
	aggTs := now.Truncate(interval)
	return aggTs
}

// compactActivityTables is used delete rows FROM the activity tables
// to keep the tables under the specified config limit.
func (u *sqlActivityUpdater) compactActivityTables(ctx context.Context, maxRowCount int64) error {
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
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		`
				DELETE
FROM system.statement_activity
WHERE aggregated_ts IN (SELECT DISTINCT aggregated_ts FROM (SELECT aggregated_ts FROM system.statement_activity ORDER BY aggregated_ts ASC limit $1));`,
		rowCount-maxRowCount,
	)

	if err != nil {
		return err
	}

	// Delete all the rows older than on the oldest statement_activity aggregated_ts.
	// This makes sure that the 2 tables are always in sync.
	_, err = u.db.Executor().ExecEx(ctx,
		"activity-txn-compaction",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		`
				DELETE
FROM system.transaction_activity
WHERE aggregated_ts not in (SELECT distinct aggregated_ts FROM system.statement_activity);`,
	)

	return err
}

// getTableRowCount is used to get the row counts of both the
// system.statement_statistics and system.transaction_statistics.
// It also gets the total execution count for the specified aggregated
// timestamp.
func (u *sqlActivityUpdater) getTableRowCount(
	ctx context.Context, tableName string,
) (rowCount int64, retErr error) {
	aost := u.testingKnobs.GetAOSTClause()
	query := fmt.Sprintf(`
				SELECT
					count_rows()::int
				FROM %s %s`, tableName, aost)
	datums, err := u.db.Executor().QueryRowEx(ctx,
		"activity-total-count",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
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
