// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
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
	"github.com/cockroachdb/redact"
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
// give an even number. The top k (controlled by sql.stats.activity.top.max)
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

const (
	numberOfStmtTopColumns = 6
	numberOfTxnTopColumns  = 5
)

const (
	sqlActivityCacheUpsertLimit = 500
)

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
	statsFlush := execCtx.ExecCfg().InternalDB.server.persistedSQLStats
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

// transferAllStats is used to transfer all the stats from
// system.statement_statistics and system.transaction_statistics
// to system.statement_activity and system.transaction_activity
func (u *sqlActivityUpdater) transferAllStats(
	ctx context.Context,
	aggTs time.Time,
	totalEstimatedStmtClusterExecSeconds float64,
	totalEstimatedTxnClusterExecSeconds float64,
) (retErr error) {
	it, err := u.db.Executor().QueryIteratorEx(ctx,
		"sql-activity-select-all-transactions",
		nil, // txn
		sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
		u.buildSelectAllTransactionsQuery(), aggTs)

	if err != nil {
		return err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	if err := u.upsertTopTransactions(ctx, "activity-flush-txn-select-all", u.db.Executor(),
		nil /* txn */, it, aggTs, totalEstimatedTxnClusterExecSeconds); err != nil {
		return err
	}

	it, err = u.db.Executor().QueryIteratorEx(ctx,
		"sql-activity-select-all-statements",
		nil, // txn
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		u.buildSelectAllStatementsQuery(), aggTs)

	if err != nil {
		return err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	return u.upsertStatements(ctx, "activity-flush-stmt-transfer-all", u.db.Executor(),
		nil /* txn */, it, aggTs, totalEstimatedStmtClusterExecSeconds)
}

/**
 * transferTopStats is used to transfer top k (sql.stats.activity.top.max)
 * stats from system.statement_statistics and system.transaction_statistics
 * to system.statement_activity and system.transaction_activity.
 */
func (u *sqlActivityUpdater) transferTopStats(
	ctx context.Context,
	aggTs time.Time,
	topLimit int64,
	totalEstimatedStmtClusterExecSeconds float64,
	totalEstimatedTxnClusterExecSeconds float64,
) (retErr error) {

	// Select the top transactions for each of the following 5 columns in the
	// provided aggregation time. Up to (sql.stats.activity.top.max * 5)
	// rows will be added to system.transaction_activity (default is 2500).
	// - execution count
	// - total execution time
	// - service latency
	// - cpu sql nanos
	// - contention time
	//
	// Note that it's important we use follower reads here to avoid causing
	// contention // since the rest of the cluster is still writing to the statistics
	// tables during the execution of this job.
	//
	it, err := u.db.Executor().QueryIteratorEx(ctx,
		"sql-activity-select-top-transactions",
		nil, // txn
		sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
		u.buildSelectTopTransactionsQuery(), aggTs, topLimit)
	if err != nil {
		return err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	// Deleting and inserting the activity tables needs to be done in the same
	// transaction. A user could try to access the table during the update. If
	// delete was done in a separate txn the user would get no results.
	const transferTopTxnsOpName = "activity-flush-txn-transfer-tops"
	errTxn := u.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {

		// Delete all the rows of the old data from the table for the current
		// aggregated timestamp. This is necessary because if a customer generates
		// a lot of fingerprints each time the upsert runs it will add all new rows
		// instead of updating the existing one. This causes the
		// transaction_activity to grow too large causing the UI to be slow.
		_, err := txn.ExecEx(ctx,
			transferTopTxnsOpName,
			txn.KV(), /* txn */
			sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
			`DELETE FROM system.public.transaction_activity WHERE aggregated_ts = $1;`,
			aggTs)
		if err != nil {
			return err
		}

		err = u.upsertTopTransactions(ctx, transferTopTxnsOpName, txn, txn.KV(), it, aggTs, totalEstimatedTxnClusterExecSeconds)
		if err != nil {
			return err
		}

		return nil
	})

	if errTxn != nil {
		return errTxn
	}

	// Select the top k (controlled by sql.stats.activity.top.max) for each of
	// execution_count, total execution time, service_latency, cpu_sql_nanos,
	// contention_time, p99_latency. Also include all statements that are in the
	// top k transactions. This is needed so the statement information is
	// available for the ui so a user can see what is in the transaction.
	it, err = u.db.Executor().QueryIteratorEx(ctx,
		"sql-activity-select-top-statements",
		nil, /* txn */
		sessiondata.NodeUserWithLowUserPrioritySessionDataOverride,
		u.buildSelectTopStatementsQuery(), aggTs, topLimit)

	if err != nil {
		return err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

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

		return u.upsertStatements(ctx, "activity-flush-stmt-transfer-tops", txn,
			txn.KV(), it, aggTs, totalEstimatedStmtClusterExecSeconds)
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

// buildSelectAllTransactionsQuery is used to build the query to select and
// aggregate all transactions for the provided aggregated timestamp to upsert
// into the system.transaction_activity table.
// The constructd query will expect 1 argument:
// arg1: the aggregated timestamp to select the transactions.
func (u *sqlActivityUpdater) buildSelectAllTransactionsQuery() string {
	aost := u.testingKnobs.GetAOSTClause()
	return fmt.Sprintf(`
WITH aggregate_transaction_statistics AS (
  SELECT 
    app_name,
    fingerprint_id,
    max(aggregated_ts) AS max_aggregated_ts,
    max(agg_interval) AS max_agg_interval,
    max(metadata) AS metadata,
    merge_transaction_stats(statistics) AS statistics
  FROM system.public.transaction_statistics %[1]s
  WHERE 
    aggregated_ts = $1 AND
    app_name NOT LIKE '$ internal%%'
  GROUP BY app_name, fingerprint_id
)
SELECT 
  fingerprint_id,
  app_name,
  max_agg_interval,
  metadata,
  statistics,
  (statistics->'statistics'->>'cnt')::int AS count,
  ((statistics->'statistics'->>'cnt')::float) * 
    ((statistics->'statistics'->'svcLat'->>'mean')::float) AS total_latency,
  COALESCE((statistics->'execution_statistics'->'contentionTime'->>'mean')::float, 0) AS avg_contention_time,
  COALESCE((statistics->'execution_statistics'->'cpuSQLNanos'->>'mean')::float, 0) AS avg_cpu_time,
  (statistics->'statistics'->'svcLat'->>'mean')::float AS avg_service_latency
FROM aggregate_transaction_statistics %[1]s
`, aost)
}

// buildSelectTopTransactionsQuery is used to build the query to select the top
// transactions for the provided aggregated timestamp by
// - execution count
// - total execution time
// - service latency
// - cpu sql nanos
// - contention time
//
// The query expects 2 arguments:
// arg1: aggregated timestamp to select the top transactions
// arg2: the top limit to select for each of the columns
func (u *sqlActivityUpdater) buildSelectTopTransactionsQuery() string {
	aost := u.testingKnobs.GetAOSTClause()

	// The query uses a multi-stage approach:
	// 1. transaction_aggregates CTE: Groups and aggregates transaction statistics in the
	//    requested interval..
	// 2. extracted_metrics CTE: Extracts relevant top-k metrics.
	// 3. ranked_stats CTE: Calculates rankings for each desired metric.
	// 4. Finally, we filter to get top transactions based on multiple ranking criteria.
	return fmt.Sprintf(`
WITH transaction_aggregates AS (
    SELECT 
        fingerprint_id,
        app_name,
        max(agg_interval) AS agg_interval,
        max(metadata) AS metadata,
        merge_transaction_stats(statistics) AS merge_stats
    FROM system.public.transaction_statistics %[1]s
    WHERE aggregated_ts = $1 AND app_name NOT LIKE '$ internal%%'
    GROUP BY app_name, fingerprint_id
),
extracted_metrics AS (
    SELECT
        fingerprint_id,
        app_name,
        agg_interval,
        metadata,
        merge_stats,
        (merge_stats -> 'statistics' ->> 'cnt')::int AS exec_count,
        (merge_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float AS svc_lat,
        COALESCE((merge_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0) AS contention_time,
        COALESCE((merge_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0) AS cpu_time
    FROM transaction_aggregates %[1]s
),
ranked_stats AS (
    SELECT
        *,
        row_number() OVER (ORDER BY exec_count DESC) AS exec_cnt_pos,
        row_number() OVER (ORDER BY svc_lat DESC) AS svc_lat_pos,
        row_number() OVER (ORDER BY exec_count::float * svc_lat DESC) AS run_time_pos,
        row_number() OVER (ORDER BY contention_time DESC) AS contention_pos,
        row_number() OVER (ORDER BY cpu_time DESC) AS cpu_pos
    FROM extracted_metrics
)
SELECT 
    fingerprint_id,
    app_name,
    agg_interval,
    metadata,
    merge_stats,
    exec_count,
    exec_count::float * svc_lat AS exec_lat_product,
    contention_time,
    cpu_time,
    svc_lat
FROM ranked_stats %[1]s
WHERE exec_cnt_pos < $2 
   OR svc_lat_pos < $2 
   OR run_time_pos < $2 
   OR (contention_pos < $2 AND contention_time > 0) 
   OR (cpu_pos < $2 AND cpu_time > 0)
`, aost)
}

// buildSelectAllStatementsQuery is used to build the query to select and
// aggregate all statements for the provided aggregated timestamp to upsert
// into the system.statement_activity table.
// The constructed query will expect 1 arguments:
// arg1: the aggregated timestamp to select the statements.
func (u *sqlActivityUpdater) buildSelectAllStatementsQuery() string {
	aost := u.testingKnobs.GetAOSTClause()
	return fmt.Sprintf(`
WITH statement_aggregates AS (
  SELECT 
    fingerprint_id,
    plan_hash,
    app_name,
    max(agg_interval) as max_agg_interval,
    merge_stats_metadata(metadata) AS merged_metadata,
    merge_statement_stats(statistics) AS merged_stats,
    max(plan) AS max_plan
  FROM system.public.statement_statistics %[1]s
  WHERE aggregated_ts = $1
    and app_name not like '$ internal%%'
  GROUP BY app_name,
           fingerprint_id,
           plan_hash
)
SELECT 
  fingerprint_id,
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
  COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
  COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0),
  (merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float,
  COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
FROM statement_aggregates %[1]s`, aost)
}

// buildSelectTopStatementsQuery is used to build the query to select the top
// statements for the provided aggregated timestamp by
// - execution count
// - total execution time
// - service latency
// - cpu sql nanos
// - contention time
// - p99 latency
//
// The query expects 2 arguments:
// arg1: aggregated timestamp to select the top statements
// arg2: the top limit to select for each of the columns
func (u *sqlActivityUpdater) buildSelectTopStatementsQuery() string {
	aost := u.testingKnobs.GetAOSTClause()
	return fmt.Sprintf(`
WITH agg_stmt_stats AS (
  SELECT 
    aggregated_ts,
    fingerprint_id,
    app_name,
    merge_statement_stats(statistics) AS merged_stats
  FROM system.public.statement_statistics %[1]s
  WHERE aggregated_ts = $1
    AND app_name NOT LIKE '$ internal%%'
  GROUP BY aggregated_ts, app_name, fingerprint_id
),
ranked_stats AS (
  SELECT 
    aggregated_ts,
    fingerprint_id,
    app_name,
    merged_stats,
    row_number() OVER (ORDER BY (merged_stats -> 'statistics' ->> 'cnt')::int DESC) AS cnt_rank,
    row_number() OVER (ORDER BY (merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float DESC) AS lat_rank,
    row_number() OVER (ORDER BY ((merged_stats -> 'statistics' ->> 'cnt')::float) * 
                                ((merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float) DESC) AS combined_rank,
    row_number() OVER (ORDER BY COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0) DESC) AS contention_rank,
    row_number() OVER (ORDER BY COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0) DESC) AS cpu_rank,
    row_number() OVER (ORDER BY COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0) DESC) AS p99_rank,
    COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0) AS contention_mean,
    COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0) AS cpu_mean,
    COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0) AS p99_latency
  FROM agg_stmt_stats
),
limit_stmt_stats AS (
  SELECT 
    aggregated_ts,
    fingerprint_id,
    app_name,
    merged_stats
  FROM ranked_stats
  WHERE cnt_rank < $2
     OR lat_rank < $2
     OR combined_rank < $2
     OR (contention_rank < $2 AND contention_mean > 0)
     OR (cpu_rank < $2 AND cpu_mean > 0)
     OR (p99_rank < $2 AND p99_latency > 0)
),
statement_details AS (
  SELECT 
    ss.aggregated_ts,
    ss.fingerprint_id,
    ss.plan_hash,
    ss.app_name,
    max(ss.agg_interval) AS max_agg_interval,
    max(ss.plan) AS max_plan,
    merge_stats_metadata(ss.metadata) AS metadata,
    merge_statement_stats(ss.statistics) AS merged_stats
  FROM system.statement_statistics ss
  JOIN limit_stmt_stats ls 
  ON ss.aggregated_ts = ls.aggregated_ts 
  AND ss.fingerprint_id = ls.fingerprint_id 
  AND ss.app_name = ls.app_name
  %[1]s
  GROUP BY ss.aggregated_ts, ss.fingerprint_id, ss.plan_hash, ss.app_name
)
SELECT 
  fingerprint_id,
  plan_hash,
  app_name,
  max_agg_interval,
  metadata,
  merged_stats,
  max_plan,
  jsonb_array_to_string_array(merged_stats -> 'index_recommendations') AS idx_rec,
  (merged_stats -> 'statistics' ->> 'cnt')::int,
  ((merged_stats -> 'statistics' ->> 'cnt')::float) * ((merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float),
  COALESCE((merged_stats -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::float, 0),
  COALESCE((merged_stats -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::float, 0),
  (merged_stats -> 'statistics' -> 'svcLat' ->> 'mean')::float,
  COALESCE((merged_stats -> 'statistics' -> 'latencyInfo' ->> 'p99')::float, 0)
FROM statement_details %[1]s
`, aost)
}

// upsertStatements is used to insert the rows in the iterator
// into the system.statement_activity table in batches of 500.
// It is possible for the provided txn to be nil.
func (u *sqlActivityUpdater) upsertStatements(
	ctx context.Context,
	opName string,
	executor isql.Executor,
	txn *kv.Txn,
	it isql.Rows,
	aggTs time.Time,
	totalStmtClusterExecSeconds float64,
) (retErr error) {

	const colCount = 17
	const queryBase = `
UPSERT INTO system.public.statement_activity (
		aggregated_ts,
		fingerprint_id,
		transaction_fingerprint_id,
		plan_hash,
		app_name,
		agg_interval,
		metadata,
		statistics,
		plan,
		index_recommendations,
		execution_count,
		execution_total_seconds,
		execution_total_cluster_seconds,
		contention_time_avg_seconds,
		cpu_sql_avg_nanos,
		service_latency_avg_seconds,
		service_latency_p99_seconds
	) VALUES
`

	ok, err := it.Next(ctx)
	if err != nil || !ok {
		return err
	}

	qArgs := make([]interface{}, 0, sqlActivityCacheUpsertLimit*colCount)
	var queryStr bytes.Buffer
	queryStr.WriteString(queryBase)

	for ; ok; ok, err = it.Next(ctx) {
		if err != nil {
			return err
		}
		if len(qArgs) > 0 {
			queryStr.WriteString(",")
		}

		queryStr.WriteString("(")
		for i := 0; i < colCount; i++ {
			if i > 0 {
				queryStr.WriteString(",")
			}
			queryStr.WriteString(fmt.Sprintf("$%d", len(qArgs)+i+1))
		}
		queryStr.WriteString(")")

		row := it.Cur()
		// Note that we don't track transaction_fingerprint_id in the activity cache table.
		qArgs = append(qArgs,
			aggTs,                       // aggregated_ts
			row[0],                      // fingerprint_id
			"",                          // transaction_fingerprint_id
			row[1],                      // plan_hash
			row[2],                      // app_name
			row[3],                      // agg_interval
			row[4],                      // metadata
			row[5],                      // statistics
			row[6],                      // plan
			row[7],                      // index_recommendations
			row[8],                      // execution_count
			row[9],                      // execution_total_seconds
			totalStmtClusterExecSeconds, // execution_total_cluster_seconds
			row[10],                     // contention_time_avg_seconds
			row[11],                     // cpu_sql_avg_nanos
			row[12],                     // service_latency_avg_seconds
			row[13],                     // service_latency_p99_seconds
		)

		if len(qArgs) == sqlActivityCacheUpsertLimit*colCount {
			// Batch is full.
			break
		}
	}

	_, err = executor.ExecEx(ctx, redact.Sprint(opName),
		txn,
		sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
		queryStr.String(), qArgs...)

	if err != nil {
		return err
	}

	return nil
}

// upsertTopTransactions is used to insert the rows in the iterator
// into the system.transaction_activity table in batches of 500.
// It is possible for the provided txn to be nil.
func (u *sqlActivityUpdater) upsertTopTransactions(
	ctx context.Context,
	opName string,
	executor isql.Executor,
	txn *kv.Txn,
	it isql.Rows,
	aggTs time.Time,
	totalTxnClusterExecSeconds float64,
) (retErr error) {
	const colCount = 14
	const queryBase = `
UPSERT INTO system.public.transaction_activity (
		aggregated_ts,
		fingerprint_id,
		app_name,
		agg_interval,
		metadata,
		statistics,
		query,
		execution_count,
		execution_total_seconds,
		execution_total_cluster_seconds,
		contention_time_avg_seconds,
		cpu_sql_avg_nanos,
		service_latency_avg_seconds,
		service_latency_p99_seconds
	) VALUES
`

	ok, err := it.Next(ctx)
	if err != nil || !ok {
		return err
	}

	qArgs := make([]interface{}, 0, sqlActivityCacheUpsertLimit*colCount)
	var queryStr bytes.Buffer
	queryStr.WriteString(queryBase)

	for ; ok; ok, err = it.Next(ctx) {
		if err != nil {
			return err
		}
		if len(qArgs) > 0 {
			queryStr.WriteString(",")
		}

		queryStr.WriteString("(")
		for i := 0; i < colCount; i++ {
			if i > 0 {
				queryStr.WriteString(",")
			}
			queryStr.WriteString(fmt.Sprintf("$%d", len(qArgs)+i+1))
		}
		queryStr.WriteString(")")

		row := it.Cur()
		qArgs = append(qArgs,
			aggTs,                      // aggregated_ts
			row[0],                     // fingerprint_id
			row[1],                     // app_name
			row[2],                     // agg_interval
			row[3],                     // metadata
			row[4],                     // stats
			"",                         // query
			row[5],                     // execution count
			row[6],                     // execution_total_seconds
			totalTxnClusterExecSeconds, // execution_total_cluster_seconds
			row[7],                     // contention_time_avg_seconds
			row[8],                     // cpu_sql_avg_nanos
			row[9],                     // service_latency_avg_seconds
			0,                          // service_latency_p99_seconds
		)

		if len(qArgs) == sqlActivityCacheUpsertLimit*colCount {
			// Batch is full.
			break
		}
	}

	_, err = executor.ExecEx(ctx, redact.Sprint(opName),
		txn,
		sessiondata.NodeUserWithBulkLowPriSessionDataOverride,
		queryStr.String(), qArgs...)
	if err != nil {
		return err
	}

	return nil
}
