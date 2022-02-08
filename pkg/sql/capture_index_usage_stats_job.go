// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/robfig/cron/v3"
	"time"
)

// CaptureIndexUsageStatsInterval is the interval at which index
// usage stats are exported to the telemetry logging channel.
var CaptureIndexUsageStatsInterval = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.capture_index_usage_stats.telemetry.interval",
	"cron-tab interval to capture index usage statistics to the telemetry logging channel",
	"0 */8 * * *", /* defaultValue */
	func(_ *settings.Values, s string) error {
		if _, err := cron.ParseStandard(s); err != nil {
			return errors.Wrap(err, "invalid cron expression")
		}
		return nil
	},
).WithPublic()

const captureIndexStatsScheduleLabel = "capture-index-stats-telemetry"

//////////////////////////////////////
//////// SCHEDULED JOB LOGIC	////////
//////////////////////////////////////

func init() {
	jobs.RegisterConstructor(jobspb.TypeCaptureIndexUsageStats, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &captureIndexUsageStatsResumer{
			job: job,
			st:  settings,
		}
	})

	jobs.RegisterScheduledJobExecutorFactory(
		tree.CaptureIndexUsageStatsExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.CaptureIndexUsageStatsExecutor.InternalName())
			return &captureIndexUsageStatsExecutor{
				metrics: captureIndexUsageStatsMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		})
}

func CreateCaptureIndexUsageStatsScheduleJob(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, st *cluster.Settings,
) (*jobs.ScheduledJob, error) {
	scheduleLabel := captureIndexStatsScheduleLabel

	fmt.Println("CREATING SCHEDULE LOGGING JOB")

	// Check for existing scheduled logging job.
	scheduleExists, err := checkExistingCaptureIndexUsageStatsSchedule(ctx, ie, txn, scheduleLabel)
	if err != nil {
		return nil, err
	}
	if scheduleExists {
		return nil, errors.Newf("creating multiple %s scheduled jobs is not allowed", scheduleLabel)
	}

	captureIndexUsageStatsScheduleJob := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
	schedule := CaptureIndexUsageStatsInterval.Get(&st.SV)
	if err := captureIndexUsageStatsScheduleJob.SetSchedule(schedule); err != nil {
		return nil, err
	}

	captureIndexUsageStatsScheduleJob.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait:    jobspb.ScheduleDetails_SKIP,
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})

	// TODO(thomas): is this ok
	args, err := pbtypes.MarshalAny(&CaptureIndexUsageStatsExecutionArgs{})
	if err != nil {
		return nil, err
	}
	captureIndexUsageStatsScheduleJob.SetExecutionDetails(
		tree.CaptureIndexUsageStatsExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: args},
	)

	captureIndexUsageStatsScheduleJob.SetScheduleLabel(scheduleLabel)
	captureIndexUsageStatsScheduleJob.SetOwner(security.NodeUserName())

	captureIndexUsageStatsScheduleJob.SetScheduleStatus(string(jobs.StatusPending))
	if err := captureIndexUsageStatsScheduleJob.Create(ctx, ie, txn); err != nil {
		return nil, err
	}

	return captureIndexUsageStatsScheduleJob, nil
}

func checkExistingCaptureIndexUsageStatsSchedule(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, label string,
) (exists bool, _ error) {
	query := "SELECT count(*) FROM system.scheduled_jobs WHERE schedule_name = $1"

	row, err := ie.QueryRowEx(ctx, "check-existing-sql-stats-schedule", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		query, label,
	)

	if err != nil {
		return false /* exists */, err
	}

	if row == nil {
		return false /* exists */, errors.AssertionFailedf("unexpected empty result when querying system.scheduled_job")
	}

	if len(row) != 1 {
		return false /* exists */, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
	}

	// Defensively check the count.
	return tree.MustBeDInt(row[0]) > 0, nil /* err */
}

//////////////////////////////////
//////// SYSTEM JOB LOGIC	////////
//////////////////////////////////

type captureIndexUsageStatsResumer struct {
	job *jobs.Job
	st  *cluster.Settings
	sj  *jobs.ScheduledJob
}

// Pass linting (unused type).
var _ jobs.Resumer = &captureIndexUsageStatsResumer{}

func (r *captureIndexUsageStatsResumer) Resume(ctx context.Context, execCtx interface{}) error {
	log.Infof(ctx, "starting capture of index usage stats job")
	fmt.Println("starting capture of index usage stats job")
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor

	// Get all database names.
	var allDatabaseNames []string
	var ok bool
	var expectedNumDatums = 1
	it, err := ie.QueryIteratorEx(
		ctx,
		"get-all-db-names",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		`SELECT database_name FROM [SHOW DATABASES]`,
	)
	if err != nil {
		return err
	}

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { err = errors.CombineErrors(err, it.Close()) }()
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return errors.New("unexpected null row while capturing index usage stats")
		}
		if row.Len() != expectedNumDatums {
			return errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
		}

		databaseName := string(tree.MustBeDString(row[0]))
		allDatabaseNames = append(allDatabaseNames, databaseName)
	}

	// Capture index usage statistics for each database.
	//var collectedCapturedIndexStats eventpb.CollectedCapturedIndexUsageStats
	expectedNumDatums = 10
	for _, databaseName := range allDatabaseNames {
		// Omit index usage statistics of the 'system' database.
		if databaseName == "system" {
			continue
		}
		stmt := fmt.Sprintf(`
		SELECT
		'%s' as database_name,
		 ti.descriptor_name as table_name,
		 ti.descriptor_id as table_id,
		 ti.index_name,
		 ti.index_id,
		 ti.index_type,
		 ti.is_unique,
		 ti.is_inverted,
		 total_reads,
		 last_read
		FROM %s.crdb_internal.index_usage_statistics AS us
		JOIN %s.crdb_internal.table_indexes ti
		ON us.index_id = ti.index_id
		 AND us.table_id = ti.descriptor_id
		ORDER BY total_reads ASC;
	`, databaseName, databaseName, databaseName)

		it, err = ie.QueryIteratorEx(
			ctx,
			"capture-index-usage-stats",
			nil,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			stmt,
		)
		if err != nil {
			return err
		}

		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			var row tree.Datums
			if row = it.Cur(); row == nil {
				return errors.New("unexpected null row while capturing index usage stats")
			}

			if row.Len() != expectedNumDatums {
				return errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
			}

			databaseName := tree.MustBeDString(row[0])
			tableName := tree.MustBeDString(row[1])
			tableID := tree.MustBeDInt(row[2])
			indexName := tree.MustBeDString(row[3])
			indexID := tree.MustBeDInt(row[4])
			indexType := tree.MustBeDString(row[5])
			isUnique := tree.MustBeDBool(row[6])
			isInverted := tree.MustBeDBool(row[7])
			totalReads := uint64(tree.MustBeDInt(row[8]))
			lastRead := time.Time{}
			if row[9] != tree.DNull {
				lastRead = tree.MustBeDTimestampTZ(row[9]).Time
			}

			capturedIndexStats := &eventpb.CapturedIndexUsageStats{
				TableID:        uint32(roachpb.TableID(tableID)),
				IndexID:        uint32(roachpb.IndexID(indexID)),
				TotalReadCount: totalReads,
				LastRead:       lastRead.String(),
				DatabaseName:   string(databaseName),
				TableName:      string(tableName),
				IndexName:      string(indexName),
				IndexType:      string(indexType),
				IsUnique:       bool(isUnique),
				IsInverted:     bool(isInverted),
			}

			//collectedCapturedIndexStats.Stats = append(collectedCapturedIndexStats.Stats, capturedIndexStats)
			log.StructuredEvent(ctx, capturedIndexStats)
		}
		err = it.Close()
		if err != nil {
			return err
		}
	}
	//log.StructuredEvent(ctx, collectedCapturedIndexStats)
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
// TODO(thomas): using archer's fail/cancel logic. Not really sure what other cases we'd need to consider,
// the logic seems like it would be applicable to most jobs.
func (r *captureIndexUsageStatsResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	execCfg := p.ExecCfg()
	ie := execCfg.InternalExecutor
	return r.maybeNotifyJobTerminated(ctx, ie, execCfg, jobs.StatusFailed)
}

// maybeNotifyJobTerminated will notify the job termination
// (with termination status).
// TODO(thomas): using archer's fail/cancel logic. Not really sure what other cases we'd need to consider,
// the logic seems like it would be applicable to most jobs.
func (r *captureIndexUsageStatsResumer) maybeNotifyJobTerminated(
	ctx context.Context, ie sqlutil.InternalExecutor, exec *ExecutorConfig, status jobs.Status,
) error {
	log.Infof(ctx, "sql stats compaction job terminated with status = %s", status)
	if r.sj != nil {
		env := scheduledjobs.ProdJobSchedulerEnv
		if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
			if knobs.JobSchedulerEnv != nil {
				env = knobs.JobSchedulerEnv
			}
		}
		if err := jobs.NotifyJobTermination(
			ctx, env, r.job.ID(), status, r.job.Details(), r.sj.ScheduleID(),
			ie, nil /* txn */); err != nil {
			return err
		}

		return nil
	}
	return nil
}

type captureIndexUsageStatsMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &captureIndexUsageStatsMetrics{}

// MetricStruct implements metric.Struct interface.
func (m captureIndexUsageStatsMetrics) MetricStruct() {}

// captureIndexUsageStatsExecutor is executed by scheduledjob subsystem
// to launch captureIndexUsageStatsResumer through the job subsystem.
type captureIndexUsageStatsExecutor struct {
	metrics captureIndexUsageStatsMetrics
}

// Pass linting (unused type).
var _ jobs.ScheduledJobExecutor = &captureIndexUsageStatsExecutor{}

// Metrics implements jobs.ScheduledJobExecutor interface.
func (e *captureIndexUsageStatsExecutor) Metrics() metric.Struct {
	return e.metrics
}

// ExecuteJob implements the jobs.ScheduledJobExecutor interface.
func (e *captureIndexUsageStatsExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	fmt.Println("executing capture index usage stats job")
	if err := e.createCaptureIndexUsageStatsJob(ctx, cfg, sj, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
	}
	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *captureIndexUsageStatsExecutor) createCaptureIndexUsageStatsJob(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	p, cleanup := cfg.PlanHookMaker("invoke-capture-index-usage-stats", txn, security.NodeUserName())
	defer cleanup()

	_, err :=
		CreateCaptureIndexUsageStatsJob(ctx, &jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		}, txn, cfg.InternalExecutor, p.(*planner).ExecCfg().JobRegistry)

	if err != nil {
		return err
	}
	fmt.Println("Created capture index usage stats job")
	return nil
}

// CreateCaptureIndexUsageStatsJob creates a system.jobs record if there is no other
// logging jobs (under the same label) running. This is invoked by the
// scheduled job Executor.
// This function is exported for use by createCaptureIndexUsageStatsJob in the sql package,
// as createCaptureIndexUsageStatsJob will have scope of planner, giving access to the job
// registry.
func CreateCaptureIndexUsageStatsJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	jobRegistry *jobs.Registry,
) (jobspb.JobID, error) {

	// TODO(thomas): Check for an existing logging job with the same label
	err := CheckExistingCaptureIndexUsageStatsJob(ctx, nil /* job */, ie, txn)
	if err != nil {
		return jobspb.InvalidJobID, err
	}

	record := jobs.Record{
		Description: "Capture index usage statistics to telemetry logging channel",
		Username:    security.NodeUserName(),
		Details:     jobspb.CaptureIndexUsageStatsDetails{},
		Progress:    jobspb.CaptureIndexUsageStatsProgress{},
		CreatedBy:   createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

// NotifyJobTermination implements the jobs.ScheduledJobExecutor interface.
func (e *captureIndexUsageStatsExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if jobStatus == jobs.StatusFailed {
		jobs.DefaultHandleFailedRun(sj, "capturing index usage statistics job %d failed", jobID)
		e.metrics.NumFailed.Inc(1)
		return nil
	}

	if jobStatus == jobs.StatusSucceeded {
		e.metrics.NumSucceeded.Inc(1)
	}

	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobExecutor interface.
func (e *captureIndexUsageStatsExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	// TODO(thomas): get create schedule statement
	return "SELECT crdb_internal.capture_index_usage_stats()", nil
}

// CheckExistingCaptureIndexUsageStatsJob checks for existing logging job
// that are either PAUSED, CANCELED, or RUNNING. If so, it returns a
// ErrConcurrentSQLStatsCompaction.
func CheckExistingCaptureIndexUsageStatsJob(
	ctx context.Context, job *jobs.Job, ie sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	jobID := jobspb.InvalidJobID
	if job != nil {
		jobID = job.ID()
	}
	exists, err := jobs.RunningJobExists(ctx, jobID, ie, txn, func(payload *jobspb.Payload) bool {
		return payload.Type() == jobspb.TypeCaptureIndexUsageStats
	})

	if err == nil && exists {
		err = errors.New("another capture index usage stats job is running")
	}
	return err
}
