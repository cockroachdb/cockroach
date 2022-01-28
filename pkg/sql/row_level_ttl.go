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
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/ttlpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type rowLevelTTLResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*rowLevelTTLResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}

// newRowLevelTTLScheduledJob returns a *jobs.ScheduledJob for row level TTL
// for a given table.
func newRowLevelTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv, owner security.SQLUsername, tblID descpb.ID,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(fmt.Sprintf("row-level-ttl-%d", tblID))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait: jobspb.ScheduleDetails_WAIT,
		// If a job fails, try again at the allocated cron time.
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})
	// TODO(#75189): allow user to configure schedule.
	if err := sj.SetSchedule("@hourly"); err != nil {
		return nil, err
	}
	args := &ttlpb.ScheduledRowLevelTTLArgs{
		TableID: tblID,
	}
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledRowLevelTTLExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)
	return sj, nil
}

type rowLevelTTLExecutor struct {
	metrics rowLevelTTLMetrics
}

var _ jobs.ScheduledJobController = (*rowLevelTTLExecutor)(nil)

type rowLevelTTLMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &rowLevelTTLMetrics{}

// MetricStruct implements metric.Struct interface.
func (m *rowLevelTTLMetrics) MetricStruct() {}

// OnDrop implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	return errors.WithHint(
		pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot drop row level TTL schedule",
		),
		`use ALTER TABLE ... RESET (expire_after) instead`,
	)
}

// ExecuteJob implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	args := &ttlpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	p, cleanup := cfg.PlanHookMaker(
		fmt.Sprintf("invoke-row-level-ttl-%d", args.TableID),
		txn,
		security.NodeUserName(),
	)
	defer cleanup()

	if _, err := createRowLevelTTLJob(
		ctx,
		&jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		},
		txn,
		cfg.InternalExecutor,
		p.(*planner).ExecCfg().JobRegistry,
		*args,
	); err != nil {
		s.metrics.NumFailed.Inc(1)
		return err
	}
	s.metrics.NumStarted.Inc(1)
	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) NotifyJobTermination(
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
		jobs.DefaultHandleFailedRun(
			sj,
			"row level ttl for table [%d] job failed",
			details.(ttlpb.ScheduledRowLevelTTLArgs).TableID,
		)
		s.metrics.NumFailed.Inc(1)
		return nil
	}

	if jobStatus == jobs.StatusSucceeded {
		s.metrics.NumSucceeded.Inc(1)
	}

	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

// Metrics implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) Metrics() metric.Struct {
	return &s.metrics
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	args := &ttlpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return "", err
	}

	// TODO(#75428): consider using table name instead - we would need to pass in descCol from the planner.
	return fmt.Sprintf("ALTER TABLE [%d as T] WITH (expire_after = ...)", args.TableID), nil
}

func createRowLevelTTLJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	jobRegistry *jobs.Registry,
	ttlDetails ttlpb.ScheduledRowLevelTTLArgs,
) (jobspb.JobID, error) {
	record := jobs.Record{
		Description: "ttl",
		Username:    security.NodeUserName(),
		Details: jobspb.RowLevelTTLDetails{
			TableID: ttlDetails.TableID,
		},
		Progress:  jobspb.RowLevelTTLProgress{},
		CreatedBy: createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeRowLevelTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &rowLevelTTLResumer{
			job: job,
			st:  settings,
		}
	})

	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledRowLevelTTLExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledRowLevelTTLExecutor.InternalName())
			return &rowLevelTTLExecutor{
				metrics: rowLevelTTLMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		},
	)
}
