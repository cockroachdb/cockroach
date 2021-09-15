// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/ttlpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type ttlResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*ttlResumer)(nil)

func (t ttlResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor
	db := p.ExecCfg().DB
	details := t.job.Details().(jobspb.TTLDetails)
	cn := tree.Name(details.ColumnName)
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		numAffected := 1
		for numAffected > 0 {
			// TODO(XXX): prevent full table scans?
			var err error
			numAffected, err = ie.Exec(
				ctx,
				"ttl_delete",
				txn,
				fmt.Sprintf("DELETE FROM [%d AS t] WHERE %s < now() LIMIT %d", details.TableID, cn.String(), 1000),
			)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (t ttlResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	// TODO(XXX): what do we do here?
	return nil
}

// NewTTLScheduledJob XXX.
// TODO(XXX): should this be per table, or global?
func NewTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv, owner security.SQLUsername, tblID descpb.ID, ttlColumn string,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(fmt.Sprintf("TTL %d", tblID))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		// TODO(XXX): is this what we want?
		Wait:    jobspb.ScheduleDetails_WAIT,
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})
	// Let's do minutely!
	// TODO(XXX): allow user to configure schedule.
	if err := sj.SetSchedule("* * * * *"); err != nil {
		return nil, err
	}
	args := &ttlpb.TTLDetails{
		TableID:   tblID,
		TTLColumn: ttlColumn,
	}
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	// TODO(XXX): when should the first run be?
	sj.SetNextRun(env.Now().Add(time.Second * 10))
	sj.SetExecutionDetails(
		tree.ScheduledTTLExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)
	return sj, nil
}

type scheduledTTLExecutor struct {
	metrics ttlMetrics
}

func (s scheduledTTLExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	return errors.Newf("XXX undroppable")
}

type ttlMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &ttlMetrics{}

// MetricStruct implements metric.Struct interface.
func (m *ttlMetrics) MetricStruct() {}

func (s scheduledTTLExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	p, cleanup := cfg.PlanHookMaker("invoke-sql-stats-compact", txn, security.NodeUserName())
	defer cleanup()

	args := &ttlpb.TTLDetails{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	_, err := CreateTTLJob(
		ctx,
		&jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		},
		txn,
		cfg.InternalExecutor,
		p.(*planner).ExecCfg().JobRegistry,
		*args,
	)
	return err
}

func CreateTTLJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	jobRegistry *jobs.Registry,
	ttlDetails ttlpb.TTLDetails,
) (jobspb.JobID, error) {
	// TODO(XXX): check existing job.
	/*
		if err := CheckExistingCompactionJob(ctx, nil, ie, txn); err != nil {
			return jobspb.InvalidJobID, err
		}
	*/
	record := jobs.Record{
		Description: "ttl",
		Username:    security.NodeUserName(),
		// TODO(XXX): unify details
		Details: jobspb.TTLDetails{
			TableID:    ttlDetails.TableID,
			ColumnName: ttlDetails.TTLColumn,
		},
		Progress:  jobspb.TTLProgress{},
		CreatedBy: createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

func (s scheduledTTLExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

func (s scheduledTTLExecutor) Metrics() metric.Struct {
	return &s.metrics
}

func (s scheduledTTLExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	return "IMPLEMENT ME XXX", nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &ttlResumer{
			job: job,
			st:  settings,
		}
	})

	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledTTLExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledTTLExecutor.InternalName())
			return &scheduledTTLExecutor{
				metrics: ttlMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		})
}
