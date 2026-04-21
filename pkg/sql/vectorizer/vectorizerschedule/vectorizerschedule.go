// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vectorizerschedule

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	pbtypes "github.com/gogo/protobuf/types"
)

type vectorizerExecutor struct {
	metrics vectorizerMetrics
}

var _ jobs.ScheduledJobController = (*vectorizerExecutor)(nil)

type vectorizerMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &vectorizerMetrics{}

func (m *vectorizerMetrics) MetricStruct() {}

// OnDrop implements the jobs.ScheduledJobController interface.
func (s vectorizerExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn isql.Txn,
	descsCol *descs.Collection,
) (int, error) {
	// Allow dropping vectorizer schedules. The schedule is cleaned up when
	// DROP VECTORIZER runs.
	return 0, nil
}

// ExecuteJob implements the jobs.ScheduledJobExecutor interface.
func (s vectorizerExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	args := &catpb.ScheduledVectorizerArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	p, cleanup := cfg.PlanHookMaker(
		ctx,
		redact.SafeString(fmt.Sprintf("invoke-vectorizer-%d", args.TableID)),
		txn.KV(),
		sj.Owner(),
	)
	defer cleanup()

	execCfg := p.(sql.PlanHookState).ExecCfg()

	// Look up the table descriptor to get version info.
	descsCol := descs.FromTxn(txn)
	tableDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, args.TableID)
	if err != nil {
		s.metrics.NumFailed.Inc(1)
		return errors.Wrapf(err, "vectorizer: resolving table %d", args.TableID)
	}

	record := jobs.Record{
		Description: fmt.Sprintf(
			"vectorizer for table %s [%d]",
			tableDesc.GetName(), args.TableID,
		),
		Username: sj.Owner(),
		Details: jobspb.VectorizerDetails{
			TableID:      args.TableID,
			TableVersion: tableDesc.GetVersion(),
		},
		Progress: jobspb.VectorizerProgress{},
		CreatedBy: &jobs.CreatedByInfo{
			ID:   int64(sj.ScheduleID()),
			Name: jobs.CreatedByScheduledJobs,
		},
	}

	jobID := execCfg.JobRegistry.MakeJobID()
	if _, err := execCfg.JobRegistry.CreateAdoptableJobWithTxn(
		ctx, record, jobID, txn,
	); err != nil {
		s.metrics.NumFailed.Inc(1)
		return err
	}
	s.metrics.NumStarted.Inc(1)
	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobExecutor interface.
func (s vectorizerExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobStatus jobs.State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	if jobStatus == jobs.StateFailed {
		jobs.DefaultHandleFailedRun(
			sj,
			"vectorizer for table [%d] job failed",
			details.(jobspb.VectorizerDetails).TableID,
		)
		s.metrics.NumFailed.Inc(1)
		return nil
	}
	if jobStatus == jobs.StateSucceeded {
		s.metrics.NumSucceeded.Inc(1)
	}
	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

// Metrics implements the jobs.ScheduledJobExecutor interface.
func (s vectorizerExecutor) Metrics() metric.Struct {
	return &s.metrics
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobExecutor interface.
func (s vectorizerExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *jobs.ScheduledJob,
) (string, error) {
	args := &catpb.ScheduledVectorizerArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", err
	}
	descsCol := descs.FromTxn(txn)
	tbl, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, args.TableID)
	if err != nil {
		return "", err
	}
	tn, err := descs.GetObjectName(ctx, txn.KV(), descsCol, tbl)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CREATE VECTORIZER ON %s ...", tn.FQString()), nil
}

func init() {
	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledVectorizerExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(
				tree.ScheduledVectorizerExecutor.InternalName(),
			)
			return &vectorizerExecutor{
				metrics: vectorizerMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		},
	)
}
