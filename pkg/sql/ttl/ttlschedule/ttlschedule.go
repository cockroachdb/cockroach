// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttlschedule

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

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
	descsCol *descs.Collection,
) (int, error) {

	var args catpb.ScheduledRowLevelTTLArgs
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, &args); err != nil {
		return 0, err
	}

	canDrop, err := canDropTTLSchedule(ctx, txn, descsCol, schedule, args)
	if err != nil {
		return 0, err
	}

	if !canDrop {
		tbl, err := descsCol.GetImmutableTableByID(
			ctx, txn, args.TableID, tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return 0, err
		}
		tn, err := descs.GetObjectName(ctx, txn, descsCol, tbl)
		if err != nil {
			return 0, err
		}
		return 0, errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot drop a row level TTL schedule",
			),
			`use ALTER TABLE %s RESET (ttl) instead`,
			tn.FQString(),
		)
	}
	return 0, nil
}

// canDropTTLSchedule determines whether we can drop a given row-level TTL
// schedule. This is intended to only be permitted for schedules which are not
// valid.
func canDropTTLSchedule(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	schedule *jobs.ScheduledJob,
	args catpb.ScheduledRowLevelTTLArgs,
) (bool, error) {
	desc, err := descsCol.GetImmutableTableByID(ctx, txn, args.TableID, tree.ObjectLookupFlags{})
	if err != nil {
		// If the descriptor does not exist we can drop this schedule.
		if sqlerrors.IsUndefinedRelationError(err) {
			return true, nil
		}
		return false, err
	}
	if desc == nil {
		return true, nil
	}
	// If there is no row-level TTL on the table we can drop this schedule.
	if !desc.HasRowLevelTTL() {
		return true, nil
	}
	// If there is a schedule id mismatch we can drop this schedule.
	if desc.GetRowLevelTTL().ScheduleID != schedule.ScheduleID() {
		return true, nil
	}
	return false, nil
}

// ExecuteJob implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	args := &catpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	p, cleanup := cfg.PlanHookMaker(
		fmt.Sprintf("invoke-row-level-ttl-%d", args.TableID),
		txn,
		username.NodeUserName(),
	)
	defer cleanup()

	if _, err := createRowLevelTTLJob(
		ctx,
		&jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		},
		txn,
		p.(sql.PlanHookState).ExtendedEvalContext().Descs,
		p.(sql.PlanHookState).ExecCfg().JobRegistry,
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
			details.(catpb.ScheduledRowLevelTTLArgs).TableID,
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
	descsCol *descs.Collection,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	args := &catpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", err
	}
	tbl, err := descsCol.GetImmutableTableByID(
		ctx, txn, args.TableID, tree.ObjectLookupFlagsWithRequired(),
	)
	if err != nil {
		return "", err
	}
	tn, err := descs.GetObjectName(ctx, txn, descsCol, tbl)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s WITH (ttl = 'on', ...)`, tn.FQString()), nil
}

func makeTTLJobDescription(tableDesc catalog.TableDescriptor, tn tree.ObjectName) string {
	pkColumns := tableDesc.GetPrimaryIndex().IndexDesc().KeyColumnNames
	pkColumnNamesSQL := ttlbase.MakeColumnNamesSQL(pkColumns)
	selectQuery := fmt.Sprintf(
		ttlbase.SelectTemplate,
		pkColumnNamesSQL,
		tableDesc.GetID(),
		int64(ttlbase.DefaultAOSTDuration.Seconds()),
		"<crdb_internal_expiration OR ttl_expiration_expression>",
		fmt.Sprintf("AND (%s) > (<span start>)", pkColumnNamesSQL),
		fmt.Sprintf(" AND (%s) < (<span end>)", pkColumnNamesSQL),
		"<ttl_select_batch_size>",
	)
	deleteQuery := fmt.Sprintf(
		ttlbase.DeleteTemplate,
		tableDesc.GetID(),
		"<crdb_internal_expiration OR ttl_expiration_expression>",
		pkColumnNamesSQL,
		"<rows selected above>",
	)
	return fmt.Sprintf(`ttl for %s
-- for each span, iterate to find rows:
%s
-- then delete with:
%s`, tn.FQString(), selectQuery, deleteQuery)
}

func createRowLevelTTLJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	descsCol *descs.Collection,
	jobRegistry *jobs.Registry,
	ttlArgs catpb.ScheduledRowLevelTTLArgs,
) (jobspb.JobID, error) {
	tableDesc, err := descsCol.GetImmutableTableByID(ctx, txn, ttlArgs.TableID, tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return 0, err
	}
	tn, err := descs.GetObjectName(ctx, txn, descsCol, tableDesc)
	if err != nil {
		return 0, err
	}
	record := jobs.Record{
		Description: makeTTLJobDescription(tableDesc, tn),
		Username:    username.NodeUserName(),
		Details: jobspb.RowLevelTTLDetails{
			TableID:      ttlArgs.TableID,
			Cutoff:       timeutil.Now(),
			TableVersion: tableDesc.GetVersion(),
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
