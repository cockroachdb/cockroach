// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttlschedule

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	txn isql.Txn,
	descsCol *descs.Collection,
) (int, error) {

	var args catpb.ScheduledRowLevelTTLArgs
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, &args); err != nil {
		return 0, err
	}

	canDrop, err := canDropTTLSchedule(ctx, txn.KV(), descsCol, schedule, args)
	if err != nil {
		return 0, err
	}

	if !canDrop {
		tbl, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, args.TableID)
		if err != nil {
			return 0, err
		}
		tn, err := descs.GetObjectName(ctx, txn.KV(), descsCol, tbl)
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
	desc, err := descsCol.ByIDWithLeased(txn).WithoutNonPublic().Get().Table(ctx, args.TableID)
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
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	args := &catpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	// TODO(chrisseto): Should opName be updated to match schedule_name? We'll
	// have to query to resolve the table name or delegate to sj.ScheduleLabel,
	// which may make debugging quite confusing if the label gets out of whack.
	p, cleanup := cfg.PlanHookMaker(
		ctx,
		// TableID is not sensitive.
		redact.SafeString(fmt.Sprintf("invoke-row-level-ttl-%d", args.TableID)),
		txn.KV(),
		username.NodeUserName(),
	)
	defer cleanup()

	execCfg := p.(sql.PlanHookState).ExecCfg()
	if _, err := createRowLevelTTLJob(
		ctx,
		&jobs.CreatedByInfo{
			ID:   int64(sj.ScheduleID()),
			Name: jobs.CreatedByScheduledJobs,
		},
		txn,
		execCfg.JobRegistry,
		*args,
		execCfg.SV(),
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
			"row level ttl for table [%d] job failed",
			details.(catpb.ScheduledRowLevelTTLArgs).TableID,
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

// Metrics implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) Metrics() metric.Struct {
	return &s.metrics
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *jobs.ScheduledJob,
) (string, error) {
	descsCol := descs.FromTxn(txn)
	args := &catpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", err
	}
	tbl, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, args.TableID)
	if err != nil {
		return "", err
	}
	tn, err := descs.GetObjectName(ctx, txn.KV(), descsCol, tbl)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s WITH (ttl = 'on', ...)`, tn.FQString()), nil
}

func makeTTLJobDescription(
	tableDesc catalog.TableDescriptor, tn tree.ObjectName, sv *settings.Values,
) string {
	relationName := tn.FQString()
	pkIndex := tableDesc.GetPrimaryIndex().IndexDesc()
	pkColNames := pkIndex.KeyColumnNames
	pkColDirs := pkIndex.KeyColumnDirections
	rowLevelTTL := tableDesc.GetRowLevelTTL()
	ttlExpirationExpr := rowLevelTTL.GetTTLExpr()
	numPkCols := len(pkColNames)
	selectBatchSize := ttlbase.GetSelectBatchSize(sv, rowLevelTTL)
	selectQuery := ttlbase.BuildSelectQuery(
		relationName,
		pkColNames,
		pkColDirs,
		ttlbase.DefaultAOSTDuration,
		ttlExpirationExpr,
		numPkCols,
		numPkCols,
		selectBatchSize,
		true, /*startIncl*/
	)
	deleteQuery := ttlbase.BuildDeleteQuery(
		relationName,
		pkColNames,
		ttlExpirationExpr,
		1, /*numRows*/
	)
	return fmt.Sprintf(`ttl for %s
-- for each span, iterate to find rows:
%s
-- then delete with:
%s`, relationName, selectQuery, deleteQuery)
}

func createRowLevelTTLJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	ttlArgs catpb.ScheduledRowLevelTTLArgs,
	sv *settings.Values,
) (jobspb.JobID, error) {
	descsCol := descs.FromTxn(txn)
	tableID := ttlArgs.TableID
	tableDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
	if err != nil {
		return 0, err
	}
	tn, err := descs.GetObjectName(ctx, txn.KV(), descsCol, tableDesc)
	if err != nil {
		return 0, err
	}
	record := jobs.Record{
		Description: makeTTLJobDescription(tableDesc, tn, sv),
		Username:    username.NodeUserName(),
		Details: jobspb.RowLevelTTLDetails{
			TableID:      tableID,
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
