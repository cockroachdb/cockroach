// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type scheduledExportMetrics struct {
	*jobs.ExecutorMetrics
}

type scheduledExportExecutor struct {
	metrics scheduledExportMetrics
}

var _ jobs.ScheduledJobExecutor = (*scheduledExportExecutor)(nil)

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (s *scheduledExportExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	if err := s.executeExport(ctx, cfg, sj, txn); err != nil {
		s.metrics.NumFailed.Inc(1)
		return err
	}
	s.metrics.NumStarted.Inc(1)
	return nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (s *scheduledExportExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if jobStatus == jobs.StatusSucceeded {
		s.metrics.NumSucceeded.Inc(1)
		log.Infof(ctx, "export job %d scheduled by %d succeeded", jobID, schedule.ScheduleID())
		return nil
	}

	s.metrics.NumFailed.Inc(1)
	err := errors.Errorf(
		"export job %d scheduled by %d failed with status %s",
		jobID, schedule.ScheduleID(), jobStatus)
	log.Errorf(ctx, "export error: %v	", err)
	jobs.DefaultHandleFailedRun(schedule, "export job %d failed with err=%v", jobID, err)
	return nil
}

// Metrics implements jobs.ScheduledJobExecutor interface.
func (s *scheduledExportExecutor) Metrics() metric.Struct {
	return s.metrics
}

// GetCreateScheduleStatement implements jobs.ScheduledJobExecutor interface.
func (s *scheduledExportExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	exportNode, err := extractExportStatement(sj)
	if err != nil {
		return "", err
	}

	args := &changefeedpb.ScheduledExportExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", errors.Wrap(err, "un-marshaling args")
	}

	firstRunTime := sj.ScheduledRunTime()
	if firstRunTime.IsZero() {
		firstRunTime = env.Now()
	}
	opts := schedulebase.CommonScheduleOptions{
		FirstRun: firstRunTime,
		OnError:  sj.ScheduleDetails().OnError,
		Wait:     sj.ScheduleDetails().Wait,
	}

	scheduleOptions, err := opts.KVOptions()
	if err != nil {
		return "", err
	}

	node := &tree.ScheduledExport{
		CreateChangefeed: *exportNode.CreateChangefeed,
		ScheduleLabelSpec: tree.ScheduleLabelSpec{
			IfNotExists: false,
			Label:       tree.NewStrVal(sj.ScheduleLabel()),
		},
		Recurrence:      tree.NewStrVal(sj.ScheduleExpr()),
		ScheduleOptions: scheduleOptions,
	}

	return tree.AsString(node), nil
}

// executeExport runs the export table.
func (s *scheduledExportExecutor) executeExport(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	exportStmt, err := extractExportStatement(sj)
	if err != nil {
		return err
	}

	// Sanity check: can't execute core changefeeds from schedule.
	if exportStmt.SinkURI == nil {
		return errors.AssertionFailedf("cannot execute core changefeed from schedule")
	}

	// Sanity check: make sure the schedule is not paused so that
	// we don't set end time to 0 (this shouldn't happen since job scheduler
	// ignores paused schedules).
	if sj.IsPaused() {
		return errors.AssertionFailedf("scheduled unexpectedly paused")
	}

	// Set endTime (AsOf) to be the time this schedule was supposed to have run.
	endTime, err := tree.MakeDTimestampTZ(sj.ScheduledRunTime(), time.Microsecond)
	if err != nil {
		return err
	}
	exportStmt.ExportSpec.AsOf = tree.AsOfClause{Expr: endTime}

	if knobs, ok := cfg.TestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.OverrideAsOfClause != nil {
			knobs.OverrideAsOfClause(&exportStmt.ExportSpec.AsOf)
		}
	}

	log.Infof(ctx, "Starting scheduled export %d: %s",
		sj.ScheduleID(), tree.AsString(exportStmt))

	// Invoke changefeed plan hook.
	hook, cleanup := cfg.PlanHookMaker("exec-export", txn, sj.Owner())
	defer cleanup()
	exportFn, err := planExport(ctx, hook.(sql.PlanHookState), exportStmt)
	if err != nil {
		return err
	}
	return invokeExport(ctx, exportFn)
}

// dryRunExport executes export table in dry-run mode: we simply execute export
// under transaction savepoint, and then rollback to that save point.
func dryRunExport(
	ctx context.Context, p sql.PlanHookState, scheduleID int64, exportNode *tree.CreateChangefeed,
) error {
	sp, err := p.ExtendedEvalContext().Txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}

	err = func() error {
		annotated := annotatedChangefeedStatement{
			CreateChangefeed: exportNode,
			CreatedByInfo: &jobs.CreatedByInfo{
				Name: jobs.CreatedByScheduledJobs,
				ID:   scheduleID,
			},
		}
		exportFn, err := planExport(ctx, p, &annotated)
		if err != nil {
			return err
		}
		return invokeExport(ctx, exportFn)
	}()

	if rollbackErr := p.ExtendedEvalContext().Txn.RollbackToSavepoint(ctx, sp); rollbackErr != nil {
		return rollbackErr
	}
	return err
}

func planExport(
	ctx context.Context, p sql.PlanHookState, exportStmt tree.Statement,
) (sql.PlanHookRowFn, error) {
	fn, cols, _, _, err := changefeedPlanHook(ctx, exportStmt, p)

	if err != nil {
		return nil, errors.Wrapf(err, "export table eval: %q", tree.AsString(exportStmt))
	}
	if fn == nil {
		return nil, errors.Newf("export table eval: %q", tree.AsString(exportStmt))
	}
	if len(cols) != 1 {
		return nil, errors.Newf("unexpected result columns")
	}
	return fn, nil
}

func invokeExport(ctx context.Context, exportFn sql.PlanHookRowFn) error {
	resultCh := make(chan tree.Datums) // No need to close
	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		select {
		case <-resultCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		return exportFn(ctx, nil, resultCh)
	})

	return g.Wait()
}

// extractExportStatement returns tree.CreateChangefeed node encoded inside scheduled job.
func extractExportStatement(sj *jobs.ScheduledJob) (*annotatedChangefeedStatement, error) {
	args := &changefeedpb.ScheduledExportExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.ExportStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing export statement")
	}

	if exportStmt, ok := node.AST.(*tree.CreateChangefeed); ok {
		if exportStmt.IsExportTable() {
			return &annotatedChangefeedStatement{
				CreateChangefeed: exportStmt,
				CreatedByInfo: &jobs.CreatedByInfo{
					Name: jobs.CreatedByScheduledJobs,
					ID:   sj.ScheduleID(),
				},
			}, nil
		}
		return nil, errors.AssertionFailedf(
			"expected EXPORT changefeed, found regular changefeed: %s", tree.AsString(exportStmt))
	}

	return nil, errors.AssertionFailedf("unexpect node type %T", node)
}

// scheduledExportEval is a representation of tree.ScheduledExport, prepared
// for evaluation
type scheduledExportEval struct {
	*tree.ScheduledExport

	isEnterpriseUser bool

	// Schedule specific properties that get evaluated.
	scheduleLabel func() (string, error)
	recurrence    func() (string, error)

	// CommonScheduleOptions initialized after setScheduleOptions execution.
	schedulebase.CommonScheduleOptions
	setScheduleOptions func(evalCtx *tree.EvalContext) error

	// Export specific properties that get evaluated.
	// We need to evaluate anything in the tree.CreateChangefeed node that allows
	// placeholders to be specified so that we store evaluated
	// changefeed statement in the schedule.
	sinkURI func() (string, error)
}

const scheduleExportOp = "CREATE SCHEDULE FOR EXPORT"

// makeScheduleExportEval prepares helper scheduledExportEval struct to assist in evaluation
// of various schedule and changefeed specific components.
func makeScheduledExportEval(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledExport,
) (*scheduledExportEval, error) {
	eval := &scheduledExportEval{ScheduledExport: schedule}
	var err error

	if schedule.ScheduleLabelSpec.Label != nil {
		eval.scheduleLabel, err = p.TypeAsString(ctx, schedule.ScheduleLabelSpec.Label, scheduleExportOp)
		if err != nil {
			return nil, err
		}
	}

	if schedule.Recurrence == nil {
		// Sanity check: recurrence must be specified.
		return nil, errors.New("RECURRING clause required")
	}

	eval.recurrence, err = p.TypeAsString(ctx, schedule.Recurrence, scheduleExportOp)
	if err != nil {
		return nil, err
	}

	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
		"EXPORT TABLE")
	eval.isEnterpriseUser = enterpriseCheckErr == nil

	if !eval.isEnterpriseUser {
		// Cannot use EXPORT TABLE w/out enterprise license.
		// TODO(yevgeniy): Consider allowing cloudstorage sinks.
		return nil, enterpriseCheckErr
	}

	eval.setScheduleOptions, err = schedulebase.MakeScheduleOptionsEval(
		ctx, p, schedule.ScheduleOptions, &eval.CommonScheduleOptions)
	if err != nil {
		return nil, err
	}

	eval.sinkURI, err = p.TypeAsString(ctx, schedule.SinkURI, scheduleExportOp)
	if err != nil {
		return nil, err
	}

	return eval, nil
}

// doCreateExportSchedules creates requested schedule.
// It is a plan hook implementation responsible for the creating of scheduled export.
func doCreateExportSchedule(
	ctx context.Context, p sql.PlanHookState, eval *scheduledExportEval, resultsCh chan<- tree.Datums,
) error {
	if err := p.RequireAdminRole(ctx, scheduleExportOp); err != nil {
		return err
	}

	if eval.ExportSpec.AsOf.Expr != nil {
		p.BufferClientNotice(ctx,
			pgnotice.Newf("AS OF SYSTEM TIME clause ignored when creating export schedules"))
	}

	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	var scheduleLabel string
	if eval.scheduleLabel != nil {
		label, err := eval.scheduleLabel()
		if err != nil {
			return err
		}
		scheduleLabel = label
		if eval.ScheduleLabelSpec.IfNotExists {
			exists, err := schedulebase.CheckScheduleAlreadyExists(
				ctx, env, p.ExecCfg().InternalExecutor, scheduleLabel, p.ExtendedEvalContext().Txn)
			if err != nil {
				return err
			}

			if exists {
				p.BufferClientNotice(ctx,
					pgnotice.Newf("schedule %q already exists, skipping", scheduleLabel),
				)
				return nil
			}
		}
	} else {
		scheduleLabel = fmt.Sprintf("EXPORT %d", env.Now().Unix())
	}

	recurrence, err := schedulebase.EvalScheduleRecurrence(env.Now(), eval.recurrence)
	if err != nil {
		return err
	}

	// Sanity check.
	if recurrence.IsZero() {
		return errors.AssertionFailedf(" export recurrence should be set")
	}

	// Prepare export statement.
	// We do not set exportNode.AsOf: this is done when the scheduler kicks off the export.
	resolvedTargets, err := resolveTargetsForExportSchedule(ctx, p, eval.Targets)
	if err != nil {
		return err
	}

	// sinkURI must be evaluated in case query had placeholders.
	sinkURI, err := eval.sinkURI()
	if err != nil {
		return err
	}

	exportNode := &tree.CreateChangefeed{
		Targets:    resolvedTargets,
		SinkURI:    tree.NewStrVal(sinkURI),
		Options:    eval.Options,
		ExportSpec: &tree.ChangefeedExportSpec{},
	}

	if err := eval.setScheduleOptions(&p.ExtendedEvalContext().EvalContext); err != nil {
		return err
	}

	details := jobspb.ScheduleDetails{
		Wait:    eval.Wait,
		OnError: eval.OnError,
	}

	es, err := makeExportSchedule(env, p.User(), scheduleLabel, eval.FirstRun, recurrence, details, exportNode)
	if err != nil {
		return err
	}

	if err := es.Create(ctx, p.ExecCfg().InternalExecutor, p.ExtendedEvalContext().Txn); err != nil {
		return err
	}

	// Run export in dry-run mode.  This will do all the sanity checks
	// and validation we need to make in order to ensure the schedule is sane.
	if err := dryRunExport(ctx, p, es.ScheduleID(), exportNode); err != nil {
		return errors.Wrapf(err, "failed to dry run export")
	}

	if err := emitSchedule(es, exportNode, sinkURI, resultsCh); err != nil {
		return err
	}

	collectScheduledExportTelemetry(details)
	return nil
}

func emitSchedule(
	sj *jobs.ScheduledJob,
	exportNode *tree.CreateChangefeed,
	sinkURI string,
	resultsCh chan<- tree.Datums,
) error {
	redactedExportNode, err := getRedactedExportNode(exportNode, sinkURI)
	if err != nil {
		return err
	}
	return schedulebase.EmitSchedule(sj, redactedExportNode, resultsCh)
}

// resolveTargetsForExportSchedules resolves all targets to their fully qualified
// name.
func resolveTargetsForExportSchedule(
	ctx context.Context, p sql.PlanHookState, targets tree.TargetList,
) (tree.TargetList, error) {
	if err := verifyChangefeedTargets(targets); err != nil {
		return tree.TargetList{}, err
	}
	var res tree.TargetList
	if err := resolveChangefeedTargets(
		ctx, p, targets, p.ExecCfg().Clock.Now(),
		func(table catalog.TableDescriptor) error {
			name, err := getQualifiedTableName(ctx, *p.ExecCfg(), p.ExtendedEvalContext().Txn, table)
			if err != nil {
				return err
			}
			res.Tables = append(res.Tables, name)
			return nil
		},
	); err != nil {
		return tree.TargetList{}, err
	}

	return res, nil
}

// getRedactedExportNode returns a copy of the argument `export`, but with all
// the secret information redacted.
func getRedactedExportNode(
	export *tree.CreateChangefeed, sinkURI string,
) (*tree.CreateChangefeed, error) {
	e := &tree.CreateChangefeed{
		Targets:    export.Targets,
		Options:    export.Options,
		ExportSpec: export.ExportSpec,
	}

	sanitizedSink, err := cloud.SanitizeExternalStorageURI(sinkURI, nil /* extraParams */)
	if err != nil {
		return nil, err
	}
	e.SinkURI = tree.NewStrVal(sanitizedSink)
	return e, nil
}

func collectScheduledExportTelemetry(details jobspb.ScheduleDetails) {
	telemetry.Count("scheduled-export.create.success")
	switch details.Wait {
	case jobspb.ScheduleDetails_WAIT:
		telemetry.Count("scheduled-export.wait-policy.wait")
	case jobspb.ScheduleDetails_NO_WAIT:
		telemetry.Count("scheduled-export.wait-policy.no-wait")
	case jobspb.ScheduleDetails_SKIP:
		telemetry.Count("scheduled-export.wait-policy.skip")
	}
	switch details.OnError {
	case jobspb.ScheduleDetails_RETRY_SCHED:
		telemetry.Count("scheduled-export.error-policy.retry-schedule")
	case jobspb.ScheduleDetails_RETRY_SOON:
		telemetry.Count("scheduled-export.error-policy.retry-soon")
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		telemetry.Count("scheduled-export.error-policy.pause-schedule")
	}
}

func makeExportSchedule(
	env scheduledjobs.JobSchedulerEnv,
	owner security.SQLUsername,
	label string,
	firstRun time.Time,
	recurrence schedulebase.ScheduleRecurrence,
	details jobspb.ScheduleDetails,
	exportNode *tree.CreateChangefeed,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(label)
	sj.SetOwner(owner)

	if err := sj.SetSchedule(recurrence.Cron); err != nil {
		return nil, err
	}

	if !firstRun.IsZero() {
		sj.SetNextRun(firstRun)
	}
	sj.SetScheduleDetails(details)

	var args changefeedpb.ScheduledExportExecutionArgs
	args.ExportStatement = tree.AsStringWithFlags(exportNode, tree.FmtParsable|tree.FmtShowPasswords)
	any, err := pbtypes.MarshalAny(&args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledExportExecutor.InternalName(), jobspb.ExecutionArguments{Args: any},
	)

	return sj, nil
}

func createExportScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledExport)
	if !ok {
		return nil, nil, nil, false, nil
	}

	eval, err := makeScheduledExportEval(ctx, p, schedule)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doCreateExportSchedule(ctx, p, eval, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-export.create.failed")
			return err
		}

		return nil
	}
	return fn, schedulebase.CreateScheduleHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook(createExportScheduleHook)

	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledExportExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledExportExecutor.UserName())
			return &scheduledExportExecutor{
				metrics: scheduledExportMetrics{&m},
			}, nil
		})
}
