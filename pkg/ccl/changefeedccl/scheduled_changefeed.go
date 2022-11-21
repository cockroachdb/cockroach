// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const scheduleChangefeedOp = "CREATE SCHEDULE FOR CHANGEFEED"

const (
	optFirstRun          = "first_run"
	optOnExecFailure     = "on_execution_failure"
	optOnPreviousRunning = "on_previous_running"
)

var scheduledChangefeedOptionExpectValues = map[string]exprutil.KVStringOptValidate{
	optFirstRun:          exprutil.KVStringOptRequireValue,
	optOnExecFailure:     exprutil.KVStringOptRequireValue,
	optOnPreviousRunning: exprutil.KVStringOptRequireValue,
}

var scheduledChangefeedHeader = colinfo.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "label", Typ: types.String},
	{Name: "status", Typ: types.String},
	{Name: "first_run", Typ: types.TimestampTZ},
	{Name: "schedule", Typ: types.String},
	{Name: "changefeed_stmt", Typ: types.String},
}

type scheduledChangefeedMetrics struct {
	*jobs.ExecutorMetrics
}

type scheduledChangefeedExecutor struct {
	metrics scheduledChangefeedMetrics
}

var _ jobs.ScheduledJobExecutor = (*scheduledChangefeedExecutor)(nil)

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (s *scheduledChangefeedExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	if err := s.executeChangefeed(ctx, cfg, sj, txn); err != nil {
		s.metrics.NumFailed.Inc(1)
		return err
	}
	s.metrics.NumStarted.Inc(1)
	return nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (s *scheduledChangefeedExecutor) NotifyJobTermination(
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
		log.Infof(ctx, "changefeed job %d scheduled by %d succeeded", jobID, schedule.ScheduleID())
		return nil
	}

	s.metrics.NumFailed.Inc(1)
	err := errors.Errorf(
		"changefeed job %d scheduled by %d failed with status %s",
		jobID, schedule.ScheduleID(), jobStatus)
	log.Errorf(ctx, "changefeed error: %v	", err)
	jobs.DefaultHandleFailedRun(schedule, "changefeed job %d failed with err=%v", jobID, err)
	return nil
}

// Metrics implements jobs.ScheduledJobExecutor interface.
func (s *scheduledChangefeedExecutor) Metrics() metric.Struct {
	return s.metrics
}

// GetCreateScheduleStatement implements jobs.ScheduledJobExecutor interface.
func (s *scheduledChangefeedExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	descsCol *descs.Collection,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	changefeedNode, err := extractChangefeedStatement(sj)
	if err != nil {
		return "", err
	}

	args := &changefeedpb.ScheduledChangefeedExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", errors.Wrap(err, "un-marshaling args")
	}

	firstRunTime := sj.ScheduledRunTime()
	if firstRunTime.IsZero() {
		firstRunTime = env.Now()
	}
	firstRun, err := tree.MakeDTimestampTZ(firstRunTime, time.Microsecond)
	if err != nil {
		return "", err
	}

	wait, err := schedulebase.ParseOnPreviousRunningOption(sj.ScheduleDetails().Wait)
	if err != nil {
		return "", err
	}
	onError, err := schedulebase.ParseOnErrorOption(sj.ScheduleDetails().OnError)
	if err != nil {
		return "", err
	}

	scheduleOptions := tree.KVOptions{
		tree.KVOption{
			Key:   optOnExecFailure,
			Value: tree.NewDString(onError),
		},
		tree.KVOption{
			Key:   optOnPreviousRunning,
			Value: tree.NewDString(wait),
		},
		tree.KVOption{
			Key:   optFirstRun,
			Value: firstRun,
		},
	}

	node := &tree.ScheduledChangefeed{
		CreateChangefeed: changefeedNode.CreateChangefeed,
		ScheduleLabelSpec: tree.LabelSpec{
			IfNotExists: false,
			Label:       tree.NewStrVal(sj.ScheduleLabel()),
		},
		Recurrence:      tree.NewStrVal(sj.ScheduleExpr()),
		ScheduleOptions: scheduleOptions,
	}

	return tree.AsString(node), nil
}

// executeChangefeed runs the changefeed.
func (s *scheduledChangefeedExecutor) executeChangefeed(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	changefeedStmt, err := extractChangefeedStatement(sj)
	if err != nil {
		return err
	}

	// Sanity check: can't execute core changefeeds from schedule.
	if changefeedStmt.SinkURI == nil {
		return errors.AssertionFailedf("cannot execute core changefeed from schedule")
	}

	// Sanity check: make sure the schedule is not paused so that
	// we don't set end time to 0 (this shouldn't happen since job scheduler
	// ignores paused schedules).
	if sj.IsPaused() {
		return errors.AssertionFailedf("scheduled unexpectedly paused")
	}

	// Revisit: right now we force initial scan only. if we want to support
	// incremental changefeeds, that will not work. will probably need ot use a
	// combination of cursor and end time for that.

	//endTime, err := tree.MakeDTimestampTZ(sj.ScheduledRunTime(), time.Microsecond)
	//if err != nil {
	//	return err
	//}
	//changefeedStmt.ExportSpec.AsOf = tree.AsOfClause{Expr: endTime}

	//if knobs, ok := cfg.TestingKnobs.(*jobs.TestingKnobs); ok {
	//	if knobs.OverrideAsOfClause != nil {
	//		knobs.OverrideAsOfClause(&exportStmt.ExportSpec.AsOf)
	//	}
	//}

	log.Infof(ctx, "Starting scheduled changefeed %d: %s",
		sj.ScheduleID(), tree.AsString(changefeedStmt))

	// Invoke changefeed plan hook.
	hook, cleanup := cfg.PlanHookMaker("exec-changefeed", txn, sj.Owner())
	defer cleanup()
	changefeedFn, err := planCreateChangefeed(ctx, hook.(sql.PlanHookState), changefeedStmt)
	if err != nil {
		return err
	}
	return invokeCreateChangefeed(ctx, changefeedFn)
}

// extractChangefeedStatement returns tree.CreateChangefeed node encoded inside scheduled job.
func extractChangefeedStatement(sj *jobs.ScheduledJob) (*annotatedChangefeedStatement, error) {
	args := &changefeedpb.ScheduledChangefeedExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.ScheduledChangefeedStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing export statement")
	}

	if stmt, ok := node.AST.(*tree.CreateChangefeed); ok {

		return &annotatedChangefeedStatement{
			CreateChangefeed: stmt,
			CreatedByInfo: &jobs.CreatedByInfo{
				Name: jobs.CreatedByScheduledJobs,
				ID:   sj.ScheduleID(),
			},
		}, nil

		// Revisit: should i make sure only initial scan is set?
		// but i already do that when creating the schedule...
	}

	return nil, errors.AssertionFailedf("unexpect node type %T", node)
}

// scheduledChangefeed is a representation of tree.ScheduledChangefeed, prepared
// for evaluation
type scheduledChangefeedSpec struct {
	*tree.ScheduledChangefeed

	isEnterpriseUser bool

	// Schedule specific properties that get evaluated.
	scheduleLabel *string
	recurrence    *string
	scheduleOpts  map[string]string

	// Changefeed specific properties that get evaluated.
	createChangefeedOptions map[string]string
	evaluatedSinkURI        *string
}

func scheduleFirstRun(evalCtx *eval.Context, opts map[string]string) (*time.Time, error) {
	if v, ok := opts[optFirstRun]; ok {
		firstRun, _, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
		if err != nil {
			return nil, err
		}
		return &firstRun.Time, nil
	}
	return nil, nil
}

// fullyQualifyTargetTables fully qualifies target tables.
// Table changefeed targets must be fully qualified during scheduled changefeed
// planning. This is because the actual execution of the changefeed job occurs
// in a background, scheduled job session, that does not have the same
// resolution configuration as during planning.
func fullyQualifyTargetTables(
	ctx context.Context, p sql.PlanHookState, tables tree.TablePatterns,
) ([]tree.TablePattern, error) {
	fqTablePatterns := make([]tree.TablePattern, len(tables))
	for i, target := range tables {
		tablePattern, err := target.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		switch tp := tablePattern.(type) {
		case *tree.TableName:
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn,
				col *descs.Collection) error {
				// Resolve the table.
				un := tp.ToUnresolvedObjectName()
				found, _, tableDesc, err := resolver.ResolveExisting(ctx, un, p, tree.ObjectLookupFlags{},
					p.CurrentDatabase(), p.CurrentSearchPath())
				if err != nil {
					return err
				}
				if !found {
					return errors.Newf("target table %s could not be resolved", tp.String())
				}

				// Resolve the database.
				found, dbDesc, err := col.GetImmutableDatabaseByID(ctx, txn, tableDesc.GetParentID(),
					tree.DatabaseLookupFlags{Required: true})
				if err != nil {
					return err
				}
				if !found {
					return errors.Newf("database of target table %s could not be resolved", tp.String())
				}

				// Resolve the schema.
				schemaDesc, err := col.GetImmutableSchemaByID(ctx, txn, tableDesc.GetParentSchemaID(),
					tree.SchemaLookupFlags{Required: true})
				if err != nil {
					return err
				}
				tn := tree.NewTableNameWithSchema(
					tree.Name(dbDesc.GetName()),
					tree.Name(schemaDesc.GetName()),
					tree.Name(tableDesc.GetName()),
				)
				fqTablePatterns[i] = tn
				return nil
			}); err != nil {
				return nil, err
			}
		case *tree.AllTablesSelector:
			if !tp.ExplicitSchema {
				tp.ExplicitSchema = true
				tp.SchemaName = tree.Name(p.CurrentDatabase())
			} else if tp.ExplicitSchema && !tp.ExplicitCatalog {
				// The schema field could either be a schema or a database. If we can
				// successfully resolve the schema, we will add the DATABASE prefix.
				// Otherwise, no updates are needed since the schema field refers to the
				// database.
				var schemaID descpb.ID
				if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
					flags := tree.DatabaseLookupFlags{Required: true}
					dbDesc, err := col.GetImmutableDatabaseByName(ctx, txn, p.CurrentDatabase(), flags)
					if err != nil {
						return err
					}
					schemaID, err = col.Direct().ResolveSchemaID(ctx, txn, dbDesc.GetID(), tp.SchemaName.String())
					return err
				}); err != nil {
					return nil, err
				}

				if schemaID != descpb.InvalidID {
					tp.ExplicitCatalog = true
					tp.CatalogName = tree.Name(p.CurrentDatabase())
				}
			}
			fqTablePatterns[i] = tp
		}
	}
	return fqTablePatterns, nil
}

// makeScheduleChangefeedSpec prepares helper scheduledChangefeedSpec struct to assist in evaluation
// of various schedule and changefeed specific components.
func makeScheduledChangefeedSpec(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledChangefeed,
) (*scheduledChangefeedSpec, error) {
	exprEval := p.ExprEvaluator(scheduleChangefeedOp)
	var err error

	tablePatterns := make([]tree.TablePattern, 0)
	for _, target := range schedule.Targets {
		tablePatterns = append(tablePatterns, target.TableName)
	}

	qualifiedTablePatters, err := fullyQualifyTargetTables(ctx, p, tablePatterns)
	if err != nil {
		return nil, errors.Wrap(err, "qualifying target tables")
	}

	newTargets := make([]tree.ChangefeedTarget, 0)
	for i, table := range qualifiedTablePatters {
		newTargets = append(newTargets, tree.ChangefeedTarget{TableName: table, FamilyName: schedule.Targets[i].FamilyName})
	}

	schedule.Targets = newTargets

	// We need to change the TableExpr inside the Select clause to be fully
	// qualified. Otherwise, when we execute the schedule, we will parse the
	// serliazed tree.CreateChangefeed, which will not have the Targets
	// information (because when serializing we do not print the Targets if Select
	// clause is present). Therefore, when parsing, we try to construct the
	// ChangefeedTargets using TableExpr from Select clause, which has to be fully
	// qualified.
	if schedule.Select != nil {
		tableExprs := make(tree.TableExprs, 1)
		// For cdc transformations, we expect only 1 table. Therefore, we can always
		// expect the size of qualifiedTablePatters to be 1 - which is why we can
		// directly index with 0.
		// We can directly typecast tree.TablePatter to tree.TableExpr without
		// checking for error because when parsing the statement we typecast
		// tree.TableExpr (from Select clause) into tree.ChangefeedTarget. If that
		// typecasting was successful, it is guaranteed that the reverse should work
		// without any errors.
		tableExprs[0] = qualifiedTablePatters[0].(tree.TableExpr)
		schedule.Select.From.Tables = tableExprs
	}

	spec := &scheduledChangefeedSpec{ScheduledChangefeed: schedule}
	if schedule.ScheduleLabelSpec.Label != nil {
		label, err := exprEval.String(ctx, schedule.ScheduleLabelSpec.Label)
		if err != nil {
			return nil, err
		}
		spec.scheduleLabel = &label
	}

	if schedule.Recurrence == nil {
		// Sanity check: recurrence must be specified.
		return nil, errors.New("RECURRING clause required")
	}
	{
		rec, err := exprEval.String(ctx, schedule.Recurrence)
		if err != nil {
			return nil, err
		}
		spec.recurrence = &rec
	}

	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
		scheduleChangefeedOp)
	spec.isEnterpriseUser = enterpriseCheckErr == nil

	if !spec.isEnterpriseUser {
		// Cannot use SCHEDULED CHANGEFEED w/out enterprise license.
		// TODO(yevgeniy): Consider allowing cloudstorage sinks.
		return nil, enterpriseCheckErr
	}

	spec.scheduleOpts, err = exprEval.KVOptions(
		ctx, schedule.ScheduleOptions, scheduledChangefeedOptionExpectValues,
	)
	if err != nil {
		return nil, err
	}

	spec.createChangefeedOptions, err = exprEval.KVOptions(
		ctx, schedule.CreateChangefeed.Options, changefeedvalidators.CreateOptionValidations,
	)
	if err != nil {
		return nil, err
	}

	evaluatedSinkURI, err := exprEval.String(ctx, schedule.SinkURI)
	if err != nil {
		return nil, err
	}
	spec.evaluatedSinkURI = &evaluatedSinkURI

	return spec, nil
}

func makeChangefeedSchedule(
	env scheduledjobs.JobSchedulerEnv,
	owner username.SQLUsername,
	label string,
	recurrence *schedulebase.ScheduleRecurrence,
	details jobspb.ScheduleDetails,
	changefeedNode *tree.CreateChangefeed,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(label)
	sj.SetOwner(owner)

	if err := sj.SetSchedule(recurrence.Cron); err != nil {
		return nil, err
	}

	sj.SetScheduleDetails(details)

	var args changefeedpb.ScheduledChangefeedExecutionArgs
	args.ScheduledChangefeedStatement = tree.AsStringWithFlags(changefeedNode, tree.FmtParsable|tree.FmtShowPasswords)
	any, err := pbtypes.MarshalAny(&args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledChangefeedExecutor.InternalName(), jobspb.ExecutionArguments{Args: any},
	)

	return sj, nil
}

// dryRunChangefeed executes Changefeed in dry-run mode: we simply execute Changefeed
// under transaction savepoint, and then rollback to that save point.
func dryRunCreateChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	scheduleID int64,
	createChangefeedNode *tree.CreateChangefeed,
) error {
	sp, err := p.ExtendedEvalContext().Txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}

	err = func() error {
		annotated := &annotatedChangefeedStatement{
			CreateChangefeed: createChangefeedNode,
			CreatedByInfo: &jobs.CreatedByInfo{
				Name: jobs.CreatedByScheduledJobs,
				ID:   scheduleID,
			},
		}
		changefeedFn, err := planCreateChangefeed(ctx, p, annotated)
		if err != nil {
			return err
		}
		return invokeCreateChangefeed(ctx, changefeedFn)
	}()

	if rollbackErr := p.ExtendedEvalContext().Txn.RollbackToSavepoint(ctx, sp); rollbackErr != nil {
		return rollbackErr
	}
	return err
}

func planCreateChangefeed(
	ctx context.Context, p sql.PlanHookState, createChangefeedStmt tree.Statement,
) (sql.PlanHookRowFn, error) {
	fn, cols, _, _, err := changefeedPlanHook(ctx, createChangefeedStmt, p)

	if err != nil {
		return nil, errors.Wrapf(err, "changefeed table eval: %q", tree.AsString(createChangefeedStmt))
	}
	if fn == nil {
		return nil, errors.Newf("changefeed table eval: %q", tree.AsString(createChangefeedStmt))
	}
	if len(cols) != 1 {
		return nil, errors.Newf("unexpected result columns")
	}
	return fn, nil
}

func invokeCreateChangefeed(ctx context.Context, createChangefeedFn sql.PlanHookRowFn) error {
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
		return createChangefeedFn(ctx, nil, resultCh)
	})

	return g.Wait()
}

func makeScheduleDetails(opts map[string]string) (jobspb.ScheduleDetails, error) {
	var details jobspb.ScheduleDetails
	if v, ok := opts[optOnExecFailure]; ok {
		if err := schedulebase.ParseOnError(v, &details); err != nil {
			return details, err
		}
	}

	if v, ok := opts[optOnPreviousRunning]; ok {
		if err := schedulebase.ParseWaitBehavior(v, &details); err != nil {
			return details, err
		}
	}
	return details, nil
}

// getRedactedChangefeedNode returns a copy of the argument `changefeed`, but with all
// the secret information redacted.
func getRedactedChangefeedNode(
	createChangefeedNode *tree.CreateChangefeed, sinkURI string,
) (*tree.CreateChangefeed, error) {
	e := &tree.CreateChangefeed{
		Targets: createChangefeedNode.Targets,
		Options: createChangefeedNode.Options,
		Select:  createChangefeedNode.Select,
	}

	sanitizedSink, err := cloud.SanitizeExternalStorageURI(sinkURI, nil /* extraParams */)
	if err != nil {
		return nil, err
	}
	e.SinkURI = tree.NewStrVal(sanitizedSink)
	return e, nil
}

func emitSchedule(
	sj *jobs.ScheduledJob,
	createChangefeedNode *tree.CreateChangefeed,
	sinkURI string,
	resultsCh chan<- tree.Datums,
) error {
	redactedChangefeedNode, err := getRedactedChangefeedNode(createChangefeedNode, sinkURI)
	if err != nil {
		return err
	}
	var nextRun tree.Datum
	status := "ACTIVE"
	if sj.IsPaused() {
		nextRun = tree.DNull
		status = "PAUSED"
		if s := sj.ScheduleStatus(); s != "" {
			status += ": " + s
		}
	} else {
		next, err := tree.MakeDTimestampTZ(sj.NextRun(), time.Microsecond)
		if err != nil {
			return err
		}
		nextRun = next
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(sj.ScheduleID())),
		tree.NewDString(sj.ScheduleLabel()),
		tree.NewDString(status),
		nextRun,
		tree.NewDString(sj.ScheduleExpr()),
		tree.NewDString(tree.AsString(redactedChangefeedNode)),
	}
	return nil
}

func collectScheduledChangefeedTelemetry(details jobspb.ScheduleDetails) {
	telemetry.Count("scheduled-changefeed.create.success")
	switch details.Wait {
	case jobspb.ScheduleDetails_WAIT:
		telemetry.Count("scheduled-changefeed.wait-policy.wait")
	case jobspb.ScheduleDetails_NO_WAIT:
		telemetry.Count("scheduled-changefeed.wait-policy.no-wait")
	case jobspb.ScheduleDetails_SKIP:
		telemetry.Count("scheduled-changefeed.wait-policy.skip")
	}
	switch details.OnError {
	case jobspb.ScheduleDetails_RETRY_SCHED:
		telemetry.Count("scheduled-changefeed.error-policy.retry-schedule")
	case jobspb.ScheduleDetails_RETRY_SOON:
		telemetry.Count("scheduled-changefeed.error-policy.retry-soon")
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		telemetry.Count("scheduled-changefeed.error-policy.pause-schedule")
	}
}

// doCreateChangefeeedSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled changefeed.
func doCreateChangefeedSchedule(
	ctx context.Context,
	p sql.PlanHookState,
	eval *scheduledChangefeedSpec,
	resultsCh chan<- tree.Datums,
) error {
	// Revisit: do you need admin rights? cuz for backup you dont.
	if err := p.RequireAdminRole(ctx, scheduleChangefeedOp); err != nil {
		return err
	}

	env := sql.JobSchedulerEnv(p.ExecCfg())

	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	recurrence, err := schedulebase.ComputeScheduleRecurrence(env.Now(), eval.recurrence)
	if err != nil {
		return err
	}

	var scheduleLabel string
	if eval.scheduleLabel != nil {
		if eval.ScheduleLabelSpec.IfNotExists {
			exists, err := schedulebase.CheckScheduleAlreadyExists(ctx, p, *eval.scheduleLabel)
			if err != nil {
				return err
			}

			if exists {
				p.BufferClientNotice(ctx,
					pgnotice.Newf("schedule %q already exists, skipping", eval.scheduleLabel),
				)
				return nil
			}
		}
		scheduleLabel = *eval.scheduleLabel
	} else {
		scheduleLabel = fmt.Sprintf("CHANGEFEED %d", env.Now().Unix())
	}

	createChangefeedopts := changefeedbase.MakeStatementOptions(eval.createChangefeedOptions)

	initialScanType, err := createChangefeedopts.GetInitialScanType()
	if err != nil {
		return err
	}
	if initialScanType != changefeedbase.OnlyInitialScan {
		return errors.AssertionFailedf("%s must be `only` or %s must be specified for scheduled changefeeds",
			changefeedbase.OptInitialScan, changefeedbase.OptInitialScanOnly)
	}

	evalCtx := &p.ExtendedEvalContext().Context
	firstRun, err := scheduleFirstRun(evalCtx, eval.scheduleOpts)
	if err != nil {
		return err
	}

	details, err := makeScheduleDetails(eval.scheduleOpts)
	if err != nil {
		return err
	}

	createChangefeedNode := &tree.CreateChangefeed{
		Targets: eval.Targets,
		SinkURI: tree.NewStrVal(*eval.evaluatedSinkURI),
		Options: eval.Options,
		Select:  eval.Select,
	}

	es, err := makeChangefeedSchedule(env, p.User(), scheduleLabel, recurrence, details, createChangefeedNode)
	if err != nil {
		return err
	}

	if firstRun != nil {
		es.SetNextRun(*firstRun)
	}

	// Revisit: not using extended eval context. (from miretskiy). cuz backup does
	// not use. not sure if its correct.
	if err := es.Create(ctx, p.ExecCfg().InternalExecutor, p.Txn()); err != nil {
		return err
	}

	// Revisit: if dry run fails, do we have to clean up the created schedule?
	err = dryRunCreateChangefeed(ctx, p, es.ScheduleID(), createChangefeedNode)
	if err != nil {
		return errors.Wrapf(err, "failed to dry run create changefeed")
	}

	if err := emitSchedule(es, createChangefeedNode, *eval.evaluatedSinkURI, resultsCh); err != nil {
		return err
	}

	collectScheduledChangefeedTelemetry(details)
	return nil
}

func createChangefeedScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledChangefeed)
	if !ok {
		return nil, nil, nil, false, nil
	}

	spec, err := makeScheduledChangefeedSpec(ctx, p, schedule)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doCreateChangefeedSchedule(ctx, p, spec, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-changefeed.create.failed")
			return err
		}
		return nil
	}

	return fn, scheduledChangefeedHeader, nil, false, nil
}

func createChangefeedScheduleTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	schedule, ok := stmt.(*tree.ScheduledChangefeed)
	if !ok {
		return false, nil, nil
	}

	changefeedStmt := schedule.CreateChangefeed
	if changefeedStmt == nil {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(ctx, scheduleChangefeedOp, p.SemaCtx(),
		exprutil.Strings{
			changefeedStmt.SinkURI,
			schedule.Recurrence,
			schedule.ScheduleLabelSpec.Label,
		},
		&exprutil.KVOptions{
			KVOptions:  changefeedStmt.Options,
			Validation: changefeedvalidators.CreateOptionValidations,
		},
		&exprutil.KVOptions{
			KVOptions:  schedule.ScheduleOptions,
			Validation: scheduledChangefeedOptionExpectValues,
		},
	); err != nil {
		return false, nil, err
	}

	return true, scheduledChangefeedHeader, nil
}

func init() {
	sql.AddPlanHook("schedule changefeed", createChangefeedScheduleHook, createChangefeedScheduleTypeCheck)

	jobs.RegisterScheduledJobExecutorFactory(tree.ScheduledChangefeedExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledChangefeedExecutor.UserName())
			return &scheduledChangefeedExecutor{
				metrics: scheduledChangefeedMetrics{&m},
			}, nil
		})
}
