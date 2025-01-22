// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const opName = "CREATE SCHEDULE FOR CHANGEFEED"

const (
	optFirstRun          = "first_run"
	optOnExecFailure     = "on_execution_failure"
	optOnPreviousRunning = "on_previous_running"
)

var expectValues = map[string]exprutil.KVStringOptValidate{
	optFirstRun:          exprutil.KVStringOptRequireValue,
	optOnExecFailure:     exprutil.KVStringOptRequireValue,
	optOnPreviousRunning: exprutil.KVStringOptRequireValue,
}

var headerCols = colinfo.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "label", Typ: types.String},
	{Name: "status", Typ: types.String},
	{Name: "first_run", Typ: types.TimestampTZ},
	{Name: "schedule", Typ: types.String},
	{Name: "changefeed_stmt", Typ: types.String},
}

type scheduledChangefeedExecutor struct {
	metrics *jobs.ExecutorMetrics
}

var _ jobs.ScheduledJobExecutor = (*scheduledChangefeedExecutor)(nil)

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (s *scheduledChangefeedExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	if err := s.executeChangefeed(ctx, txn, cfg, sj); err != nil {
		s.metrics.NumFailed.Inc(1)
		return err
	}
	s.metrics.NumStarted.Inc(1)
	return nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (s *scheduledChangefeedExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState jobs.State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
) error {
	if jobState == jobs.StateSucceeded {
		s.metrics.NumSucceeded.Inc(1)
		log.Infof(ctx, "changefeed job %d scheduled by %d succeeded", jobID, schedule.ScheduleID())
		return nil
	}

	s.metrics.NumFailed.Inc(1)
	err := errors.Errorf(
		"changefeed job %d scheduled by %d failed with status %s",
		jobID, schedule.ScheduleID(), jobState)
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
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *jobs.ScheduledJob,
) (string, error) {
	changefeedNode, err := extractChangefeedStatement(sj)
	if err != nil {
		return "", err
	}

	args := &changefeedpb.ScheduledChangefeedExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", errors.Wrap(err, "un-marshaling args")
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
	ctx context.Context, txn isql.Txn, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob,
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

	// Invoke changefeed plan hook.
	hook, cleanup := cfg.PlanHookMaker(ctx, "exec-changefeed", txn.KV(), sj.Owner())
	defer cleanup()

	planner := hook.(sql.PlanHookState)
	currentClusterID := planner.ExtendedEvalContext().ClusterID
	currentDetails := sj.ScheduleDetails()

	// If the current cluster ID is different than the schedule's cluster ID,
	// pause the schedule. To maintain backward compatability with schedules
	// without a clusterID, don't pause schedules without a clusterID.
	if !currentDetails.ClusterID.Equal(uuid.Nil) && currentClusterID != currentDetails.ClusterID {
		log.Infof(ctx, "scheduled changedfeed %d last run by different cluster %s, pausing until manually resumed",
			sj.ScheduleID(),
			currentDetails.ClusterID)
		currentDetails.ClusterID = currentClusterID
		sj.SetScheduleDetails(*currentDetails)
		sj.Pause()
		return nil
	}

	log.Infof(ctx, "Starting scheduled changefeed %d: %s",
		sj.ScheduleID(), tree.AsString(changefeedStmt))
	changefeedFn, err := planCreateChangefeed(ctx, planner, changefeedStmt)
	if err != nil {
		return err
	}
	return invokeCreateChangefeed(ctx, changefeedFn)
}

// extractChangefeedStatement returns tree.CreateChangefeed node encoded inside
// scheduled job.
func extractChangefeedStatement(sj *jobs.ScheduledJob) (*annotatedChangefeedStatement, error) {
	args := &changefeedpb.ScheduledChangefeedExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.ChangefeedStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing export statement")
	}

	if stmt, ok := node.AST.(*tree.CreateChangefeed); ok {

		return &annotatedChangefeedStatement{
			CreateChangefeed: stmt,
			CreatedByInfo: &jobs.CreatedByInfo{
				Name: jobs.CreatedByScheduledJobs,
				ID:   int64(sj.ScheduleID()),
			},
		}, nil
	}

	return nil, errors.AssertionFailedf("unexpect node type %T", node)
}

// scheduledChangefeed is a representation of tree.ScheduledChangefeed, prepared
// for evaluation
type scheduledChangefeedSpec struct {
	*tree.ScheduledChangefeed

	// Schedule specific properties that get evaluated.
	scheduleLabel *string
	recurrence    *string
	scheduleOpts  map[string]string

	// Changefeed specific properties that get evaluated.
	createChangefeedOptions map[string]string
	evaluatedSinkURI        *string
}

func getFirstRunOpt(evalCtx *eval.Context, opts map[string]string) (*time.Time, error) {
	if v, ok := opts[optFirstRun]; ok {
		firstRun, _, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
		if err != nil {
			return nil, err
		}
		return &firstRun.Time, nil
	}
	return nil, nil
}

// makeScheduleChangefeedSpec prepares helper scheduledChangefeedSpec struct to
// assist in evaluation of various schedule and changefeed specific components.
func makeScheduledChangefeedSpec(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledChangefeed,
) (*scheduledChangefeedSpec, error) {
	exprEval := p.ExprEvaluator(opName)
	var err error

	tablePatterns := make([]tree.TablePattern, 0)
	for _, target := range schedule.Targets {
		tablePatterns = append(tablePatterns, target.TableName)
	}

	qualifiedTablePatterns, err := schedulebase.FullyQualifyTables(ctx, p, tablePatterns)
	if err != nil {
		return nil, errors.Wrap(err, "qualifying target tables")
	}

	newTargets := make([]tree.ChangefeedTarget, 0)
	for i, table := range qualifiedTablePatterns {
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
	// TODO(cdc): check if this is needed as we might be doing the same step
	// when we prepare the changefeed expression to be serialized.
	if schedule.Select != nil {
		tableExprs := make(tree.TableExprs, 1)
		// For cdc transformations, we expect only 1 table. Therefore, we can always
		// expect the size of qualifiedTablePatterns to be 1 - which is why we can
		// directly index with 0.
		// We can directly typecast tree.TablePatter to tree.TableExpr without
		// checking for error because when parsing the statement we typecast
		// tree.TableExpr (from Select clause) into tree.ChangefeedTarget. If that
		// typecasting was successful, it is guaranteed that the reverse should work
		// without any errors.
		tableExprs[0] = qualifiedTablePatterns[0].(tree.TableExpr)
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
		p.ExecCfg().Settings,
		opName)

	if !(enterpriseCheckErr == nil) {
		// Cannot use SCHEDULED CHANGEFEED w/out enterprise license.
		return nil, enterpriseCheckErr
	}

	spec.scheduleOpts, err = exprEval.KVOptions(
		ctx, schedule.ScheduleOptions, expectValues,
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

	if evaluatedSinkURI == "" {
		return nil, errors.AssertionFailedf("Sink URI must not be empty")
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

	if err := sj.SetScheduleAndNextRun(recurrence.Cron); err != nil {
		return nil, err
	}

	sj.SetScheduleDetails(details)

	var args changefeedpb.ScheduledChangefeedExecutionArgs
	args.ChangefeedStatement = tree.AsStringWithFlags(changefeedNode, tree.FmtParsable|tree.FmtShowPasswords)
	any, err := pbtypes.MarshalAny(&args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledChangefeedExecutor.InternalName(), jobspb.ExecutionArguments{Args: any},
	)

	return sj, nil
}

// dryRunChangefeed executes Changefeed in dry-run mode: we simply execute
// Changefeed under transaction savepoint, and then rollback to that save point.
func dryRunCreateChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	scheduleID jobspb.ScheduleID,
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
				ID:   int64(scheduleID),
			},
		}
		changefeedFn, err := planCreateChangefeed(ctx, p, annotated)
		if err != nil {
			return err
		}
		return invokeCreateChangefeed(ctx, changefeedFn)
	}()

	if rollbackErr := p.ExtendedEvalContext().Txn.RollbackToSavepoint(ctx, sp); rollbackErr != nil {
		if err != nil {
			return errors.CombineErrors(rollbackErr, err)
		}
		return rollbackErr
	}
	return err
}

func planCreateChangefeed(
	ctx context.Context, p sql.PlanHookState, createChangefeedStmt tree.Statement,
) (sql.PlanHookRowFn, error) {
	fn, cols, _, err := changefeedPlanHook(ctx, createChangefeedStmt, p)

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
		return createChangefeedFn(ctx, resultCh)
	})

	return g.Wait()
}

// TODO(msbutler): move this function into scheduleBase and remove duplicate function in scheduled backups.
func makeScheduleDetails(
	opts map[string]string, clusterID uuid.UUID, version clusterversion.ClusterVersion,
) (jobspb.ScheduleDetails, error) {
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
	details.ClusterID = clusterID
	details.CreationClusterVersion = version
	return details, nil
}

func emitSchedule(
	ctx context.Context,
	sj *jobs.ScheduledJob,
	createChangefeedNode *tree.CreateChangefeed,
	sinkURI string,
	createChangefeedOpts map[string]string,
	resultsCh chan<- tree.Datums,
) error {
	opts := changefeedbase.MakeStatementOptions(createChangefeedOpts)
	redactedChangefeedNode, err := makeChangefeedDescription(ctx, createChangefeedNode, sinkURI, opts)
	if err != nil {
		return err
	}
	var nextRun tree.Datum
	status := "ACTIVE"
	next, err := tree.MakeDTimestampTZ(sj.NextRun(), time.Microsecond)
	if err != nil {
		return err
	}
	nextRun = next

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(sj.ScheduleID())),
		tree.NewDString(sj.ScheduleLabel()),
		tree.NewDString(status),
		nextRun,
		tree.NewDString(sj.ScheduleExpr()),
		tree.NewDString(redactedChangefeedNode),
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

// doCreateChangefeeedSchedule creates requested schedule (or schedules). It is
// a plan hook implementation responsible for the creating of scheduled
// changefeed.
func doCreateChangefeedSchedule(
	ctx context.Context,
	p sql.PlanHookState,
	spec *scheduledChangefeedSpec,
	resultsCh chan<- tree.Datums,
) error {

	env := sql.JobSchedulerEnv(p.ExecCfg().JobsKnobs())

	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	recurrence, err := schedulebase.ComputeScheduleRecurrence(env.Now(), spec.recurrence)
	if err != nil {
		return err
	}

	var scheduleLabel string
	if spec.scheduleLabel != nil {
		if spec.ScheduleLabelSpec.IfNotExists {
			exists, err := schedulebase.CheckScheduleAlreadyExists(ctx, p, *spec.scheduleLabel)
			if err != nil {
				return err
			}

			if exists {
				p.BufferClientNotice(ctx,
					pgnotice.Newf("schedule %q already exists, skipping", *spec.scheduleLabel),
				)
				return nil
			}
		}
		scheduleLabel = *spec.scheduleLabel
	} else {
		scheduleLabel = fmt.Sprintf("CHANGEFEED %d", env.Now().Unix())
	}

	createChangefeedopts := changefeedbase.MakeStatementOptions(spec.createChangefeedOptions)
	initialScanSpecifiedByUser := createChangefeedopts.IsInitialScanSpecified()
	if !initialScanSpecifiedByUser {
		log.Infof(ctx, "Initial scan type not specified, forcing %s option", changefeedbase.OptInitialScanOnly)
		spec.Options = append(spec.Options, tree.KVOption{
			Key:   changefeedbase.OptInitialScan,
			Value: tree.NewStrVal("only"),
		})
		spec.createChangefeedOptions[changefeedbase.OptInitialScan] = "only"

		p.BufferClientNotice(ctx, pgnotice.Newf("added missing initial_scan='only' option to schedule changefeed"))
	} else {
		initialScanType, err := createChangefeedopts.GetInitialScanType()
		if err != nil {
			return err
		}
		if initialScanType != changefeedbase.OnlyInitialScan {
			return pgerror.Newf(pgcode.InvalidParameterValue, "%s must be `only` for scheduled changefeeds",
				changefeedbase.OptInitialScan)
		}
	}

	evalCtx := &p.ExtendedEvalContext().Context
	firstRun, err := getFirstRunOpt(evalCtx, spec.scheduleOpts)
	if err != nil {
		return err
	}

	details, err := makeScheduleDetails(spec.scheduleOpts, evalCtx.ClusterID, p.ExecCfg().Settings.Version.ActiveVersion(ctx))
	if err != nil {
		return err
	}

	createChangefeedNode := &tree.CreateChangefeed{
		Targets: spec.Targets,
		SinkURI: tree.NewStrVal(*spec.evaluatedSinkURI),
		Options: spec.Options,
		Select:  spec.Select,
	}

	es, err := makeChangefeedSchedule(env, p.User(), scheduleLabel, recurrence, details, createChangefeedNode)
	if err != nil {
		return err
	}

	if firstRun != nil {
		es.SetNextRun(*firstRun)
	}

	if err := jobs.ScheduledJobTxn(p.InternalSQLTxn()).Create(ctx, es); err != nil {
		return err
	}

	if err = dryRunCreateChangefeed(
		ctx, p, es.ScheduleID(), createChangefeedNode,
	); err != nil {
		// We do not know for sure that implicitly passing initial_scan_only caused
		// the pgcode.InvalidParameterValue, but it may have.
		if !initialScanSpecifiedByUser && pgerror.GetPGCode(err) == pgcode.InvalidParameterValue {
			err = errors.WithHintf(err, "scheduled changefeeds implicitly pass the option %s='only'", changefeedbase.OptInitialScan)
		}
		return errors.Wrapf(err, "Failed to dry run create changefeed")
	}

	if err := emitSchedule(
		ctx, es, createChangefeedNode, *spec.evaluatedSinkURI,
		spec.createChangefeedOptions, resultsCh,
	); err != nil {
		return err
	}

	collectScheduledChangefeedTelemetry(details)
	return nil
}

func createChangefeedScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledChangefeed)
	if !ok {
		return nil, nil, false, nil
	}

	spec, err := makeScheduledChangefeedSpec(ctx, p, schedule)
	if err != nil {
		return nil, nil, false, err
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		err := doCreateChangefeedSchedule(ctx, p, spec, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-changefeed.create.failed")
			return err
		}
		return nil
	}

	return fn, headerCols, false, nil
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
	if err := exprutil.TypeCheck(ctx, opName, p.SemaCtx(),
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
			Validation: expectValues,
		},
	); err != nil {
		return false, nil, err
	}

	return true, headerCols, nil
}

func init() {
	sql.AddPlanHook("schedule changefeed", createChangefeedScheduleHook, createChangefeedScheduleTypeCheck)

	jobs.RegisterScheduledJobExecutorFactory(tree.ScheduledChangefeedExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledChangefeedExecutor.UserName())
			return &scheduledChangefeedExecutor{
				metrics: &m,
			}, nil
		})
}
