// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"maps"
	"net/url"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/backup/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func init() {
	sql.AddPlanHook("alter changefeed", alterChangefeedPlanHook, alterChangefeedTypeCheck)
}

const telemetryPath = `changefeed.alter`

func alterChangefeedTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	alterChangefeedStmt, ok := stmt.(*tree.AlterChangefeed)
	if !ok {
		return false, nil, nil
	}
	toCheck := []exprutil.ToTypeCheck{
		exprutil.Ints{alterChangefeedStmt.Jobs},
	}
	// TODO(#156806): Validate the options for ADD and UNSET alter commands
	// to fail when 'PREPARE'ing an ALTER CHANGEFEED statement specifying an
	// invalid option.
	for _, cmd := range alterChangefeedStmt.Cmds {
		switch v := cmd.(type) {
		case *tree.AlterChangefeedSetOptions:
			toCheck = append(toCheck, &exprutil.KVOptions{
				KVOptions:  v.Options,
				Validation: changefeedvalidators.AlterOptionValidations,
			})
		}
	}
	if err := exprutil.TypeCheck(ctx, "ALTER CHANGEFED", p.SemaCtx(), toCheck...); err != nil {
		return false, nil, err
	}
	return true, alterChangefeedHeader, nil
}

var alterChangefeedHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "job_description", Typ: types.String},
}

// alterChangefeedPlanHook implements sql.PlanHookFn.
func alterChangefeedPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	alterChangefeedStmt, ok := stmt.(*tree.AlterChangefeed)
	if !ok {
		return nil, nil, false, nil
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		jobID, err := func() (jobspb.JobID, error) {
			origProps := p.SemaCtx().Properties
			p.SemaCtx().Properties.Require("cdc", tree.RejectSubqueries)
			defer p.SemaCtx().Properties.Restore(origProps)

			id, err := p.ExprEvaluator("ALTER CHANGEFEED").Int(ctx, alterChangefeedStmt.Jobs)
			if err != nil {
				return jobspb.JobID(0), err
			}
			return jobspb.JobID(id), nil
		}()
		if err != nil {
			return pgerror.Wrap(err, pgcode.DatatypeMismatch, "changefeed ID must be an INT value")
		}

		job, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.InternalSQLTxn())
		if err != nil {
			err = errors.Wrapf(err, `could not load job with job id %d`, jobID)
			return err
		}

		jobPayload := job.Payload()

		globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, p)
		if err != nil {
			return err
		}
		err = jobsauth.Authorize(
			ctx, p, jobID, jobPayload.UsernameProto.Decode(), jobsauth.ControlAccess, globalPrivileges,
		)
		if err != nil {
			return err
		}

		prevDetails, ok := job.Details().(jobspb.ChangefeedDetails)
		if !ok {
			return errors.Errorf(`job %d is not changefeed job`, jobID)
		}

		if job.State() != jobs.StatePaused {
			return errors.Errorf(`job %d is not paused`, jobID)
		}

		prevOpts, err := getPrevOpts(job.Payload().Description, prevDetails.Opts)
		if err != nil {
			return err
		}
		exprEval := p.ExprEvaluator("ALTER CHANGEFEED")
		newOptions, newSinkURI, err := generateNewOpts(
			ctx, exprEval, alterChangefeedStmt.Cmds, prevOpts, prevDetails.SinkURI,
		)
		if err != nil {
			return err
		}
		if p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V26_2_ChangefeedsDiscardEmptyTopicName) &&
			!p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V26_2_ChangefeedsRejectEmptyTopicName) {
			newSinkURI = changefeedbase.StripEmptyParam(newSinkURI, changefeedbase.SinkParamTopicName)
		}

		st, err := newOptions.GetInitialScanType()
		if err != nil {
			return err
		}
		if err := validateSettings(ctx, st != changefeedbase.OnlyInitialScan, p.ExecCfg()); err != nil {
			return err
		}

		if isDBLevelChangefeed(prevDetails) {
			return alterDatabaseChangefeed(
				ctx, p, alterChangefeedStmt, newOptions, newSinkURI,
				prevDetails, job, jobID, resultsCh,
			)
		}
		return alterTableChangefeed(
			ctx, p, exprEval, alterChangefeedStmt, newOptions, newSinkURI,
			prevDetails, job, jobID, resultsCh,
		)
	}

	return fn, alterChangefeedHeader, false, nil
}

// alterDatabaseChangefeed handles ALTER CHANGEFEED for database-level feeds.
// Database-level feeds do not support ADD/DROP target commands. They support
// filter changes (SET/UNSET INCLUDE/EXCLUDE TABLES) and option changes.
func alterDatabaseChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	alterChangefeedStmt *tree.AlterChangefeed,
	newOptions changefeedbase.StatementOptions,
	newSinkURI string,
	prevDetails jobspb.ChangefeedDetails,
	job *jobs.Job,
	jobID jobspb.JobID,
	resultsCh chan<- tree.Datums,
) error {
	if len(prevDetails.TargetSpecifications) != 1 {
		return errors.AssertionFailedf(
			"database level changefeed must have exactly one target specification",
		)
	}

	// Database-level feeds do not support ADD/DROP target commands.
	isAlteringTargets := slices.ContainsFunc(
		alterChangefeedStmt.Cmds,
		func(cmd tree.AlterChangefeedCmd) bool {
			_, isAdd := cmd.(*tree.AlterChangefeedAddTarget)
			_, isDrop := cmd.(*tree.AlterChangefeedDropTarget)
			return isAdd || isDrop
		})
	if isAlteringTargets {
		return errors.Errorf("cannot alter targets for a database level changefeed")
	}

	// Look up database descriptor to set DatabaseTarget.
	targetSpec := prevDetails.TargetSpecifications[0]
	txn := p.InternalSQLTxn()
	databaseDescriptor, err := txn.Descriptors().
		ByIDWithLeased(txn.KV()).Get().
		Database(ctx, targetSpec.DescID)
	if err != nil {
		return err
	}

	newChangefeedStmt := &tree.CreateChangefeed{
		Level:          tree.ChangefeedLevelDatabase,
		DatabaseTarget: tree.ChangefeedDatabaseTarget(databaseDescriptor.GetName()),
		SinkURI:        tree.NewDString(newSinkURI),
	}

	// Process filter changes (SET/UNSET INCLUDE/EXCLUDE TABLES).
	if err := processFiltersForDatabaseChangefeed(
		newChangefeedStmt, alterChangefeedStmt.Cmds, prevDetails,
	); err != nil {
		return err
	}

	newProgress := job.Progress()
	newStatementTime := prevDetails.StatementTime

	var resolveTime hlc.Timestamp
	highWater := newProgress.GetHighWater()
	if highWater != nil && !highWater.IsEmpty() {
		resolveTime = *highWater
	} else {
		resolveTime = newStatementTime
	}

	annotatedStmt := &annotatedChangefeedStatement{
		CreateChangefeed:    newChangefeedStmt,
		alterChangefeedAsOf: resolveTime,
	}

	newDescription, err := makeChangefeedDescription(
		ctx, annotatedStmt.CreateChangefeed, newSinkURI, newOptions,
	)
	if err != nil {
		return err
	}

	jobRecord, targets, err := createChangefeedJobRecord(
		ctx, p, annotatedStmt, newDescription, newSinkURI, newOptions, jobID, ``,
	)
	if err != nil {
		return errors.Wrap(err, `failed to alter changefeed`)
	}

	newDetails := jobRecord.Details.(jobspb.ChangefeedDetails)
	newDetails.StatementTime = newStatementTime

	// NB: We do not need to remove spans for tables that are no longer
	// watched from job progress, only to add spans for new tables.
	// When the changefeed is resumed, we will make the frontier so that
	// it will only watch the new set of watched tables. This means
	// that if altering a filter results in us watching fewer tables,
	// we will not emit ANY more events for that table, even if it was
	// lagging, i.e. even if they corresponded to updates made before
	// the ALTER CHANGEFEED statement.
	if err := storeFilterChangeFrontierForDatabaseChangefeed(
		ctx, p, jobID, newDetails, prevDetails,
	); err != nil {
		return err
	}

	return finalizeAlterChangefeed(
		ctx, p, job, jobID, jobRecord, newDetails, &newProgress, targets, resultsCh,
	)
}

// alterTableChangefeed handles ALTER CHANGEFEED for table-level feeds.
// Table-level feeds support ADD/DROP target commands and option changes,
// but do not support filter changes (SET/UNSET INCLUDE/EXCLUDE TABLES).
func alterTableChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	exprEval exprutil.Evaluator,
	alterChangefeedStmt *tree.AlterChangefeed,
	newOptions changefeedbase.StatementOptions,
	newSinkURI string,
	prevDetails jobspb.ChangefeedDetails,
	job *jobs.Job,
	jobID jobspb.JobID,
	resultsCh chan<- tree.Datums,
) error {
	// Table-level feeds do not support filter commands.
	filterCommands := generateNewFilters(alterChangefeedStmt.Cmds)
	if len(filterCommands) > 0 {
		return errors.Errorf("cannot set filters for table level changefeeds")
	}

	newTargets, newProgress, newStatementTime, originalSpecs, err :=
		generateAndValidateNewTargetsForTableLevelFeed(
			ctx, exprEval, p, alterChangefeedStmt.Cmds, newOptions,
			prevDetails, job.Progress(), newSinkURI,
		)
	if err != nil {
		return err
	}

	newChangefeedStmt := &tree.CreateChangefeed{
		Level:        tree.ChangefeedLevelTable,
		TableTargets: newTargets,
		SinkURI:      tree.NewDString(newSinkURI),
	}

	if prevDetails.Select != "" {
		query, err := cdceval.ParseChangefeedExpression(prevDetails.Select)
		if err != nil {
			return err
		}
		newChangefeedStmt.Select = query
	}

	var resolveTime hlc.Timestamp
	highWater := newProgress.GetHighWater()
	if highWater != nil && !highWater.IsEmpty() {
		resolveTime = *highWater
	} else {
		resolveTime = newStatementTime
	}

	annotatedStmt := &annotatedChangefeedStatement{
		CreateChangefeed:    newChangefeedStmt,
		originalSpecs:       originalSpecs,
		alterChangefeedAsOf: resolveTime,
	}

	newDescription, err := makeChangefeedDescription(
		ctx, annotatedStmt.CreateChangefeed, newSinkURI, newOptions,
	)
	if err != nil {
		return err
	}

	jobRecord, targets, err := createChangefeedJobRecord(
		ctx, p, annotatedStmt, newDescription, newSinkURI, newOptions, jobID, ``,
	)
	if err != nil {
		return errors.Wrap(err, `failed to alter changefeed`)
	}

	newDetails := jobRecord.Details.(jobspb.ChangefeedDetails)
	newDetails.StatementTime = newStatementTime

	// For table-level feeds, set the initial scan option to its default
	// value of "yes" so that we can do an initial scan for new targets.
	newDetails.Opts[changefeedbase.OptInitialScan] = ``

	return finalizeAlterChangefeed(
		ctx, p, job, jobID, jobRecord, newDetails, newProgress, targets, resultsCh,
	)
}

// finalizeAlterChangefeed persists the new job payload and progress,
// emits telemetry, and sends the result to resultsCh.
func finalizeAlterChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	job *jobs.Job,
	jobID jobspb.JobID,
	jobRecord *jobs.Record,
	newDetails jobspb.ChangefeedDetails,
	newProgress *jobspb.Progress,
	targets changefeedbase.Targets,
	resultsCh chan<- tree.Datums,
) error {
	prevDescription := job.Payload().Description

	newPayload := job.Payload()
	newPayload.Details = jobspb.WrapPayloadDetails(newDetails)
	newPayload.Description = jobRecord.Description
	newPayload.DescriptorIDs = jobRecord.DescriptorIDs

	// The maximum PTS age on jobRecord will be set correctly (based on either
	// the option or cluster setting) by createChangefeedJobRecord.
	newPayload.MaximumPTSAge = jobRecord.MaximumPTSAge

	j, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.InternalSQLTxn())
	if err != nil {
		return err
	}
	if err := j.WithTxn(p.InternalSQLTxn()).Update(ctx, func(
		txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		ju.UpdatePayload(&newPayload)
		if newProgress != nil {
			ju.UpdateProgress(newProgress)
		}
		return nil
	}); err != nil {
		return err
	}

	telemetry.Count(telemetryPath)
	shouldMigrate := log.ShouldMigrateEvent(p.ExecCfg().SV())
	logAlterChangefeedTelemetry(ctx, j, prevDescription, targets.Size, shouldMigrate)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(jobID)),
		tree.NewDString(jobRecord.Description),
	}:
		return nil
	}
}

// processFiltersForDatabaseChangefeed validates and applies filter changes to a
// database-level changefeed statement. It should only be called for
// database-level feeds.
func processFiltersForDatabaseChangefeed(
	stmt *tree.CreateChangefeed,
	alterCmds tree.AlterChangefeedCmds,
	prevDetails jobspb.ChangefeedDetails,
) error {
	filterCommands := generateNewFilters(alterCmds)

	// We add the existing filter state to the changefeed statement for
	// DB-level feeds. This ensures that existing filters are preserved when
	// we alter options unrelated to filters and that we can error if we try
	// to set both include and exclude filters at the same time.
	targetSpec := prevDetails.TargetSpecifications[0]
	filterOpt, err := parseFilterOptionFromTargetSpec(targetSpec)
	if err != nil {
		return err
	}
	stmt.FilterOption = filterOpt

	for _, filter := range filterCommands {
		currentFilter := stmt.FilterOption
		if !currentFilter.IsEmpty() && currentFilter.FilterType != filter.FilterType {
			return errors.Errorf("cannot alter filter type from %s to %s", currentFilter.FilterType, filter.FilterType)
		}
		if len(filter.Tables) > 0 {
			stmt.FilterOption = tree.ChangefeedFilterOption{
				FilterType: filter.FilterType,
				Tables:     filter.Tables,
			}
		} else {
			stmt.FilterOption = tree.ChangefeedFilterOption{}
		}
	}

	return nil
}

func getTargetDesc(
	ctx context.Context,
	p sql.PlanHookState,
	descResolver *backupresolver.DescriptorResolver,
	targetPattern tree.TablePattern,
) (catalog.Descriptor, bool, error) {
	pattern, err := targetPattern.NormalizeTablePattern()
	if err != nil {
		return nil, false, err
	}
	targetName, ok := pattern.(*tree.TableName)
	if !ok {
		return nil, false, errors.Errorf(`CHANGEFEED cannot target %q`, tree.AsString(targetPattern))
	}

	found, _, desc, err := resolver.ResolveExisting(
		ctx,
		targetName.ToUnresolvedObjectName(),
		descResolver,
		tree.ObjectLookupFlags{},
		p.CurrentDatabase(),
		p.CurrentSearchPath(),
	)
	if err != nil {
		return nil, false, err
	}

	return desc, found, nil
}

func generateNewOpts(
	ctx context.Context,
	exprEval exprutil.Evaluator,
	alterCmds tree.AlterChangefeedCmds,
	prevOpts map[string]string,
	prevSinkURI string,
) (changefeedbase.StatementOptions, string, error) {
	sinkURI := prevSinkURI
	newOptions := prevOpts
	null := changefeedbase.StatementOptions{}

	for _, cmd := range alterCmds {
		switch v := cmd.(type) {
		case *tree.AlterChangefeedSetOptions:
			opts, err := exprEval.KVOptions(
				ctx, v.Options, changefeedvalidators.AlterOptionValidations,
			)
			if err != nil {
				return null, ``, err
			}

			for key, value := range opts {
				if _, ok := changefeedbase.AlterChangefeedUnsupportedOptions[key]; ok {
					return null, ``, pgerror.Newf(pgcode.InvalidParameterValue, `cannot alter option %q`, key)
				}
				if key == changefeedbase.OptSink {
					newSinkURI, err := url.Parse(value)
					if err != nil {
						return null, ``, err
					}

					prevSinkURI, err := url.Parse(sinkURI)
					if err != nil {
						return null, ``, err
					}

					if newSinkURI.Scheme != prevSinkURI.Scheme {
						return null, ``, pgerror.Newf(
							pgcode.InvalidParameterValue,
							`New sink type %q does not match original sink type %q. `+
								`Altering the sink type of a changefeed is disallowed, consider creating a new changefeed instead.`,
							newSinkURI.Scheme,
							prevSinkURI.Scheme,
						)
					}

					sinkURI = value
				} else {
					newOptions[key] = value
				}
			}
			telemetry.CountBucketed(telemetryPath+`.set_options`, int64(len(opts)))
		case *tree.AlterChangefeedUnsetOptions:
			optKeys := v.Options.ToStrings()
			for _, key := range optKeys {
				if key == changefeedbase.OptSink {
					return null, ``, pgerror.Newf(pgcode.InvalidParameterValue, `cannot unset option %q`, key)
				}
				if _, ok := changefeedbase.ChangefeedOptionExpectValues[key]; !ok {
					return null, ``, pgerror.Newf(pgcode.InvalidParameterValue, `invalid option %q`, key)
				}
				if _, ok := changefeedbase.AlterChangefeedUnsupportedOptions[key]; ok {
					return null, ``, pgerror.Newf(pgcode.InvalidParameterValue, `cannot alter option %q`, key)
				}
				delete(newOptions, key)
			}
			telemetry.CountBucketed(telemetryPath+`.unset_options`, int64(len(optKeys)))
		}
	}

	return changefeedbase.MakeStatementOptions(newOptions), sinkURI, nil
}

func generateAndValidateNewTargetsForTableLevelFeed(
	ctx context.Context,
	exprEval exprutil.Evaluator,
	p sql.PlanHookState,
	alterCmds tree.AlterChangefeedCmds,
	opts changefeedbase.StatementOptions,
	prevDetails jobspb.ChangefeedDetails,
	prevProgress jobspb.Progress,
	sinkURI string,
) (
	tree.ChangefeedTableTargets,
	*jobspb.Progress,
	hlc.Timestamp,
	map[tree.ChangefeedTableTarget]jobspb.ChangefeedTargetSpecification,
	error,
) {
	type targetKey struct {
		TableID    descpb.ID
		FamilyName tree.Name
	}
	newTargets := make(map[targetKey]tree.ChangefeedTableTarget)
	droppedTargets := make(map[targetKey]tree.ChangefeedTableTarget)
	newTableDescs := make(map[descpb.ID]catalog.Descriptor)

	// originalSpecs provides a mapping between tree.ChangefeedTargets that
	// existed prior to the alteration of the changefeed to their corresponding
	// jobspb.ChangefeedTargetSpecification. The purpose of this mapping is to ensure
	// that the StatementTimeName of the existing targets are not modified when the
	// name of the target was modified.
	originalSpecs := make(map[tree.ChangefeedTableTarget]jobspb.ChangefeedTargetSpecification)

	// We want to store the value of whether or not the original changefeed had
	// initial_scan set to only so that we only do an initial scan on an alter
	// changefeed with initial_scan = 'only' if the original one also had
	// initial_scan = 'only'.
	originalInitialScanType, err := opts.GetInitialScanType()
	if err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}
	originalInitialScanOnlyOption := originalInitialScanType == changefeedbase.OnlyInitialScan

	// When we add new targets with or without initial scans, indicating
	// initial_scan or no_initial_scan in the job description would lose its
	// meaning. Hence, we will omit these details from the changefeed
	// description. However, to ensure that we do perform the initial scan on
	// newly added targets, we will introduce the initial_scan opt after the
	// job record is created.

	opts.Unset(changefeedbase.OptInitialScanOnly)
	opts.Unset(changefeedbase.OptNoInitialScan)
	opts.Unset(changefeedbase.OptInitialScan)

	// the new progress and statement time will start from the progress and
	// statement time of the job prior to the alteration of the changefeed. Each
	// time we add a new set of targets we update the newJobProgress and
	// newJobStatementTime accordingly.
	newJobProgress := prevProgress
	newJobStatementTime := prevDetails.StatementTime

	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}

	// we attempt to resolve the changefeed targets as of the current time to
	// ensure that all targets exist. However, we also need to make sure that all
	// targets can be resolved at the time in which the changefeed is resumed. We
	// perform these validations in the validateNewTargets function.
	allDescs, err := backupresolver.LoadAllDescs(ctx, p.ExecCfg(), statementTime)
	if err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}
	descResolver, err := backupresolver.NewDescriptorResolver(allDescs)
	if err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}

	targetTS := prevProgress.GetHighWater()
	if targetTS == nil || targetTS.IsEmpty() {
		targetTS = &prevDetails.StatementTime
	}
	prevTargets, err := AllTargets(ctx, prevDetails, p.ExecCfg(), *targetTS)
	if err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}
	noLongerExist := make(map[string]descpb.ID)
	if err := prevTargets.EachTarget(func(targetSpec changefeedbase.Target) error {
		k := targetKey{TableID: targetSpec.DescID, FamilyName: tree.Name(targetSpec.FamilyName)}
		var desc catalog.TableDescriptor
		if d, exists := descResolver.DescByID[targetSpec.DescID]; exists {
			desc = d.(catalog.TableDescriptor)
		} else {
			// Table was dropped; that's okay since the changefeed likely
			// will handle DROP alter command below; and if not, then we'll resume
			// the changefeed, which will promptly fail if the table no longer exist.
			noLongerExist[string(targetSpec.StatementTimeName)] = targetSpec.DescID
			return nil
		}

		tbName, err := getQualifiedTableNameObj(ctx, p.ExecCfg(), p.Txn(), desc)
		if err != nil {
			return err
		}

		tablePattern, err := tbName.NormalizeTablePattern()
		if err != nil {
			return err
		}

		newTarget := tree.ChangefeedTableTarget{
			TableName:  tablePattern,
			FamilyName: tree.Name(targetSpec.FamilyName),
		}
		newTargets[k] = newTarget
		newTableDescs[targetSpec.DescID] = descResolver.DescByID[targetSpec.DescID]

		originalSpecs[newTarget] = jobspb.ChangefeedTargetSpecification{
			Type:              targetSpec.Type,
			DescID:            targetSpec.DescID,
			FamilyName:        targetSpec.FamilyName,
			StatementTimeName: string(targetSpec.StatementTimeName),
		}
		return nil
	}); err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}

	checkIfCommandAllowed := func() error {
		if prevDetails.Select == "" {
			return nil
		}
		return errors.WithIssueLink(
			errors.New("cannot modify targets when using CDC query changefeed; consider recreating changefeed"),
			errors.IssueLink{
				IssueURL: build.MakeIssueURL(83033),
				Detail: "you have encountered a known bug in CockroachDB, please consider " +
					"reporting on the Github issue or reach out via Support.",
			})
	}

	for _, cmd := range alterCmds {
		switch v := cmd.(type) {
		case *tree.AlterChangefeedAddTarget:
			if err := checkIfCommandAllowed(); err != nil {
				return nil, nil, hlc.Timestamp{}, nil, err
			}

			targetOpts, err := exprEval.KVOptions(
				ctx, v.Options, changefeedvalidators.AlterTargetOptionValidations,
			)
			if err != nil {
				return nil, nil, hlc.Timestamp{}, nil, err
			}

			initialScanType, initialScanSet := targetOpts[changefeedbase.OptInitialScan]
			_, noInitialScanSet := targetOpts[changefeedbase.OptNoInitialScan]

			if initialScanType != `` && initialScanType != `yes` && initialScanType != `no` && initialScanType != `only` {
				return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot set %q to %q. possible values for initial_scan are "yes", "no", "only", or no value`,
					changefeedbase.OptInitialScan, initialScanType,
				)
			}

			if initialScanSet && noInitialScanSet {
				return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot specify both %q and %q`, changefeedbase.OptInitialScan,
					changefeedbase.OptNoInitialScan,
				)
			}

			withInitialScan := (initialScanType == `` && initialScanSet) ||
				initialScanType == `yes` ||
				(initialScanType == `only` && originalInitialScanOnlyOption)

			// TODO(#142376): Audit whether this list is generated correctly.
			var existingTargetSpanIDs []spanID
			for _, targetDesc := range newTableDescs {
				tableDesc, ok := targetDesc.(catalog.TableDescriptor)
				if !ok {
					return nil, nil, hlc.Timestamp{}, nil, errors.AssertionFailedf("expected table descriptor")
				}
				existingTargetSpanIDs = append(existingTargetSpanIDs, spanID{
					tableID: tableDesc.GetID(),
					indexID: tableDesc.GetPrimaryIndexID(),
				})
			}
			existingTargetSpans := fetchSpansForDescs(p, existingTargetSpanIDs)
			var addedTargetSpanIDs []spanID
			for _, target := range v.Targets {
				desc, found, err := getTargetDesc(ctx, p, descResolver, target.TableName)
				if err != nil {
					return nil, nil, hlc.Timestamp{}, nil, err
				}
				if !found {
					return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q does not exist`,
						tree.ErrString(&target),
					)
				}

				k := targetKey{TableID: desc.GetID(), FamilyName: target.FamilyName}
				newTargets[k] = target
				newTableDescs[desc.GetID()] = desc
				tableDesc, ok := desc.(catalog.TableDescriptor)
				if !ok {
					return nil, nil, hlc.Timestamp{}, nil, errors.AssertionFailedf("expected table descriptor")
				}
				addedTargetSpanIDs = append(addedTargetSpanIDs, spanID{
					tableID: tableDesc.GetID(),
					indexID: tableDesc.GetPrimaryIndexID(),
				})
			}

			addedTargetSpans := fetchSpansForDescs(p, addedTargetSpanIDs)

			// By default, we will not perform an initial scan on newly added
			// targets. Hence, the user must explicitly state that they want an
			// initial scan performed on the new targets.
			newJobProgress, newJobStatementTime, err = generateNewProgress(
				newJobProgress,
				newJobStatementTime,
				existingTargetSpans,
				addedTargetSpans,
				withInitialScan,
			)
			if err != nil {
				return nil, nil, hlc.Timestamp{}, nil, err
			}
			telemetry.CountBucketed(telemetryPath+`.added_targets`, int64(len(v.Targets)))
		case *tree.AlterChangefeedDropTarget:
			if err := checkIfCommandAllowed(); err != nil {
				return nil, nil, hlc.Timestamp{}, nil, err
			}

			for _, target := range v.Targets {
				desc, found, err := getTargetDesc(ctx, p, descResolver, target.TableName)
				if err != nil {
					return nil, nil, hlc.Timestamp{}, nil, err
				}
				if !found {
					if id, wasDeleted := noLongerExist[target.TableName.String()]; wasDeleted {
						// Failed to lookup table because it was deleted.
						k := targetKey{TableID: id, FamilyName: target.FamilyName}
						droppedTargets[k] = target
						continue
					} else {
						return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
							pgcode.InvalidParameterValue,
							`target %q does not exist`,
							tree.ErrString(&target),
						)
					}
				}
				k := targetKey{TableID: desc.GetID(), FamilyName: target.FamilyName}
				droppedTargets[k] = target
				_, recognized := newTargets[k]
				if !recognized {
					return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q already not watched by changefeed`,
						tree.ErrString(&target),
					)
				}
				newTableDescs[desc.GetID()] = desc
				delete(newTargets, k)
			}
			telemetry.CountBucketed(telemetryPath+`.dropped_targets`, int64(len(v.Targets)))
		}
	}

	// Remove tables from the job progress if and only if the number of
	// targets referencing them has fallen to zero. For example, we might
	// drop one column family from a table and add another at the same time,
	// and since we watch entire table spans the set of spans won't change.
	if len(droppedTargets) > 0 {
		addedTargets := make(map[descpb.ID]struct{}, len(newTargets))
		for k := range newTargets {
			addedTargets[k.TableID] = struct{}{}
		}
		droppedSpanIDs := make([]spanID, 0, len(droppedTargets))
		for k := range droppedTargets {
			if _, wasAdded := addedTargets[k.TableID]; !wasAdded {
				// For dropped tables, we might not have the desc anymore so
				// we can't get the index ID. In any case, it's safe to wipe
				// out the entire table span.
				droppedSpanIDs = append(droppedSpanIDs, spanID{
					tableID: k.TableID,
				})
			}
		}
		droppedTargetSpans := fetchSpansForDescs(p, droppedSpanIDs)
		if err := removeSpansFromProgress(newJobProgress, droppedTargetSpans); err != nil {
			return nil, nil, hlc.Timestamp{}, nil, err
		}
	}

	newTargetList := tree.ChangefeedTableTargets{}

	for _, target := range newTargets {
		newTargetList = append(newTargetList, target)
	}

	hasSelectPrivOnAllTables := true
	hasChangefeedPrivOnAllTables := true
	for _, desc := range newTableDescs {
		hasSelect, hasChangefeed, err := checkPrivilegesForDescriptor(ctx, p, desc)
		if err != nil {
			return nil, nil, hlc.Timestamp{}, nil, err
		}
		hasSelectPrivOnAllTables = hasSelectPrivOnAllTables && hasSelect
		hasChangefeedPrivOnAllTables = hasChangefeedPrivOnAllTables && hasChangefeed
	}
	if err := authorizeUserToCreateChangefeed(
		ctx, p, sinkURI, hasSelectPrivOnAllTables, hasChangefeedPrivOnAllTables, tree.ChangefeedLevelTable); err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}

	if err := validateNewTargets(ctx, p, newTargetList, newJobProgress, newJobStatementTime); err != nil {
		return nil, nil, hlc.Timestamp{}, nil, err
	}

	return newTargetList, &newJobProgress, newJobStatementTime, originalSpecs, nil
}

func validateNewTargets(
	ctx context.Context,
	p sql.PlanHookState,
	newTargets tree.ChangefeedTableTargets,
	jobProgress jobspb.Progress,
	jobStatementTime hlc.Timestamp,
) error {
	if len(newTargets) == 0 {
		return pgerror.New(pgcode.InvalidParameterValue, "cannot drop all targets")
	}

	// when we resume the changefeed, we need to ensure that the newly added
	// targets can be resolved at the time of the high watermark. If the high
	// watermark is empty, then we need to ensure that the newly added targets can
	// be resolved at the StatementTime of the changefeed job.
	var resolveTime hlc.Timestamp
	highWater := jobProgress.GetHighWater()
	if highWater != nil && !highWater.IsEmpty() {
		resolveTime = *highWater
	} else {
		resolveTime = jobStatementTime
	}

	allDescs, err := backupresolver.LoadAllDescs(ctx, p.ExecCfg(), resolveTime)
	if err != nil {
		return errors.Wrap(err, `error while validating new targets`)
	}
	descResolver, err := backupresolver.NewDescriptorResolver(allDescs)
	if err != nil {
		return errors.Wrap(err, `error while validating new targets`)
	}

	for _, target := range newTargets {
		targetName := target.TableName
		_, found, err := getTargetDesc(ctx, p, descResolver, targetName)
		if err != nil {
			return errors.Wrap(err, `error while validating new targets`)
		}
		if !found {
			if highWater != nil && !highWater.IsEmpty() {
				return errors.Errorf(`target %q cannot be resolved as of the high water mark. `+
					`Please wait until the high water mark progresses past the creation time of this target in order to add it to the changefeed.`,
					tree.ErrString(targetName),
				)
			}
			return errors.Errorf(`target %q cannot be resolved as of the creation time of the changefeed. `+
				`Please wait until the high water mark progresses past the creation time of this target in order to add it to the changefeed.`,
				tree.ErrString(targetName),
			)
		}
	}

	return nil
}

// generateNewProgress determines if the progress of a changefeed job needs to
// be updated based on the targets that have been added, the options associated
// with each target we are adding/removing (i.e. with initial_scan or
// no_initial_scan), and the current status of the job. If the progress does not
// need to be updated, we will simply return the previous progress and statement
// time that is passed into the function.
func generateNewProgress(
	prevProgress jobspb.Progress,
	prevStatementTime hlc.Timestamp,
	existingTargetSpans []roachpb.Span,
	newSpans []roachpb.Span,
	withInitialScan bool,
) (jobspb.Progress, hlc.Timestamp, error) {
	prevHighWater := prevProgress.GetHighWater()
	changefeedProgress := prevProgress.GetChangefeed()

	haveHighwater := prevHighWater != nil && prevHighWater.IsSet()
	// TODO(#142376): Whether a checkpoint exists seems orthogonal to what
	// we do in this function. Consider removing this flag.
	haveCheckpoint := changefeedProgress != nil && !changefeedProgress.SpanLevelCheckpoint.IsEmpty()

	// Check if the progress does not need to be updated. The progress does not
	// need to be updated if:
	// * the high watermark is empty, and we would like to perform an initial scan.
	// * the high watermark is non-empty, the checkpoint is empty, and we do not want to
	//   perform an initial scan.
	// TODO(#142376): Consider in the scenario where we have a highwater whether
	// we should starting sending events for the new table starting at the ALTER
	// CHANGEFEED statement time instead of the current highwater.
	if (!haveHighwater && withInitialScan) || (haveHighwater && !haveCheckpoint && !withInitialScan) {
		return prevProgress, prevStatementTime, nil
	}

	// Check if the user is trying to perform an initial scan during a
	// non-initial backfill.
	if haveHighwater && haveCheckpoint && withInitialScan {
		return prevProgress, prevStatementTime, errors.Errorf(
			`cannot perform initial scan on newly added targets while the checkpoint is non-empty, `+
				`please unpause the changefeed and wait until the high watermark progresses past the current value %s to add these targets.`,
			eval.TimestampToDecimalDatum(*prevHighWater).Decimal.String(),
		)
	}

	// TODO(#142369): We should create a new PTS record instead of just
	// copying the old one.
	var ptsRecord uuid.UUID
	if changefeedProgress != nil {
		ptsRecord = changefeedProgress.ProtectedTimestampRecord
	}

	// Check if the user is trying to perform an initial scan while the high
	// watermark is non-empty but the checkpoint is empty.
	if haveHighwater && !haveCheckpoint && withInitialScan {
		// If we would like to perform an initial scan on the new targets,
		// we need to reset the high watermark. However, by resetting the high
		// watermark, the initial scan will be performed on existing targets as well.
		// To avoid this, we update the statement time of the job to the previous high
		// watermark, and add all the existing targets to the checkpoint to skip the
		// initial scan on these targets.
		// TODO(#142376): Consider whether we want to set the new statement time
		// to the actual new statement time (ALTER CHANGEFEED statement time).
		newStatementTime := *prevHighWater

		newProgress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{
					ProtectedTimestampRecord: ptsRecord,
					SpanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
						newStatementTime: existingTargetSpans,
					}),
				},
			},
		}

		return newProgress, newStatementTime, nil
	}

	// At this point, we are left with one of two cases:
	// * the high watermark is empty, and we do not want to perform
	//   an initial scan on the new targets.
	// * the high watermark is non-empty, the checkpoint is non-empty,
	//   and we do not want to perform an initial scan on the new targets.
	// In either case, we need to update the checkpoint to include the spans
	// of the newly added targets so that the changefeed will skip performing
	// a backfill on these targets.

	// TODO(#142376): In the case where we have a highwater, we will resend
	// events since the original statement time for all existing tables.
	// We might want to change this. We might also want to change whether
	// the events for the new table start at the ALTER CHANGEFEED statement
	// time instead.

	spanLevelCheckpoint, err := getSpanLevelCheckpointFromProgress(prevProgress)
	if err != nil {
		return jobspb.Progress{}, hlc.Timestamp{}, err
	}

	checkpointSpansMap := maps.Collect(spanLevelCheckpoint.All())
	var spanGroup roachpb.SpanGroup
	spanGroup.Add(checkpointSpansMap[prevStatementTime]...)
	spanGroup.Add(newSpans...)
	checkpointSpansMap[prevStatementTime] = spanGroup.Slice()

	newProgress := jobspb.Progress{
		Progress: &jobspb.Progress_HighWater{},
		Details: &jobspb.Progress_Changefeed{
			Changefeed: &jobspb.ChangefeedProgress{
				ProtectedTimestampRecord: ptsRecord,
				SpanLevelCheckpoint:      jobspb.NewTimestampSpansMap(checkpointSpansMap),
			},
		},
	}
	return newProgress, prevStatementTime, nil
}

// storeFilterChangeFrontierForDatabaseChangefeed uses the persistent job frontier to ensure that
// when we resume the changefeed, new tables being watched by the database level
// feed (due to ALTER CHANGEFEED changing the filter options) are watched only
// from the time of the ALTER CHANGEFEED statement.
func storeFilterChangeFrontierForDatabaseChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	jobID jobspb.JobID,
	newDetails jobspb.ChangefeedDetails,
	prevDetails jobspb.ChangefeedDetails,
) error {
	// For database level changefeeds, we may have set a new value for
	// the filter options. In this case, the set of tables we are watching
	// may have changed. We need to store the new frontier so that we only
	// emit events for the new tables from the time of the ALTER CHANGEFEED
	// statement, but not before.
	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}

	getSpans := func(details jobspb.ChangefeedDetails) ([]roachpb.Span, error) {
		targets, err := AllTargets(ctx, details, p.ExecCfg(), statementTime)
		if err != nil {
			return nil, err
		}
		tableDescs, err := fetchTableDescriptors(ctx, p.ExecCfg(), targets, statementTime)
		if err != nil {
			return nil, err
		}
		spans, err := fetchSpansForTables(ctx, p, tableDescs, details, statementTime)
		if err != nil {
			return nil, err
		}
		return spans, nil
	}

	// We compute the difference between the spans we would have been tracking
	// at the time of the ALTER CHANGEFEED statement and the spans we expect to
	// be tracking at that time with the new filter.
	trackedSpansBeforeAlter, err := getSpans(prevDetails)
	if err != nil {
		return err
	}
	// Note that these spans are computed based on the spans we expect to be
	// tracking when processing events from the time of the ALTER CHANGEFEED
	// statement. This may be different than the spans/targets we will be
	// tracking when the changefeed is resumed (which will be computed based on
	// the highwater). This will happen if a table that is matched by the filter
	// is created between the highwater and the ALTER CHANGEFEED statement.
	trackedSpansAfterAlter, err := getSpans(newDetails)
	if err != nil {
		return err
	}

	var newTrackedSpans roachpb.SpanGroup
	newTrackedSpans.Add(trackedSpansAfterAlter...)
	newTrackedSpans.Sub(trackedSpansBeforeAlter...)

	if newTrackedSpans.Len() == 0 {
		return nil
	}

	frontier, found, err := jobfrontier.Get(ctx, p.InternalSQLTxn(), jobID, `alter_changefeed`)
	if err != nil {
		return err
	}
	if !found {
		frontier, err = span.MakeFrontier()
		if err != nil {
			return err
		}
	}

	if err := frontier.AddSpansAt(statementTime, newTrackedSpans.Slice()...); err != nil {
		return err
	}

	return jobfrontier.Store(ctx, p.InternalSQLTxn(), jobID, `alter_changefeed`, frontier)
}

func removeSpansFromProgress(progress jobspb.Progress, spansToRemove []roachpb.Span) error {
	spanLevelCheckpoint, err := getSpanLevelCheckpointFromProgress(progress)
	if err != nil {
		return err
	}
	if spanLevelCheckpoint == nil {
		return nil
	}
	checkpointSpansMap := make(map[hlc.Timestamp]roachpb.Spans)
	for ts, sp := range spanLevelCheckpoint.All() {
		var spanGroup roachpb.SpanGroup
		spanGroup.Add(sp...)
		spanGroup.Sub(spansToRemove...)
		if spans := spanGroup.Slice(); len(spans) > 0 {
			checkpointSpansMap[ts] = spans
		}
	}
	progress.GetChangefeed().SpanLevelCheckpoint = jobspb.NewTimestampSpansMap(checkpointSpansMap)

	return nil
}

func getSpanLevelCheckpointFromProgress(
	progress jobspb.Progress,
) (*jobspb.TimestampSpansMap, error) {
	changefeedProgress := progress.GetChangefeed()
	if changefeedProgress == nil {
		return nil, nil
	}
	return changefeedProgress.SpanLevelCheckpoint, nil
}

type spanID struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

func fetchSpansForDescs(p sql.PlanHookState, spanIDs []spanID) (primarySpans []roachpb.Span) {
	seen := make(map[spanID]struct{})
	codec := p.ExtendedEvalContext().Codec
	for _, id := range spanIDs {
		if _, isDup := seen[id]; isDup {
			continue
		}
		seen[id] = struct{}{}
		primarySpan := func() roachpb.Span {
			if id.indexID == 0 {
				tablePrefix := codec.TablePrefix(uint32(id.tableID))
				return roachpb.Span{
					Key:    tablePrefix,
					EndKey: tablePrefix.PrefixEnd(),
				}
			}
			indexPrefix := codec.IndexPrefix(uint32(id.tableID), uint32(id.indexID))
			return roachpb.Span{
				Key:    indexPrefix,
				EndKey: indexPrefix.PrefixEnd(),
			}
		}()
		primarySpans = append(primarySpans, primarySpan)
	}
	return primarySpans
}

// parseFilterOptionFromTargetSpec parses the existing filter option from the
// target specification so that it can be used to generate the new filter option.
func parseFilterOptionFromTargetSpec(
	targetSpec jobspb.ChangefeedTargetSpecification,
) (tree.ChangefeedFilterOption, error) {
	var tables tree.TableNames
	if targetSpec.FilterList == nil {
		return tree.ChangefeedFilterOption{}, errors.AssertionFailedf("filter list is nil")
	}
	for table := range targetSpec.FilterList.Tables {
		// Parse the fully-qualified table name string back into a TableName object.
		unresolvedName, err := parser.ParseTableName(table)
		if err != nil {
			return tree.ChangefeedFilterOption{}, err
		}
		tableName := unresolvedName.ToTableName()
		tables = append(tables, tableName)
	}
	return tree.ChangefeedFilterOption{
		FilterType: targetSpec.FilterList.FilterType,
		Tables:     tables,
	}, nil
}

// generateNewFilters processes alter changefeed commands and extracts filter changes.
// It returns a slice of Filter structs containing all filter modifications.
func generateNewFilters(
	alterCmds tree.AlterChangefeedCmds,
) (filters []tree.ChangefeedFilterOption) {
	for _, cmd := range alterCmds {
		switch v := cmd.(type) {
		case *tree.AlterChangefeedSetFilterOption:
			filters = append(filters, tree.ChangefeedFilterOption{
				FilterType: v.ChangefeedFilterOption.FilterType,
				Tables:     v.ChangefeedFilterOption.Tables,
			})
		case *tree.AlterChangefeedUnsetFilterOption:
			// Here we do not return the default changefeed filter option (Exclude
			// with an empty tables list) so we can treat UNSET INCLUDE TABLES and
			// UNSET EXCLUDE TABLES differently. For example, if we create a feed
			// with EXCLUDE TABLES foo, bar, and then UNSET EXCLUDE TABLES, that
			// would succeed. However, we expect UNSET INCLUDE TABLES to fail,
			// even though both correspond to the empty filter state.
			filters = append(filters, tree.ChangefeedFilterOption{
				FilterType: v.ChangefeedFilterOption.FilterType,
			})
		}
	}

	return filters
}

func getPrevOpts(prevDescription string, opts map[string]string) (map[string]string, error) {
	prevStmt, err := parser.ParseOne(prevDescription)
	if err != nil {
		return nil, err
	}

	prevChangefeedStmt, ok := prevStmt.AST.(*tree.CreateChangefeed)
	if !ok {
		return nil, errors.Errorf(`could not parse job description`)
	}

	prevOpts := make(map[string]string, len(prevChangefeedStmt.Options))
	for _, opt := range prevChangefeedStmt.Options {
		prevOpts[opt.Key.String()] = opts[opt.Key.String()]
	}

	return prevOpts, nil
}
