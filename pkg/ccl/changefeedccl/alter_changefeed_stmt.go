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

	var resumeTS hlc.Timestamp
	highWater := newProgress.GetHighWater()
	if highWater != nil && !highWater.IsEmpty() {
		resumeTS = *highWater
	} else {
		resumeTS = newStatementTime
	}

	annotatedStmt := &annotatedChangefeedStatement{
		CreateChangefeed:    newChangefeedStmt,
		alterChangefeedAsOf: resumeTS,
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

	prevProgress := job.Progress()
	var prevHighWater hlc.Timestamp
	if hw := prevProgress.GetHighWater(); hw != nil {
		prevHighWater = *hw
	}
	resumeTS := prevHighWater
	if resumeTS.IsEmpty() {
		resumeTS = prevDetails.StatementTime
	}

	newTargets, originalSpecs, primaryIndexIDs, tableOps, err :=
		generateAndValidateNewTargetsForTableLevelFeed(
			ctx, exprEval, p, alterChangefeedStmt.Cmds, newOptions,
			prevDetails, resumeTS, newSinkURI,
		)
	if err != nil {
		return err
	}

	// Compute the new progress based on the table ops.
	newProgress, err := generateNewProgressForTableOps(
		p, prevProgress, prevHighWater, resumeTS, primaryIndexIDs, tableOps,
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

	annotatedStmt := &annotatedChangefeedStatement{
		CreateChangefeed:    newChangefeedStmt,
		originalSpecs:       originalSpecs,
		alterChangefeedAsOf: resumeTS,
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
	newDetails.StatementTime = resumeTS

	// For table-level feeds, set the initial scan option to its default
	// value of "yes" so that we can do an initial scan for new targets.
	newDetails.Opts[changefeedbase.OptInitialScan] = ``

	return finalizeAlterChangefeed(
		ctx, p, job, jobID, jobRecord, newDetails, &newProgress, targets, resultsCh,
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

// targetOp describes what happened to a target during an ALTER CHANGEFEED.
type targetOp int

// The targetOp's are sorted in order of increasing precedence (i.e. which op
// wins for a table when multiple families are changed).
const (
	_ targetOp = iota
	// targetOpDrop indicates the target was dropped.
	targetOpDrop
	// targetOpAdd indicates the target was added without an initial scan.
	targetOpAdd
	// targetOpAddWithInitialScan indicates the target was added with
	// an initial scan requested.
	targetOpAddWithInitialScan
)

// generateAndValidateNewTargetsForTableLevelFeed processes all ADD/DROP
// commands for a table-level changefeed and returns the new target list,
// statement time, original specs, and a map describing which tables were
// added or dropped. Commands are processed in order with last-writer-wins
// semantics (e.g. DROP then ADD re-adds the table). Adding a table that
// is already watched, or dropping a table that is not watched, returns
// an error. The caller is responsible for updating the job progress
// based on the returned tableOps map.
//
// resumeTS is the timestamp the changefeed will resume from (highwater or
// original statement time).
func generateAndValidateNewTargetsForTableLevelFeed(
	ctx context.Context,
	exprEval exprutil.Evaluator,
	p sql.PlanHookState,
	alterCmds tree.AlterChangefeedCmds,
	opts changefeedbase.StatementOptions,
	prevDetails jobspb.ChangefeedDetails,
	resumeTS hlc.Timestamp,
	sinkURI string,
) (
	newTargetList tree.ChangefeedTableTargets,
	originalSpecs map[tree.ChangefeedTableTarget]jobspb.ChangefeedTargetSpecification,
	primaryIndexIDs map[descpb.ID]descpb.IndexID,
	tableOps map[descpb.ID]targetOp,
	err error,
) {
	type targetKey struct {
		TableID    descpb.ID
		FamilyName tree.Name
	}
	newTargets := make(map[targetKey]tree.ChangefeedTableTarget)
	newTableDescs := make(map[descpb.ID]catalog.Descriptor)
	// targetOps tracks ADD/DROP per family target during the command
	// loop. After all commands are processed, these are consolidated
	// into the returned table-level tableOps map.
	targetOps := make(map[targetKey]targetOp)

	// originalSpecs provides a mapping between tree.ChangefeedTargets that
	// existed prior to the alteration of the changefeed to their corresponding
	// jobspb.ChangefeedTargetSpecification. The purpose of this mapping is to
	// ensure that the StatementTimeName of the existing targets are not modified
	// when the name of the target was modified.
	originalSpecs = make(map[tree.ChangefeedTableTarget]jobspb.ChangefeedTargetSpecification)

	// We want to store the value of whether or not the original changefeed had
	// initial_scan set to only so that we only do an initial scan on an alter
	// changefeed with initial_scan = 'only' if the original one also had
	// initial_scan = 'only'.
	originalInitialScanType, err := opts.GetInitialScanType()
	if err != nil {
		return nil, nil, nil, nil, err
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

	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}

	// We attempt to resolve the changefeed targets as of the current time to
	// ensure that all targets exist. However, we also need to make sure that
	// all targets can be resolved at the time in which the changefeed is
	// resumed. We perform these validations in validateNewTargetsAtResumeTime.
	allDescs, err := backupresolver.LoadAllDescs(ctx, p.ExecCfg(), statementTime)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	descResolver, err := backupresolver.NewDescriptorResolver(allDescs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	prevTargets, err := AllTargets(ctx, prevDetails, p.ExecCfg(), resumeTS)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	noLongerExist := make(map[string]descpb.ID)
	if err := prevTargets.EachTarget(func(targetSpec changefeedbase.Target) error {
		k := targetKey{TableID: targetSpec.DescID, FamilyName: tree.Name(targetSpec.FamilyName)}
		var desc catalog.TableDescriptor
		if d, exists := descResolver.DescByID[targetSpec.DescID]; exists {
			desc = d.(catalog.TableDescriptor)
		} else {
			// Table was dropped; that's okay since the changefeed likely
			// will handle DROP alter command below; and if not, then we'll
			// resume the changefeed, which will promptly fail if the table
			// no longer exists.
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
		newTableDescs[targetSpec.DescID] = desc

		originalSpecs[newTarget] = jobspb.ChangefeedTargetSpecification{
			Type:              targetSpec.Type,
			DescID:            targetSpec.DescID,
			FamilyName:        targetSpec.FamilyName,
			StatementTimeName: string(targetSpec.StatementTimeName),
		}
		return nil
	}); err != nil {
		return nil, nil, nil, nil, err
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
				return nil, nil, nil, nil, err
			}

			targetOpts, err := exprEval.KVOptions(
				ctx, v.Options, changefeedvalidators.AlterTargetOptionValidations,
			)
			if err != nil {
				return nil, nil, nil, nil, err
			}

			initialScanType, initialScanSet := targetOpts[changefeedbase.OptInitialScan]
			_, noInitialScanSet := targetOpts[changefeedbase.OptNoInitialScan]

			if initialScanType != `` && initialScanType != `yes` && initialScanType != `no` && initialScanType != `only` {
				return nil, nil, nil, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot set %q to %q. possible values for initial_scan are "yes", "no", "only", or no value`,
					changefeedbase.OptInitialScan, initialScanType,
				)
			}

			if initialScanSet && noInitialScanSet {
				return nil, nil, nil, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot specify both %q and %q`, changefeedbase.OptInitialScan,
					changefeedbase.OptNoInitialScan,
				)
			}

			// By default, we will not perform an initial scan on newly added
			// targets. Hence, the user must explicitly state that they want
			// an initial scan performed on the new targets.
			withInitialScan := (initialScanType == `` && initialScanSet) ||
				initialScanType == `yes` ||
				(initialScanType == `only` && originalInitialScanOnlyOption)

			for _, target := range v.Targets {
				desc, found, err := getTargetDesc(ctx, p, descResolver, target.TableName)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				if !found {
					return nil, nil, nil, nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q does not exist`,
						tree.ErrString(&target),
					)
				}

				k := targetKey{TableID: desc.GetID(), FamilyName: target.FamilyName}
				if _, exists := newTargets[k]; exists && targetOps[k] != targetOpDrop {
					return nil, nil, nil, nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q already watched by changefeed`,
						tree.ErrString(&target),
					)
				}
				newTargets[k] = target
				newTableDescs[desc.GetID()] = desc
				if withInitialScan {
					targetOps[k] = targetOpAddWithInitialScan
				} else {
					targetOps[k] = targetOpAdd
				}
			}
			telemetry.CountBucketed(telemetryPath+`.added_targets`, int64(len(v.Targets)))
		case *tree.AlterChangefeedDropTarget:
			if err := checkIfCommandAllowed(); err != nil {
				return nil, nil, nil, nil, err
			}

			for _, target := range v.Targets {
				desc, found, err := getTargetDesc(ctx, p, descResolver, target.TableName)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				if !found {
					if id, wasDeleted := noLongerExist[target.TableName.String()]; wasDeleted {
						// Failed to lookup table because it was deleted.
						k := targetKey{TableID: id, FamilyName: target.FamilyName}
						delete(newTargets, k)
						targetOps[k] = targetOpDrop
						continue
					} else {
						return nil, nil, nil, nil, pgerror.Newf(
							pgcode.InvalidParameterValue,
							`target %q does not exist`,
							tree.ErrString(&target),
						)
					}
				}
				k := targetKey{TableID: desc.GetID(), FamilyName: target.FamilyName}
				_, recognized := newTargets[k]
				if !recognized {
					return nil, nil, nil, nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q already not watched by changefeed`,
						tree.ErrString(&target),
					)
				}
				delete(newTargets, k)
				targetOps[k] = targetOpDrop
			}
			telemetry.CountBucketed(telemetryPath+`.dropped_targets`, int64(len(v.Targets)))
		}
	}

	// Build the final target list.
	for _, target := range newTargets {
		newTargetList = append(newTargetList, target)
	}

	// Validate targets at the resume timestamp and get the primary index
	// IDs as of that time.
	primaryIndexIDs, err = validateNewTargetsAtResumeTime(
		ctx, p, newTargetList, resumeTS)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Consolidate per-family targetOps into table-level tableOps
	// following the order of precedence defined for targetOp's.
	tableOps = make(map[descpb.ID]targetOp)
	for k, op := range targetOps {
		if op > tableOps[k.TableID] {
			tableOps[k.TableID] = op
		}
	}

	hasSelectPrivOnAllTables := true
	hasChangefeedPrivOnAllTables := true
	for _, desc := range newTableDescs {
		hasSelect, hasChangefeed, err := checkPrivilegesForDescriptor(ctx, p, desc)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		hasSelectPrivOnAllTables = hasSelectPrivOnAllTables && hasSelect
		hasChangefeedPrivOnAllTables = hasChangefeedPrivOnAllTables && hasChangefeed
	}
	if err := authorizeUserToCreateChangefeed(
		ctx, p, sinkURI, hasSelectPrivOnAllTables, hasChangefeedPrivOnAllTables, tree.ChangefeedLevelTable); err != nil {
		return nil, nil, nil, nil, err
	}

	return newTargetList, originalSpecs, primaryIndexIDs, tableOps, nil
}

// validateNewTargetsAtResumeTime validates that all targets can be resolved
// at the given resumeTS (the changefeed's highwater or original statement
// time). It also returns a map of table IDs to their primary index IDs as of
// resumeTS, which should be used for span computation since that's the
// timestamp the changefeed will resume from.
func validateNewTargetsAtResumeTime(
	ctx context.Context,
	p sql.PlanHookState,
	newTargets tree.ChangefeedTableTargets,
	resumeTS hlc.Timestamp,
) (map[descpb.ID]descpb.IndexID, error) {
	if len(newTargets) == 0 {
		return nil, pgerror.New(
			pgcode.InvalidParameterValue, "cannot drop all targets")
	}

	allDescs, err := backupresolver.LoadAllDescs(ctx, p.ExecCfg(), resumeTS)
	if err != nil {
		return nil, errors.Wrap(err, `error while validating new targets`)
	}
	descResolver, err := backupresolver.NewDescriptorResolver(allDescs)
	if err != nil {
		return nil, errors.Wrap(err, `error while validating new targets`)
	}

	primaryIndexIDs := make(map[descpb.ID]descpb.IndexID, len(newTargets))
	for _, target := range newTargets {
		targetName := target.TableName
		desc, found, err := getTargetDesc(ctx, p, descResolver, targetName)
		if err != nil {
			return nil, errors.Wrap(err, `error while validating new targets`)
		}
		if !found {
			return nil, errors.Errorf(
				`target %q cannot be resolved as of the resume time %s. `+
					`Please wait until the high water mark progresses past the creation `+
					`time of this target in order to add it to the changefeed.`,
				tree.ErrString(targetName), resumeTS,
			)
		}
		tableDesc, ok := desc.(catalog.TableDescriptor)
		if !ok {
			return nil, errors.AssertionFailedf(
				"expected table descriptor for %s", targetName)
		}
		primaryIndexIDs[tableDesc.GetID()] = tableDesc.GetPrimaryIndexID()
	}

	return primaryIndexIDs, nil
}

// generateNewProgressForTableOps computes the new job progress for the
// given table operations. It modifies the checkpoint to ensure that we
// don't do any extra initial scans of any targets.
func generateNewProgressForTableOps(
	p sql.PlanHookState,
	prevProgress jobspb.Progress,
	prevHighWater hlc.Timestamp,
	resumeTS hlc.Timestamp,
	primaryIndexIDs map[descpb.ID]descpb.IndexID,
	tableOps map[descpb.ID]targetOp,
) (jobspb.Progress, error) {
	if len(tableOps) == 0 {
		return prevProgress, nil
	}

	var ptsRecord uuid.UUID
	var checkpoint *jobspb.TimestampSpansMap
	if changefeedProgress := prevProgress.GetChangefeed(); changefeedProgress != nil {
		// TODO(#142369): We should create a new PTS record instead of just
		// keeping the old one.
		ptsRecord = changefeedProgress.ProtectedTimestampRecord
		checkpoint = changefeedProgress.SpanLevelCheckpoint
	}

	var alteredSpanIDs, addedWithoutScanSpanIDs, addedWithScanSpanIDs []spanID
	for id, op := range tableOps {
		spID := spanID{
			tableID: id,
			indexID: primaryIndexIDs[id],
		}
		alteredSpanIDs = append(alteredSpanIDs, spID)
		switch op {
		case targetOpAdd:
			addedWithoutScanSpanIDs = append(addedWithoutScanSpanIDs, spID)
		case targetOpAddWithInitialScan:
			addedWithScanSpanIDs = append(addedWithScanSpanIDs, spID)
		default:
		}
	}
	alteredSpans := fetchSpansForDescs(p, alteredSpanIDs)
	addedWithoutScanSpans := fetchSpansForDescs(p, addedWithoutScanSpanIDs)

	// Remove all altered spans from the checkpoint.
	checkpoint = removeSpansFromCheckpoint(checkpoint, alteredSpans)

	// If we're still in an initial scan, all we need to do is add any new
	// targets that don't need to be scanned to the checkpoint.
	if prevHighWater.IsEmpty() {
		checkpoint = addSpansToCheckpoint(checkpoint, addedWithoutScanSpans, resumeTS)
		return jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{
					ProtectedTimestampRecord: ptsRecord,
					SpanLevelCheckpoint:      checkpoint,
				},
			},
		}, nil
	}

	// If we're done the initial scan already and don't need to do an initial scan
	// for any of the new targets, we don't need to do anything else.
	if len(addedWithScanSpanIDs) == 0 {
		return jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{
				HighWater: &prevHighWater,
			},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{
					ProtectedTimestampRecord: ptsRecord,
					SpanLevelCheckpoint:      checkpoint,
				},
			},
		}, nil
	}

	// Otherwise, we'll need to reset the highwater so that we do an initial scan
	// for all the new targets that need scans. We ensure we don't re-scan any
	// the existing tables or newly added targets that don't need scans by
	// adding them to the checkpoint.
	var unchangedSpanIDs []spanID
	for tableID, indexID := range primaryIndexIDs {
		if _, ok := tableOps[tableID]; !ok {
			unchangedSpanIDs = append(unchangedSpanIDs, spanID{
				tableID: tableID,
				indexID: indexID,
			})
		}
	}
	unchangedSpans := fetchSpansForDescs(p, unchangedSpanIDs)
	checkpoint = addSpansToCheckpoint(checkpoint, unchangedSpans, resumeTS)
	checkpoint = addSpansToCheckpoint(checkpoint, addedWithoutScanSpans, resumeTS)
	return jobspb.Progress{
		Progress: &jobspb.Progress_HighWater{},
		Details: &jobspb.Progress_Changefeed{
			Changefeed: &jobspb.ChangefeedProgress{
				ProtectedTimestampRecord: ptsRecord,
				SpanLevelCheckpoint:      checkpoint,
			},
		},
	}, nil
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

// addSpansToCheckpoint returns a new checkpoint with the given spans
// merged in at the given timestamp.
func addSpansToCheckpoint(
	checkpoint *jobspb.TimestampSpansMap, spans []roachpb.Span, ts hlc.Timestamp,
) *jobspb.TimestampSpansMap {
	checkpointSpansMap := make(map[hlc.Timestamp]roachpb.Spans)
	if checkpoint != nil {
		checkpointSpansMap = maps.Collect(checkpoint.All())
	}
	var spanGroup roachpb.SpanGroup
	spanGroup.Add(checkpointSpansMap[ts]...)
	spanGroup.Add(spans...)
	checkpointSpansMap[ts] = spanGroup.Slice()
	return jobspb.NewTimestampSpansMap(checkpointSpansMap)
}

// removeSpansFromCheckpoint returns a new checkpoint with the given
// spans removed from all timestamps.
func removeSpansFromCheckpoint(
	checkpoint *jobspb.TimestampSpansMap, spans []roachpb.Span,
) *jobspb.TimestampSpansMap {
	if checkpoint == nil || checkpoint.IsEmpty() {
		return checkpoint
	}
	checkpointSpansMap := make(map[hlc.Timestamp]roachpb.Spans)
	for ts, sp := range checkpoint.All() {
		var spanGroup roachpb.SpanGroup
		spanGroup.Add(sp...)
		spanGroup.Sub(spans...)
		if remaining := spanGroup.Slice(); len(remaining) > 0 {
			checkpointSpansMap[ts] = remaining
		}
	}
	return jobspb.NewTimestampSpansMap(checkpointSpansMap)
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
