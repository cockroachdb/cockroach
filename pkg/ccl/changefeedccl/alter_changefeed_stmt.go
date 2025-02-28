// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/backup/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/jobs"
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
		getLegacyPayload := func(ctx context.Context) (*jobspb.Payload, error) {
			return &jobPayload, nil
		}
		err = jobsauth.Authorize(
			ctx, p, jobID, getLegacyPayload, jobPayload.UsernameProto.Decode(), jobPayload.Type(), jobsauth.ControlAccess, globalPrivileges,
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

		newChangefeedStmt := &tree.CreateChangefeed{}

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

		newTargets, newProgress, newStatementTime, originalSpecs, err := generateAndValidateNewTargets(
			ctx, exprEval, p,
			alterChangefeedStmt.Cmds,
			newOptions.AsMap(), // TODO: Remove .AsMap()
			prevDetails, job.Progress(),
			newSinkURI,
		)
		if err != nil {
			return err
		}
		newChangefeedStmt.Targets = newTargets

		if prevDetails.Select != "" {
			query, err := cdceval.ParseChangefeedExpression(prevDetails.Select)
			if err != nil {
				return err
			}
			newChangefeedStmt.Select = query
		}

		for key, value := range newOptions.AsMap() {
			opt := tree.KVOption{Key: tree.Name(key)}
			if len(value) > 0 {
				opt.Value = tree.NewDString(value)
			}
			newChangefeedStmt.Options = append(newChangefeedStmt.Options, opt)
		}
		newChangefeedStmt.SinkURI = tree.NewDString(newSinkURI)

		// We validate that all the tables are resolvable at the
		// resolveTime below in validateNewTargets. resolveTime is also
		// the time from which changefeed will resume. Therefore we
		// will override with this time in createChangefeedJobRecord
		// when we get table descriptors.
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

		newDescription, err := makeChangefeedDescription(ctx, annotatedStmt.CreateChangefeed, newSinkURI, newOptions)
		if err != nil {
			return err
		}

		jobRecord, err := createChangefeedJobRecord(
			ctx,
			p,
			annotatedStmt,
			newDescription,
			newSinkURI,
			newOptions,
			jobID,
			``,
		)
		if err != nil {
			return errors.Wrap(err, `failed to alter changefeed`)
		}

		newDetails := jobRecord.Details.(jobspb.ChangefeedDetails)
		newDetails.Opts[changefeedbase.OptInitialScan] = ``

		// newStatementTime will either be the StatementTime of the job prior to the
		// alteration, or it will be the high watermark of the job.
		newDetails.StatementTime = newStatementTime

		newPayload := job.Payload()
		newPayload.Details = jobspb.WrapPayloadDetails(newDetails)
		newPayload.Description = jobRecord.Description
		newPayload.DescriptorIDs = jobRecord.DescriptorIDs
		newExpiration, err := newOptions.GetPTSExpiration()
		if err != nil {
			return err
		}
		newPayload.MaximumPTSAge = newExpiration
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

	return fn, alterChangefeedHeader, false, nil
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

func generateAndValidateNewTargets(
	ctx context.Context,
	exprEval exprutil.Evaluator,
	p sql.PlanHookState,
	alterCmds tree.AlterChangefeedCmds,
	opts map[string]string,
	prevDetails jobspb.ChangefeedDetails,
	prevProgress jobspb.Progress,
	sinkURI string,
) (
	tree.ChangefeedTargets,
	*jobspb.Progress,
	hlc.Timestamp,
	map[tree.ChangefeedTarget]jobspb.ChangefeedTargetSpecification,
	error,
) {

	type targetKey struct {
		TableID    descpb.ID
		FamilyName tree.Name
	}
	newTargets := make(map[targetKey]tree.ChangefeedTarget)
	droppedTargets := make(map[targetKey]tree.ChangefeedTarget)
	newTableDescs := make(map[descpb.ID]catalog.Descriptor)

	// originalSpecs provides a mapping between tree.ChangefeedTargets that
	// existed prior to the alteration of the changefeed to their corresponding
	// jobspb.ChangefeedTargetSpecification. The purpose of this mapping is to ensure
	// that the StatementTimeName of the existing targets are not modified when the
	// name of the target was modified.
	originalSpecs := make(map[tree.ChangefeedTarget]jobspb.ChangefeedTargetSpecification)

	// We want to store the value of whether or not the original changefeed had
	// initial_scan set to only so that we only do an initial scan on an alter
	// changefeed with initial_scan = 'only' if the original one also had
	// initial_scan = 'only'.
	_, originalInitialScanOnlyOption := opts[changefeedbase.OptInitialScanOnly]

	// When we add new targets with or without initial scans, indicating
	// initial_scan or no_initial_scan in the job description would lose its
	// meaning. Hence, we will omit these details from the changefeed
	// description. However, to ensure that we do perform the initial scan on
	// newly added targets, we will introduce the initial_scan opt after the
	// job record is created.

	delete(opts, changefeedbase.OptInitialScanOnly)
	delete(opts, changefeedbase.OptNoInitialScan)
	delete(opts, changefeedbase.OptInitialScan)

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

	prevTargets := AllTargets(prevDetails)
	noLongerExist := make(map[string]descpb.ID)
	err = prevTargets.EachTarget(func(targetSpec changefeedbase.Target) error {
		k := targetKey{TableID: targetSpec.TableID, FamilyName: tree.Name(targetSpec.FamilyName)}
		var desc catalog.TableDescriptor
		if d, exists := descResolver.DescByID[targetSpec.TableID]; exists {
			desc = d.(catalog.TableDescriptor)
		} else {
			// Table was dropped; that's okay since the changefeed likely
			// will handle DROP alter command below; and if not, then we'll resume
			// the changefeed, which will promptly fail if the table no longer exist.
			noLongerExist[string(targetSpec.StatementTimeName)] = targetSpec.TableID
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

		newTarget := tree.ChangefeedTarget{
			TableName:  tablePattern,
			FamilyName: tree.Name(targetSpec.FamilyName),
		}
		newTargets[k] = newTarget
		newTableDescs[targetSpec.TableID] = descResolver.DescByID[targetSpec.TableID]

		originalSpecs[newTarget] = jobspb.ChangefeedTargetSpecification{
			Type:              targetSpec.Type,
			TableID:           targetSpec.TableID,
			FamilyName:        targetSpec.FamilyName,
			StatementTimeName: string(targetSpec.StatementTimeName),
		}
		return nil
	})

	if err != nil {
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

			var withInitialScan bool
			initialScanType, initialScanSet := targetOpts[changefeedbase.OptInitialScan]
			_, noInitialScanSet := targetOpts[changefeedbase.OptNoInitialScan]
			_, initialScanOnlySet := targetOpts[changefeedbase.OptInitialScanOnly]

			if initialScanSet {
				if initialScanType == `no` || (initialScanType == `only` && !originalInitialScanOnlyOption) {
					withInitialScan = false
				} else {
					withInitialScan = true
				}
			} else {
				withInitialScan = false
			}

			if initialScanType != `` && initialScanType != `yes` && initialScanType != `no` && initialScanType != `only` {
				return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot set initial_scan to %q. possible values for initial_scan are "yes", "no", "only", or no value`, changefeedbase.OptInitialScan,
				)
			}

			if initialScanSet && noInitialScanSet {
				return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot specify both %q and %q`, changefeedbase.OptInitialScan,
					changefeedbase.OptNoInitialScan,
				)
			}

			if initialScanSet && initialScanOnlySet {
				return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot specify both %q and %q`, changefeedbase.OptInitialScan,
					changefeedbase.OptInitialScanOnly,
				)
			}

			if noInitialScanSet && initialScanOnlySet {
				return nil, nil, hlc.Timestamp{}, nil, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot specify both %q and %q`, changefeedbase.OptInitialScanOnly,
					changefeedbase.OptNoInitialScan,
				)
			}

			var existingTargetIDs []descpb.ID
			for _, targetDesc := range newTableDescs {
				existingTargetIDs = append(existingTargetIDs, targetDesc.GetID())
			}
			existingTargetSpans := fetchSpansForDescs(p, existingTargetIDs)
			var newTargetIDs []descpb.ID
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
				newTargetIDs = append(newTargetIDs, k.TableID)
			}

			addedTargetSpans := fetchSpansForDescs(p, newTargetIDs)

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
		droppedIDs := make([]descpb.ID, 0, len(droppedTargets))
		for k := range droppedTargets {
			if _, wasAdded := addedTargets[k.TableID]; !wasAdded {
				droppedIDs = append(droppedIDs, k.TableID)
			}
		}
		droppedTargetSpans := fetchSpansForDescs(p, droppedIDs)
		removeSpansFromProgress(newJobProgress, droppedTargetSpans)
	}

	newTargetList := tree.ChangefeedTargets{}

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
	if err := authorizeUserToCreateChangefeed(ctx, p, sinkURI, hasSelectPrivOnAllTables, hasChangefeedPrivOnAllTables); err != nil {
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
	newTargets tree.ChangefeedTargets,
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
// TODO(#140509): Update this function to work with the new span-level checkpoint.
func generateNewProgress(
	prevProgress jobspb.Progress,
	prevStatementTime hlc.Timestamp,
	existingTargetSpans []roachpb.Span,
	newSpans []roachpb.Span,
	withInitialScan bool,
) (jobspb.Progress, hlc.Timestamp, error) {
	prevHighWater := prevProgress.GetHighWater()
	changefeedProgress := prevProgress.GetChangefeed()
	ptsRecord := uuid.UUID{}
	if changefeedProgress != nil {
		ptsRecord = changefeedProgress.ProtectedTimestampRecord
	}

	haveHighwater := !(prevHighWater == nil || prevHighWater.IsEmpty())
	haveCheckpoint := changefeedProgress != nil &&
		(!changefeedProgress.Checkpoint.IsEmpty() || !changefeedProgress.SpanLevelCheckpoint.IsEmpty())

	// Check if the progress does not need to be updated. The progress does not
	// need to be updated if:
	// * the high watermark is empty, and we would like to perform an initial scan.
	// * the high watermark is non-empty, the checkpoint is empty, and we do not want to
	//   perform an initial scan.
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

	// Check if the user is trying to perform an initial scan while the high
	// watermark is non-empty but the checkpoint is empty.
	if haveHighwater && !haveCheckpoint && withInitialScan {
		// If we would like to perform an initial scan on the new targets,
		// we need to reset the high watermark. However, by resetting the high
		// watermark, the initial scan will be performed on existing targets as well.
		// To avoid this, we update the statement time of the job to the previous high
		// watermark, and add all the existing targets to the checkpoint to skip the
		// initial scan on these targets.
		newStatementTime := *prevHighWater

		newProgress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{
					//lint:ignore SA1019 deprecated usage
					Checkpoint: &jobspb.ChangefeedProgress_Checkpoint{
						Spans: existingTargetSpans,
						// TODO(#140509): ALTER CHANGEFED should handle fine grained
						// progress and checkpointed timestamp properly.
					},
					ProtectedTimestampRecord: ptsRecord,
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

	var mergedSpanGroup roachpb.SpanGroup
	if haveCheckpoint {
		mergedSpanGroup.Add(changefeedProgress.Checkpoint.Spans...)
	}
	mergedSpanGroup.Add(newSpans...)

	newProgress := jobspb.Progress{
		Progress: &jobspb.Progress_HighWater{},
		Details: &jobspb.Progress_Changefeed{
			Changefeed: &jobspb.ChangefeedProgress{
				//lint:ignore SA1019 deprecated usage
				Checkpoint: &jobspb.ChangefeedProgress_Checkpoint{
					Spans: mergedSpanGroup.Slice(),
				},
				ProtectedTimestampRecord: ptsRecord,
			},
		},
	}
	return newProgress, prevStatementTime, nil
}

// TODO(#140509): Update this function to work with the new span-level checkpoint.
func removeSpansFromProgress(prevProgress jobspb.Progress, spansToRemove []roachpb.Span) {
	changefeedProgress := prevProgress.GetChangefeed()
	if changefeedProgress == nil {
		return
	}
	changefeedCheckpoint := changefeedProgress.Checkpoint
	if changefeedCheckpoint == nil {
		return
	}
	prevSpans := changefeedCheckpoint.Spans

	var spanGroup roachpb.SpanGroup
	spanGroup.Add(prevSpans...)
	spanGroup.Sub(spansToRemove...)
	changefeedProgress.Checkpoint.Spans = spanGroup.Slice()
}

func fetchSpansForDescs(p sql.PlanHookState, droppedIDs []descpb.ID) (primarySpans []roachpb.Span) {
	seen := make(map[descpb.ID]struct{})
	codec := p.ExtendedEvalContext().Codec
	for _, id := range droppedIDs {
		if _, isDup := seen[id]; isDup {
			continue
		}
		seen[id] = struct{}{}
		tablePrefix := codec.TablePrefix(uint32(id))
		primarySpan := roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		}
		primarySpans = append(primarySpans, primarySpan)
	}
	return primarySpans
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
