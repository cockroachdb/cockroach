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
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	sql.AddPlanHook("alter changefeed", alterChangefeedPlanHook)
}

// alterChangefeedPlanHook implements sql.PlanHookFn.
func alterChangefeedPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	alterChangefeedStmt, ok := stmt.(*tree.AlterChangefeed)
	if !ok {
		return nil, nil, nil, false, nil
	}

	header := colinfo.ResultColumns{
		{Name: "job_id", Typ: types.Int},
		{Name: "job_description", Typ: types.String},
	}
	lockForUpdate := false

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		if err := validateSettings(ctx, p); err != nil {
			return err
		}

		typedExpr, err := alterChangefeedStmt.Jobs.TypeCheck(ctx, p.SemaCtx(), types.Int)
		if err != nil {
			return err
		}
		jobID := jobspb.JobID(tree.MustBeDInt(typedExpr))

		job, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.ExtendedEvalContext().Txn)
		if err != nil {
			err = errors.Wrapf(err, `could not load job with job id %d`, jobID)
			return err
		}

		prevDetails, ok := job.Details().(jobspb.ChangefeedDetails)
		if !ok {
			return errors.Errorf(`job %d is not changefeed job`, jobID)
		}

		if job.Status() != jobs.StatusPaused {
			return errors.Errorf(`job %d is not paused`, jobID)
		}

		newChangefeedStmt := &tree.CreateChangefeed{}

		newOptions, newSinkURI, err := generateNewOpts(ctx, p, alterChangefeedStmt.Cmds, prevDetails)
		if err != nil {
			return err
		}

		newTargets, newProgress, newStatementTime, err := generateNewTargets(ctx,
			p,
			alterChangefeedStmt.Cmds,
			newOptions,
			prevDetails,
			job.Progress(),
		)
		if err != nil {
			return err
		}
		newChangefeedStmt.Targets = newTargets

		for key, value := range newOptions {
			opt := tree.KVOption{Key: tree.Name(key)}
			if len(value) > 0 {
				opt.Value = tree.NewDString(value)
			}
			newChangefeedStmt.Options = append(newChangefeedStmt.Options, opt)
		}
		newChangefeedStmt.SinkURI = tree.NewDString(newSinkURI)

		jobRecord, err := createChangefeedJobRecord(
			ctx,
			p,
			newChangefeedStmt,
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

		err = p.ExecCfg().JobRegistry.UpdateJobWithTxn(ctx, jobID, p.ExtendedEvalContext().Txn, lockForUpdate, func(
			txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			ju.UpdatePayload(&newPayload)
			if newProgress != nil {
				ju.UpdateProgress(newProgress)
			}
			return nil
		})

		if err != nil {
			return err
		}

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

	return fn, header, nil, false, nil
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
	p sql.PlanHookState,
	alterCmds tree.AlterChangefeedCmds,
	details jobspb.ChangefeedDetails,
) (map[string]string, string, error) {
	sinkURI := details.SinkURI
	newOptions := make(map[string]string, len(details.Opts))

	// pull the options that are set for the existing changefeed.
	for key, value := range details.Opts {
		// There are some options (e.g. topics) that we set during the creation of
		// a changefeed, but we do not allow these options to be set by the user.
		// Hence, we can not include these options in our new CREATE CHANGEFEED
		// statement.
		if _, ok := changefeedbase.ChangefeedOptionExpectValues[key]; !ok {
			continue
		}
		newOptions[key] = value
	}

	for _, cmd := range alterCmds {
		switch v := cmd.(type) {
		case *tree.AlterChangefeedSetOptions:
			optsFn, err := p.TypeAsStringOpts(ctx, v.Options, changefeedbase.AlterChangefeedOptionExpectValues)
			if err != nil {
				return nil, ``, err
			}

			opts, err := optsFn()
			if err != nil {
				return nil, ``, err
			}

			for key, value := range opts {
				if _, ok := changefeedbase.AlterChangefeedUnsupportedOptions[key]; ok {
					return nil, ``, pgerror.Newf(pgcode.InvalidParameterValue, `cannot alter option %q`, key)
				}
				if key == changefeedbase.OptSink {
					newSinkURI, err := url.Parse(value)
					if err != nil {
						return nil, ``, err
					}

					prevSinkURI, err := url.Parse(details.SinkURI)
					if err != nil {
						return nil, ``, err
					}

					if newSinkURI.Scheme != prevSinkURI.Scheme {
						return nil, ``, pgerror.Newf(
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
		case *tree.AlterChangefeedUnsetOptions:
			optKeys := v.Options.ToStrings()
			for _, key := range optKeys {
				if key == changefeedbase.OptSink {
					return nil, ``, pgerror.Newf(pgcode.InvalidParameterValue, `cannot unset option %q`, key)
				}
				if _, ok := changefeedbase.ChangefeedOptionExpectValues[key]; !ok {
					return nil, ``, pgerror.Newf(pgcode.InvalidParameterValue, `invalid option %q`, key)
				}
				if _, ok := changefeedbase.AlterChangefeedUnsupportedOptions[key]; ok {
					return nil, ``, pgerror.Newf(pgcode.InvalidParameterValue, `cannot alter option %q`, key)
				}
				delete(newOptions, key)
			}
		}
	}

	return newOptions, sinkURI, nil
}

func generateNewTargets(
	ctx context.Context,
	p sql.PlanHookState,
	alterCmds tree.AlterChangefeedCmds,
	opts map[string]string,
	prevDetails jobspb.ChangefeedDetails,
	prevProgress jobspb.Progress,
) (tree.ChangefeedTargets, *jobspb.Progress, hlc.Timestamp, error) {

	type targetKey struct {
		TableID    descpb.ID
		FamilyName tree.Name
	}
	newTargets := make(map[targetKey]tree.ChangefeedTarget)
	droppedTargets := make(map[targetKey]tree.ChangefeedTarget)
	newTableDescs := make(map[descpb.ID]catalog.Descriptor)

	// When we add new targets with or without initial scans, indicating
	// initial_scan or no_initial_scan in the job description would lose its
	// meaning. Hence, we will omit these details from the changefeed
	// description. However, to ensure that we do perform the initial scan on
	// newly added targets, we will introduce the initial_scan opt after the
	// job record is created.
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
		return nil, nil, hlc.Timestamp{}, err
	}
	descResolver, err := backupresolver.NewDescriptorResolver(allDescs)
	if err != nil {
		return nil, nil, hlc.Timestamp{}, err
	}

	for _, target := range AllTargets(prevDetails) {
		k := targetKey{TableID: target.TableID, FamilyName: tree.Name(target.FamilyName)}
		newTargets[k] = tree.ChangefeedTarget{
			TableName:  tree.NewUnresolvedName(target.StatementTimeName),
			FamilyName: tree.Name(target.FamilyName),
		}
		newTableDescs[target.TableID] = descResolver.DescByID[target.TableID]
	}

	for _, cmd := range alterCmds {
		switch v := cmd.(type) {
		case *tree.AlterChangefeedAddTarget:
			targetOptsFn, err := p.TypeAsStringOpts(ctx, v.Options, changefeedbase.AlterChangefeedTargetOptions)
			if err != nil {
				return nil, nil, hlc.Timestamp{}, err
			}
			targetOpts, err := targetOptsFn()
			if err != nil {
				return nil, nil, hlc.Timestamp{}, err
			}

			_, withInitialScan := targetOpts[changefeedbase.OptInitialScan]
			_, noInitialScan := targetOpts[changefeedbase.OptNoInitialScan]
			if withInitialScan && noInitialScan {
				return nil, nil, hlc.Timestamp{}, pgerror.Newf(
					pgcode.InvalidParameterValue,
					`cannot specify both %q and %q`, changefeedbase.OptInitialScan,
					changefeedbase.OptNoInitialScan,
				)
			}

			var existingTargetDescs []catalog.Descriptor
			for _, targetDesc := range newTableDescs {
				existingTargetDescs = append(existingTargetDescs, targetDesc)
			}
			existingTargetSpans, err := fetchSpansForDescs(ctx, p, opts, statementTime, existingTargetDescs)
			if err != nil {
				return nil, nil, hlc.Timestamp{}, err
			}

			var newTargetDescs []catalog.Descriptor
			for _, target := range v.Targets {
				desc, found, err := getTargetDesc(ctx, p, descResolver, target.TableName)
				if err != nil {
					return nil, nil, hlc.Timestamp{}, err
				}
				if !found {
					return nil, nil, hlc.Timestamp{}, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q does not exist`,
						tree.ErrString(&target),
					)
				}
				k := targetKey{TableID: desc.GetID(), FamilyName: target.FamilyName}
				newTargets[k] = target
				newTableDescs[desc.GetID()] = desc
				newTargetDescs = append(newTargetDescs, desc)
			}

			addedTargetSpans, err := fetchSpansForDescs(ctx, p, opts, statementTime, newTargetDescs)
			if err != nil {
				return nil, nil, hlc.Timestamp{}, err
			}

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
				return nil, nil, hlc.Timestamp{}, err
			}
		case *tree.AlterChangefeedDropTarget:
			for _, target := range v.Targets {
				desc, found, err := getTargetDesc(ctx, p, descResolver, target.TableName)
				if err != nil {
					return nil, nil, hlc.Timestamp{}, err
				}
				if !found {
					return nil, nil, hlc.Timestamp{}, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q does not exist`,
						tree.ErrString(&target),
					)
				}
				k := targetKey{TableID: desc.GetID(), FamilyName: target.FamilyName}
				droppedTargets[k] = target
				_, recognized := newTargets[k]
				if !recognized {
					return nil, nil, hlc.Timestamp{}, pgerror.Newf(
						pgcode.InvalidParameterValue,
						`target %q already not watched by changefeed`,
						tree.ErrString(&target),
					)
				}
				delete(newTargets, k)
			}
		}
	}

	// Remove tables from the job progress if and only if the number of
	// targets referencing them has fallen to zero. For example, we might
	// drop one column family from a table and add another at the same time,
	// and since we watch entire table spans the set of spans won't change.
	if len(droppedTargets) > 0 {
		stillThere := make(map[descpb.ID]bool)
		for k := range newTargets {
			stillThere[k.TableID] = true
		}
		for k := range droppedTargets {
			if !stillThere[k.TableID] {
				stillThere[k.TableID] = false
			}
		}
		var droppedTargetDescs []catalog.Descriptor
		for id, there := range stillThere {
			if !there {
				droppedTargetDescs = append(droppedTargetDescs, descResolver.DescByID[id])
			}
		}
		if len(droppedTargetDescs) > 0 {
			droppedTargetSpans, err := fetchSpansForDescs(ctx, p, opts, statementTime, droppedTargetDescs)
			if err != nil {
				return nil, nil, hlc.Timestamp{}, err
			}
			removeSpansFromProgress(newJobProgress, droppedTargetSpans)
		}
	}

	newTargetList := tree.ChangefeedTargets{}

	for _, target := range newTargets {
		newTargetList = append(newTargetList, target)
	}

	if err := validateNewTargets(ctx, p, newTargetList, newJobProgress, newJobStatementTime); err != nil {
		return nil, nil, hlc.Timestamp{}, err
	}

	return newTargetList, &newJobProgress, newJobStatementTime, nil
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
func generateNewProgress(
	prevProgress jobspb.Progress,
	prevStatementTime hlc.Timestamp,
	existingTargetSpans []roachpb.Span,
	newSpans []roachpb.Span,
	withInitialScan bool,
) (jobspb.Progress, hlc.Timestamp, error) {
	prevHighWater := prevProgress.GetHighWater()
	changefeedProgress := prevProgress.GetChangefeed()

	haveHighwater := !(prevHighWater == nil || prevHighWater.IsEmpty())
	haveCheckpoint := changefeedProgress != nil && changefeedProgress.Checkpoint != nil &&
		len(changefeedProgress.Checkpoint.Spans) != 0

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
			tree.TimestampToDecimalDatum(*prevHighWater).Decimal.String(),
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
					Checkpoint: &jobspb.ChangefeedProgress_Checkpoint{
						Spans: existingTargetSpans,
					},
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
				Checkpoint: &jobspb.ChangefeedProgress_Checkpoint{
					Spans: mergedSpanGroup.Slice(),
				},
			},
		},
	}
	return newProgress, prevStatementTime, nil
}

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

func fetchSpansForDescs(
	ctx context.Context,
	p sql.PlanHookState,
	opts map[string]string,
	statementTime hlc.Timestamp,
	descs []catalog.Descriptor,
) ([]roachpb.Span, error) {
	_, tables, err := getTargetsAndTables(ctx, p, descs, tree.ChangefeedTargets{}, opts)
	if err != nil {
		return nil, err
	}

	details := jobspb.ChangefeedDetails{
		Tables: tables,
	}

	spans, err := fetchSpansForTargets(ctx, p.ExecCfg(), AllTargets(details), statementTime)

	return spans, err
}
