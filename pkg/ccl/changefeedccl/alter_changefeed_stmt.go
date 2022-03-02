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
	"github.com/cockroachdb/cockroach/pkg/sql"
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

		// this CREATE CHANGEFEED node will be used to update the existing changefeed
		newChangefeedStmt := &tree.CreateChangefeed{
			SinkURI: tree.NewDString(prevDetails.SinkURI),
		}

		optionsMap := make(map[string]tree.KVOption, len(prevDetails.Opts))

		// pull the options that are set for the existing changefeed
		for key, value := range prevDetails.Opts {
			// There are some options (e.g. topics) that we set during the creation of
			// a changefeed, but we do not allow these options to be set by the user.
			// Hence, we can not include these options in our new CREATE CHANGEFEED
			// statement.
			if _, ok := changefeedbase.ChangefeedOptionExpectValues[key]; !ok {
				continue
			}
			existingOpt := tree.KVOption{Key: tree.Name(key)}
			if len(value) > 0 {
				existingOpt.Value = tree.NewDString(value)
			}
			optionsMap[key] = existingOpt
		}

		statementTime := hlc.Timestamp{
			WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
		}

		allDescs, err := backupresolver.LoadAllDescs(ctx, p.ExecCfg(), statementTime)
		if err != nil {
			return err
		}
		descResolver, err := backupresolver.NewDescriptorResolver(allDescs)
		if err != nil {
			return err
		}

		newDescs := make(map[descpb.ID]*tree.UnresolvedName)

		for _, target := range AllTargets(prevDetails) {
			desc := descResolver.DescByID[target.TableID]
			newDescs[target.TableID] = tree.NewUnresolvedName(desc.GetName())
		}

		for _, cmd := range alterChangefeedStmt.Cmds {
			switch v := cmd.(type) {
			case *tree.AlterChangefeedAddTarget:
				for _, targetPattern := range v.Targets.Tables {
					targetName, err := getTargetName(targetPattern)
					if err != nil {
						return err
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
						return err
					}
					if !found {
						return pgerror.Newf(pgcode.InvalidParameterValue, `target %q does not exist`, tree.ErrString(targetPattern))
					}
					newDescs[desc.GetID()] = tree.NewUnresolvedName(desc.GetName())
				}
			case *tree.AlterChangefeedDropTarget:
				for _, targetPattern := range v.Targets.Tables {
					targetName, err := getTargetName(targetPattern)
					if err != nil {
						return err
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
						return err
					}
					if !found {
						return pgerror.Newf(pgcode.InvalidParameterValue, `target %q does not exist`, tree.ErrString(targetPattern))
					}
					delete(newDescs, desc.GetID())
				}
			case *tree.AlterChangefeedSetOptions:
				optsFn, err := p.TypeAsStringOpts(ctx, v.Options, changefeedbase.AlterChangefeedOptionExpectValues)
				if err != nil {
					return err
				}

				opts, err := optsFn()
				if err != nil {
					return err
				}

				for key, value := range opts {
					if _, ok := changefeedbase.AlterChangefeedUnsupportedOptions[key]; ok {
						return pgerror.Newf(pgcode.InvalidParameterValue, `cannot alter option %q`, key)
					}
					if key == changefeedbase.OptSink {
						newSinkURI, err := url.Parse(value)
						if err != nil {
							return err
						}

						prevSinkURI, err := url.Parse(prevDetails.SinkURI)
						if err != nil {
							return err
						}

						if newSinkURI.Scheme != prevSinkURI.Scheme {
							return pgerror.Newf(
								pgcode.InvalidParameterValue,
								`New sink type %q does not match original sink type %q. Altering the sink type of a changefeed is disallowed, consider creating a new changefeed instead.`,
								newSinkURI.Scheme,
								prevSinkURI.Scheme,
							)
						}

						newChangefeedStmt.SinkURI = tree.NewDString(value)
					} else {
						opt := tree.KVOption{Key: tree.Name(key)}
						if len(value) > 0 {
							opt.Value = tree.NewDString(value)
						}
						optionsMap[key] = opt
					}
				}
			case *tree.AlterChangefeedUnsetOptions:
				optKeys := v.Options.ToStrings()
				for _, key := range optKeys {
					if key == changefeedbase.OptSink {
						return pgerror.Newf(pgcode.InvalidParameterValue, `cannot unset option %q`, key)
					}
					if _, ok := changefeedbase.ChangefeedOptionExpectValues[key]; !ok {
						return pgerror.Newf(pgcode.InvalidParameterValue, `invalid option %q`, key)
					}
					if _, ok := changefeedbase.AlterChangefeedUnsupportedOptions[key]; ok {
						return pgerror.Newf(pgcode.InvalidParameterValue, `cannot alter option %q`, key)
					}
					delete(optionsMap, key)
				}
			}
		}

		if len(newDescs) == 0 {
			return pgerror.Newf(pgcode.InvalidParameterValue, "cannot drop all targets for changefeed job %d", jobID)
		}

		for _, targetName := range newDescs {
			newChangefeedStmt.Targets.Tables = append(newChangefeedStmt.Targets.Tables, targetName)
		}

		for _, val := range optionsMap {
			newChangefeedStmt.Options = append(newChangefeedStmt.Options, val)
		}

		sinkURIFn, err := p.TypeAsString(ctx, newChangefeedStmt.SinkURI, `ALTER CHANGEFEED`)
		if err != nil {
			return err
		}

		optsFn, err := p.TypeAsStringOpts(ctx, newChangefeedStmt.Options, changefeedbase.ChangefeedOptionExpectValues)
		if err != nil {
			return err
		}

		sinkURI, err := sinkURIFn()
		if err != nil {
			return err
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		jobRecord, err := createChangefeedJobRecord(
			ctx,
			p,
			newChangefeedStmt,
			sinkURI,
			opts,
			jobID,
			``,
		)
		if err != nil {
			return errors.Wrap(err, `failed to alter changefeed`)
		}

		newDetails := jobRecord.Details.(jobspb.ChangefeedDetails)

		// We need to persist the statement time that was generated during the
		// creation of the changefeed
		newDetails.StatementTime = prevDetails.StatementTime

		newPayload := job.Payload()
		newPayload.Details = jobspb.WrapPayloadDetails(newDetails)
		newPayload.Description = jobRecord.Description
		newPayload.DescriptorIDs = jobRecord.DescriptorIDs

		err = p.ExecCfg().JobRegistry.UpdateJobWithTxn(ctx, jobID, p.ExtendedEvalContext().Txn, lockForUpdate, func(
			txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			ju.UpdatePayload(&newPayload)
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

func getTargetName(targetPattern tree.TablePattern) (*tree.TableName, error) {
	pattern, err := targetPattern.NormalizeTablePattern()
	if err != nil {
		return nil, err
	}
	targetName, ok := pattern.(*tree.TableName)
	if !ok {
		return nil, errors.Errorf(`CHANGEFEED cannot target %q`, tree.AsString(targetPattern))
	}

	return targetName, nil
}
