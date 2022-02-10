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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	sql.AddPlanHook("alter changefeed", alterChangefeedPlanHook)
}

type alterChangefeedOpts struct {
	AddTargets  []tree.TargetList
	DropTargets []tree.TargetList
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

		details, ok := job.Details().(jobspb.ChangefeedDetails)
		if !ok {
			return errors.Errorf(`job %d is not changefeed job`, jobID)
		}

		if job.Status() != jobs.StatusPaused {
			return errors.Errorf(`job %d is not paused`, jobID)
		}

		var opts alterChangefeedOpts
		for _, cmd := range alterChangefeedStmt.Cmds {
			switch v := cmd.(type) {
			case *tree.AlterChangefeedAddTarget:
				opts.AddTargets = append(opts.AddTargets, v.Targets)
			case *tree.AlterChangefeedDropTarget:
				opts.DropTargets = append(opts.DropTargets, v.Targets)
			}
		}

		var initialHighWater hlc.Timestamp
		statementTime := hlc.Timestamp{
			WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
		}

		if opts.AddTargets != nil {
			var targetDescs []catalog.Descriptor

			for _, targetList := range opts.AddTargets {
				descs, err := getTableDescriptors(ctx, p, &targetList, statementTime, initialHighWater)
				if err != nil {
					return err
				}
				targetDescs = append(targetDescs, descs...)
			}

			newTargets, err := getTargets(ctx, p, targetDescs, details.Opts)
			if err != nil {
				return err
			}
			// add old targets
			for id, target := range details.Targets {
				newTargets[id] = target
			}
			details.Targets = newTargets
		}

		if opts.DropTargets != nil {
			var targetDescs []catalog.Descriptor

			for _, targetList := range opts.DropTargets {
				descs, err := getTableDescriptors(ctx, p, &targetList, statementTime, initialHighWater)
				if err != nil {
					return err
				}
				targetDescs = append(targetDescs, descs...)
			}

			for _, desc := range targetDescs {
				if table, isTable := desc.(catalog.TableDescriptor); isTable {
					if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
						return err
					}
					delete(details.Targets, table.GetID())
				}
			}
		}

		if len(details.Targets) == 0 {
			return errors.Errorf("cannot drop all targets for changefeed job %d", jobID)
		}

		if err := validateSink(ctx, p, jobID, details, details.Opts); err != nil {
			return err
		}

		oldStmt, err := parser.ParseOne(job.Payload().Description)
		if err != nil {
			return err
		}
		oldChangefeedStmt, ok := oldStmt.AST.(*tree.CreateChangefeed)
		if !ok {
			return errors.Errorf(`could not parse create changefeed statement for job %d`, jobID)
		}

		var targets tree.TargetList
		for _, target := range details.Targets {
			targetName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, tree.Name(target.StatementTimeName))
			targets.Tables = append(targets.Tables, &targetName)
		}

		oldChangefeedStmt.Targets = targets
		jobDescription := tree.AsString(oldChangefeedStmt)

		newPayload := job.Payload()
		newPayload.Description = jobDescription
		newPayload.Details = jobspb.WrapPayloadDetails(details)

		finalDescs, err := getTableDescriptors(ctx, p, &targets, statementTime, initialHighWater)
		if err != nil {
			return err
		}

		newPayload.DescriptorIDs = func() (sqlDescIDs []descpb.ID) {
			for _, desc := range finalDescs {
				sqlDescIDs = append(sqlDescIDs, desc.GetID())
			}
			return sqlDescIDs
		}()

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
			tree.NewDString(jobDescription),
		}:
			return nil
		}
	}

	return fn, header, nil, false, nil
}
