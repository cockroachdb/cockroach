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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	sql.AddPlanHook(alterChangefeedPlanHook)
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

	jobID := jobspb.JobID(alterChangefeedStmt.JobID)

	job, err := p.ExecCfg().JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		err = errors.Wrapf(err, `could not load job with job id %d`, jobID)
		return nil, nil, nil, false, err
	}

	details, ok := job.Details().(jobspb.ChangefeedDetails)
	if !ok {
		return nil, nil, nil, false, errors.Errorf(`job %d is not changefeed job`, jobID)
	}

	if job.Status() != jobs.StatusPaused {
		return nil, nil, nil, false, errors.Errorf(`job %d is not paused`, jobID)
	}

	avoidBuffering := false
	header := colinfo.ResultColumns{
		{Name: "job_id", Typ: types.Int},
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		if err := validateSettings(ctx, p); err != nil {
			return err
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

		err = saveDetails(ctx, p, jobID, details)
		return err
	}

	return fn, header, nil, avoidBuffering, nil
}

func saveDetails(
	ctx context.Context, p sql.PlanHookState, jobID jobspb.JobID, details jobspb.ChangefeedDetails,
) error {
	err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := p.ExecCfg().JobRegistry.UpdateJobWithTxn(ctx, jobID, txn, true, func(
			txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			md.Payload.Details = jobspb.WrapPayloadDetails(details)
			ju.UpdatePayload(md.Payload)
			return nil
		})
		return err
	})

	return err
}
