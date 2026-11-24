// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func init() {
	AddPlanHook(
		"alter logical replication stream",
		alterLogicalReplicationStreamPlanHook,
		alterLogicalReplicationStreamTypeCheck,
	)
}

func alterLogicalReplicationStreamTypeCheck(
	ctx context.Context, stmt tree.Statement, p PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	alterStmt, ok := stmt.(*tree.AlterLogicalReplicationStream)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, "ALTER LOGICAL REPLICATION STREAM", p.SemaCtx(),
		exprutil.Ints{alterStmt.JobID},
	); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

// alterLogicalReplicationStreamPlanHook implements PlanHookFn.
func alterLogicalReplicationStreamPlanHook(
	ctx context.Context, stmt tree.Statement, p PlanHookState,
) (PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	alterStmt, ok := stmt.(*tree.AlterLogicalReplicationStream)
	if !ok {
		return nil, nil, false, nil
	}

	fn := func(ctx context.Context, _ chan<- tree.Datums) error {
		jobIDInt, err := p.ExprEvaluator("ALTER LOGICAL REPLICATION STREAM").Int(ctx, alterStmt.JobID)
		if err != nil {
			return err
		}
		jobID := jobspb.JobID(jobIDInt)

		for _, cmd := range alterStmt.Cmds {
			switch cmd.(type) {
			case *tree.SkipConflictCmd:
				if err := skipLogicalReplicationConflict(ctx, p, jobID); err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf(
					"unknown ALTER LOGICAL REPLICATION STREAM command: %T", cmd)
			}
		}
		return nil
	}

	return fn, nil, false, nil
}

// skipLogicalReplicationConflict advances the high-water mark of a paused
// transactional LDR job by one logical tick, causing the next resume to start
// strictly after the conflicting transaction.
func skipLogicalReplicationConflict(
	ctx context.Context, p PlanHookState, jobID jobspb.JobID,
) error {
	txn := p.InternalSQLTxn()
	registry := p.ExecCfg().JobRegistry
	j, err := registry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return err
	}

	globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, p)
	if err != nil {
		return err
	}
	if err := jobsauth.Authorize(ctx, p,
		jobID, j.Payload().UsernameProto.Decode(), jobsauth.ControlAccess, globalPrivileges,
	); err != nil {
		return err
	}

	if j.State() != jobs.StatePaused {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"job %d is not paused (current state: %s)", jobID, j.State())
	}

	details, ok := j.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails)
	if !ok {
		return pgerror.Newf(pgcode.WrongObjectType,
			"job %d is not a logical replication job", jobID)
	}
	if details.LogicalReplicationDetails.Mode != jobspb.LogicalReplicationDetails_Transactional {
		return pgerror.New(pgcode.FeatureNotSupported,
			"SKIP is only supported for transactional mode")
	}

	_, resolved, _, err := j.ProgressStorage().Get(ctx, txn)
	if err != nil {
		return err
	}
	if resolved.IsEmpty() {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"job %d has no replicated time to advance", jobID)
	}
	return j.ProgressStorage().SetResolved(ctx, txn, resolved.Next())
}
