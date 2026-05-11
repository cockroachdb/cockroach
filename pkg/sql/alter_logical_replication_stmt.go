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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
	_ context.Context, stmt tree.Statement, _ PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	_, ok := stmt.(*tree.AlterLogicalReplicationStream)
	if !ok {
		return false, nil, nil
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

// skipLogicalReplicationConflict advances the ReplicatedTime of a paused
// transactional LDR job by one logical tick, causing the next resume to start
// strictly after the conflicting transaction.
func skipLogicalReplicationConflict(
	ctx context.Context, p PlanHookState, jobID jobspb.JobID,
) error {
	registry := p.ExecCfg().JobRegistry
	globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, p)
	if err != nil {
		return err
	}

	//lint:ignore SA1019 TODO: migrate to job_info_storage.go API
	return registry.DeprecatedUpdateJobWithTxn(ctx, jobID, p.InternalSQLTxn(), func(
		txn isql.Txn, md jobs.DeprecatedJobMetadata, ju *jobs.DeprecatedJobUpdater,
	) error {
		if err := jobsauth.Authorize(
			ctx, p, md.ID, md.Payload.UsernameProto.Decode(), jobsauth.ControlAccess, globalPrivileges,
		); err != nil {
			return err
		}

		if md.State != jobs.StatePaused {
			return pgerror.New(pgcode.ObjectNotInPrerequisiteState, "job is not paused")
		}

		payloadDetails, ok := md.Payload.Details.(*jobspb.Payload_LogicalReplicationDetails)
		if !ok {
			return pgerror.New(pgcode.WrongObjectType,
				"cannot run SKIP on a non-logical-replication job")
		}
		if payloadDetails.LogicalReplicationDetails.Mode != jobspb.LogicalReplicationDetails_Transactional {
			return pgerror.New(pgcode.FeatureNotSupported,
				"SKIP is only supported for transactional mode")
		}

		ldrProg, ok := md.Progress.Details.(*jobspb.Progress_LogicalReplication)
		if !ok {
			return errors.AssertionFailedf(
				"expected job to have LogicalReplication progress, but got %T", md.Progress.Details)
		}

		prog := ldrProg.LogicalReplication
		if prog.ReplicatedTime.IsEmpty() {
			return pgerror.New(pgcode.ObjectNotInPrerequisiteState,
				"cannot SKIP a job without a replicated time")
		}
		prog.ReplicatedTime = prog.ReplicatedTime.Next()
		ju.UpdateProgress(md.Progress)
		return nil
	})
}
