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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (p *planner) planAlterLogicalReplicationStream(
	ctx context.Context, n *tree.AlterLogicalReplicationStream,
) (planNode, error) {
	jobIDInt, err := p.ExprEvaluator("ALTER LOGICAL REPLICATION STREAM").Int(ctx, n.JobID)
	if err != nil {
		return nil, err
	}
	jobID := jobspb.JobID(jobIDInt)

	for _, cmd := range n.Cmds {
		switch c := cmd.(type) {
		case *tree.SkipConflictCmd:
			if err := p.skipLogicalReplicationConflict(ctx, jobID); err != nil {
				return nil, err
			}
		default:
			return nil, errors.AssertionFailedf("unknown ALTER LOGICAL REPLICATION STREAM command: %T", c)
		}
	}

	return newZeroNode(nil /* columns */), nil
}

// skipLogicalReplicationConflict advances the ReplicatedTime of a paused
// transactional LDR job by one logical tick, causing the next resume to start
// strictly after the conflicting transaction.
func (p *planner) skipLogicalReplicationConflict(ctx context.Context, jobID jobspb.JobID) error {
	registry := p.ExecCfg().JobRegistry

	job, err := registry.LoadJob(ctx, jobID)
	if err != nil {
		return err
	}
	globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, p)
	if err != nil {
		return err
	}
	if err := jobsauth.Authorize(
		ctx, p, jobID, job.Payload().UsernameProto.Decode(), jobsauth.ControlAccess, globalPrivileges,
	); err != nil {
		return err
	}

	//lint:ignore SA1019 TODO: migrate to job_info_storage.go API
	return p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return registry.DeprecatedUpdateJobWithTxn(ctx, jobID, txn, func(
			txn isql.Txn, md jobs.DeprecatedJobMetadata, ju *jobs.DeprecatedJobUpdater,
		) error {
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
	})
}
