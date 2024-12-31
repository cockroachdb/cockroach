// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type alterJobOwnerNode struct {
	zeroInputPlanNode
	jobID tree.TypedExpr
	owner username.SQLUsername
}

// alterJobOwner updates the owner of a job. It requires the user calling it be
// the current owner, a member of the role that is the current owner, or an
// admin and a user can only transfer ownership to themself or to a role of
// which they are a member, unless they are an admin (ownership can never be
// transferred to or from "node").
func (p *planner) alterJobOwner(ctx context.Context, n *tree.AlterJobOwner) (planNode, error) {
	owner, err := decodeusername.FromRoleSpec(
		p.SessionData(), username.PurposeValidation, n.Owner,
	)
	if err != nil {
		return nil, err
	}
	var dummyHelper tree.IndexedVarHelper
	typedJobID, err := p.analyzeExpr(ctx, n.Job, dummyHelper, types.Int, true, "ALTER JOB OWNER")
	if err != nil {
		return nil, err
	}
	return &alterJobOwnerNode{
		jobID: typedJobID,
		owner: owner,
	}, nil
}

func (n *alterJobOwnerNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	newOwner := n.owner

	// The top-level owner column this updates was added in 25.1.
	v, err := p.InternalSQLTxn().GetSystemSchemaVersion(ctx)
	if err != nil {
		return err
	}
	if !v.AtLeast(clusterversion.V25_1.Version()) {
		return pgerror.Newf(pgcode.FeatureNotSupported, "ALTER JOB OWNER requires version %s", clusterversion.V25_1)
	}

	exprEval := p.ExprEvaluator("ALTER JOB OWNER")
	jobIDInt, err := exprEval.Int(ctx, n.jobID)
	if err != nil {
		return err
	}
	jobID := jobspb.JobID(jobIDInt)

	cur, err := params.ExecCfg().InternalDB.Executor().QueryRowEx(
		ctx, "get-job-owner", p.txn, sessiondata.NodeUserSessionDataOverride,
		`SELECT owner FROM system.jobs WHERE id = $1`, jobID,
	)
	if err != nil {
		return err
	}
	if cur == nil {
		return jobs.NewJobNotFoundError(jobID)
	}

	currentOwner := username.MakeSQLUsernameFromPreNormalizedString(string(tree.MustBeDString(cur[0])))

	if err := n.checkOwnership(ctx, params.p, currentOwner, false); err != nil {
		return err
	}

	if err := n.checkOwnership(ctx, params.p, newOwner, true); err != nil {
		return err
	}

	if newOwner == currentOwner {
		return nil
	}

	if _, err = params.ExecCfg().InternalDB.Executor().ExecEx(
		ctx, "set-job-owner", p.txn, sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.jobs SET owner = $2 WHERE id = $1`, jobID, newOwner.Normalized(),
	); err != nil {
		return err
	}

	// Update the legacy payload's copy as well since there are still some paths
	// which read it.
	// TODO(dt): remove this when we drop the field from the legacy payload.
	legacyJob, err := params.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.InternalSQLTxn())
	if err != nil {
		return err
	}
	return legacyJob.WithTxn(p.InternalSQLTxn()).Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		md.Payload.UsernameProto = newOwner.EncodeProto()
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// checkOwnership checks that the current user is allowed to set the owner of a
// job that is owned or will be owned by the passed owner. This should be called
// twice: once for the current owner and once for the new owner, with the param
// forNewOwner indicating which is being done to provide clearer errors.
func (n *alterJobOwnerNode) checkOwnership(
	ctx context.Context, p *planner, owner username.SQLUsername, forNewOwner bool,
) error {
	// If the user is owner, this is trivially true.
	if owner == p.User() {
		return nil
	}

	// Nobody is allowed to transfer jobs to "node".
	if owner == username.NodeUserName() {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "cannot transfer ownership to or from internal node user")
	}

	// If the user is an admin, ownership is irrelevant.
	isAdmin, err := p.UserHasAdminRole(ctx, p.User())
	if err != nil {
		return err
	}
	if isAdmin {
		return nil
	}

	// If the user is a member of a role that is the owner, they are an owner.
	members, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return err
	}
	if _, ok := members[owner]; ok {
		return nil
	}

	if forNewOwner {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "%s cannot transfer ownership to %s", p.User(), owner)
	}
	// The user doesn't own the job and isn't an admin; they cannot set the owner.
	return pgerror.Newf(pgcode.InsufficientPrivilege, "%s does not own job", p.User())
}

//func (n *alterJobOwnerNode) ReadingOwnWrites() {}

func (n *alterJobOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterJobOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterJobOwnerNode) Close(context.Context)        {}
