// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

type controlJobsNode struct {
	rows          planNode
	desiredStatus jobs.Status
	numRows       int
	reason        string
}

var jobCommandToDesiredStatus = map[tree.JobCommand]jobs.Status{
	tree.CancelJob: jobs.StatusCanceled,
	tree.ResumeJob: jobs.StatusRunning,
	tree.PauseJob:  jobs.StatusPaused,
}

var jobTypeToPrivilegeKind = map[jobspb.Type]privilege.Kind{
	jobspb.TypeChangefeed: privilege.CHANGEFEED,
}

// FastPathResults implements the planNodeFastPath inteface.
func (n *controlJobsNode) FastPathResults() (int, bool) {
	return n.numRows, true
}

func (n *controlJobsNode) startExec(params runParams) error {
	userIsAdmin, err := params.p.HasAdminRole(params.ctx)
	if err != nil {
		return err
	}
	var hasControlJob bool
	var systemPrivileges map[jobspb.Type]struct{}

	// users can pause/resume/cancel jobs owned by non-admin users
	// if they have the CONTROLJOBS role option or the system privilege
	// for the job's type.
	if !userIsAdmin {
		hasControlJob, err := params.p.HasRoleOption(params.ctx, roleoption.CONTROLJOB)
		if err != nil {
			return err
		}

		systemPrivileges = make(map[jobspb.Type]struct{})

		if params.extendedEvalCtx.Settings.Version.IsActive(params.ctx, clusterversion.SystemPrivilegesTable) {
			for jobType, priv := range jobTypeToPrivilegeKind {
				if params.p.CheckPrivilege(params.ctx, syntheticprivilege.GlobalPrivilegeObject, priv) != nil {
					systemPrivileges[jobType] = struct{}{}
				}
			}
		}

		if !hasControlJob && len(systemPrivileges) == 0 {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s does not have %s privilege",
				params.p.User(), roleoption.CONTROLJOB)
		}
	}

	if n.desiredStatus != jobs.StatusPaused && len(n.reason) > 0 {
		return errors.AssertionFailedf("status %v is not %v and thus does not support a reason %v",
			n.desiredStatus, jobs.StatusPaused, n.reason)
	}

	reg := params.p.ExecCfg().JobRegistry
	for {
		ok, err := n.rows.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		jobIDDatum := n.rows.Values()[0]
		if jobIDDatum == tree.DNull {
			continue
		}

		jobID, ok := tree.AsDInt(jobIDDatum)
		if !ok {
			return errors.AssertionFailedf("%q: expected *DInt, found %T", jobIDDatum, jobIDDatum)
		}

		job, err := reg.LoadJobWithTxn(params.ctx, jobspb.JobID(jobID), params.p.Txn())
		if err != nil {
			return err
		}

		// If the user is not an admin, we need more information about the job to check authorization.
		if job != nil && !userIsAdmin {
			payload := job.Payload()
			var ownerHasAdminRole, userHasRelevantSystemPrivilege bool

			if hasControlJob {
				owner := payload.UsernameProto.Decode()

				ownerHasAdminRole, err = params.p.UserHasAdminRole(params.ctx, owner)
				if err != nil {
					return err
				}
			}

			legacyAuthCheckPassed := hasControlJob && !ownerHasAdminRole

			if !legacyAuthCheckPassed {
				_, userHasRelevantSystemPrivilege = systemPrivileges[payload.Type()]
			}

			// At least one of the following needs to be true:
			// 1 - User is admin.
			// 2 - User has CONTROLJOB and the job is not owned by an admin.
			// 3 - User has been granted the jobtype privilege ON SYSTEM.
			if !(legacyAuthCheckPassed || userHasRelevantSystemPrivilege) {
				return pgerror.Newf(pgcode.InsufficientPrivilege,
					"user is not authorized to control this job or job type")
			}
		}

		switch n.desiredStatus {
		case jobs.StatusPaused:
			err = reg.PauseRequested(params.ctx, params.p.txn, jobspb.JobID(jobID), n.reason)
		case jobs.StatusRunning:
			err = reg.Unpause(params.ctx, params.p.txn, jobspb.JobID(jobID))
		case jobs.StatusCanceled:
			err = reg.CancelRequested(params.ctx, params.p.txn, jobspb.JobID(jobID))
		default:
			err = errors.AssertionFailedf("unhandled status %v", n.desiredStatus)
		}
		if err != nil {
			return err
		}
		n.numRows++
	}
	switch n.desiredStatus {
	case jobs.StatusPaused:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("pause"))
	case jobs.StatusRunning:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("resume"))
	case jobs.StatusCanceled:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("cancel"))
	}
	return nil
}

func (*controlJobsNode) Next(runParams) (bool, error) { return false, nil }

func (*controlJobsNode) Values() tree.Datums { return nil }

func (n *controlJobsNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
