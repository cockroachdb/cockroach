// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type controlJobsNode struct {
	singleInputPlanNode
	desiredStatus jobs.State
	numRows       int
	reason        string
}

var jobCommandToDesiredStatus = map[tree.JobCommand]jobs.State{
	tree.CancelJob: jobs.StateCanceled,
	tree.ResumeJob: jobs.StateRunning,
	tree.PauseJob:  jobs.StatePaused,
}

// FastPathResults implements the planNodeFastPath interface.
func (n *controlJobsNode) FastPathResults() (int, bool) {
	return n.numRows, true
}

func (n *controlJobsNode) startExec(params runParams) error {
	if n.desiredStatus != jobs.StatePaused && len(n.reason) > 0 {
		return errors.AssertionFailedf("status %v is not %v and thus does not support a reason %v",
			n.desiredStatus, jobs.StatePaused, n.reason)
	}

	reg := params.p.ExecCfg().JobRegistry
	globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(params.ctx, params.p)
	if err != nil {
		return err
	}
	for {
		ok, err := n.input.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		jobIDDatum := n.input.Values()[0]
		if jobIDDatum == tree.DNull {
			continue
		}

		jobID, ok := tree.AsDInt(jobIDDatum)
		if !ok {
			return errors.AssertionFailedf("%q: expected *DInt, found %T", jobIDDatum, jobIDDatum)
		}

		if err := reg.UpdateJobWithTxn(params.ctx, jobspb.JobID(jobID), params.p.InternalSQLTxn(),
			func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				getLegacyPayload := func(ctx context.Context) (*jobspb.Payload, error) {
					return md.Payload, nil
				}
				if err := jobsauth.Authorize(params.ctx, params.p,
					md.ID, getLegacyPayload, md.Payload.UsernameProto.Decode(), md.Payload.Type(), jobsauth.ControlAccess, globalPrivileges); err != nil {
					return err
				}
				switch n.desiredStatus {
				case jobs.StatePaused:
					return ju.PauseRequested(params.ctx, txn, md, n.reason)
				case jobs.StateRunning:
					return ju.Unpaused(params.ctx, md)
				case jobs.StateCanceled:
					return ju.CancelRequested(params.ctx, md)
				default:
					return errors.AssertionFailedf("unhandled status %v", n.desiredStatus)
				}
			}); err != nil {
			return err
		}

		n.numRows++
	}
	switch n.desiredStatus {
	case jobs.StatePaused:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("pause"))
	case jobs.StateRunning:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("resume"))
	case jobs.StateCanceled:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("cancel"))
	}
	return nil
}

func (*controlJobsNode) Next(runParams) (bool, error) { return false, nil }

func (*controlJobsNode) Values() tree.Datums { return nil }

func (n *controlJobsNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}
