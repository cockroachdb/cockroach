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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
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

// FastPathResults implements the planNodeFastPath inteface.
func (n *controlJobsNode) FastPathResults() (int, bool) {
	return n.numRows, true
}

func (n *controlJobsNode) startExec(params runParams) error {
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

		job, err := reg.LoadJobWithTxn(params.ctx, jobspb.JobID(jobID), params.p.InternalSQLTxn())
		if err != nil {
			return err
		}

		payload := job.Payload()
		if err := jobsauth.Authorize(params.ctx, params.p,
			job.ID(), &payload, jobsauth.ControlAccess); err != nil {
			return err
		}
		ctrl := job.WithTxn(params.p.InternalSQLTxn())
		switch n.desiredStatus {
		case jobs.StatusPaused:
			err = ctrl.PauseRequested(params.ctx, n.reason)
		case jobs.StatusRunning:
			err = ctrl.Unpaused(params.ctx)
		case jobs.StatusCanceled:
			err = ctrl.CancelRequested(params.ctx)
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
