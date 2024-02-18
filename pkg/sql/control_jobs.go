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

		if err := reg.UpdateJobWithTxn(params.ctx, jobspb.JobID(jobID), params.p.InternalSQLTxn(),
			func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				if err := jobsauth.Authorize(params.ctx, params.p,
					md.ID, md.Payload, jobsauth.ControlAccess); err != nil {
					return err
				}
				switch n.desiredStatus {
				case jobs.StatusPaused:
					return ju.PauseRequested(params.ctx, txn, md, n.reason)
				case jobs.StatusRunning:
					return ju.Unpaused(params.ctx, md)
				case jobs.StatusCanceled:
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
