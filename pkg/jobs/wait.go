// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// NotifyToAdoptJobs notifies the job adoption loop to start claimed jobs.
func (r *Registry) NotifyToAdoptJobs() {
	select {
	case r.adoptionCh <- resumeClaimedJobs:
	default:
	}
}

// WaitForJobs waits for a given list of jobs to reach some sort
// of terminal state.
func (r *Registry) WaitForJobs(
	ctx context.Context, ex sqlutil.InternalExecutor, jobs []jobspb.JobID,
) error {
	if len(jobs) == 0 {
		return nil
	}
	buf := bytes.Buffer{}
	for i, id := range jobs {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(" %d", id))
	}
	// Manually retry instead of using SHOW JOBS WHEN COMPLETE so we have greater
	// control over retries. Also, avoiding SHOW JOBS prevents us from having to
	// populate the crdb_internal.jobs vtable.
	query := fmt.Sprintf(
		`SELECT count(*) FROM system.jobs WHERE id IN (%s)
       AND status NOT IN ( `+
			`'`+string(StatusSucceeded)+`', `+
			`'`+string(StatusFailed)+`',`+
			`'`+string(StatusCanceled)+`',`+
			`'`+string(StatusRevertFailed)+`',`+
			`'`+string(StatusPaused)+`'`+
			` )`,
		buf.String())
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     1.5,
	}); r.Next(); {
		// We poll the number of queued jobs that aren't finished. As with SHOW JOBS
		// WHEN COMPLETE, if one of the jobs is missing from the jobs table for
		// whatever reason, we'll fail later when we try to load the job.
		row, err := ex.QueryRowEx(
			ctx,
			"poll-show-jobs",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			query,
		)
		if err != nil {
			return errors.Wrap(err, "polling for queued jobs to complete")
		}
		if row == nil {
			return errors.New("polling for queued jobs failed")
		}
		count := int64(tree.MustBeDInt(row[0]))
		if log.V(3) {
			log.Infof(ctx, "waiting for %d queued jobs to complete", count)
		}
		if count == 0 {
			break
		}
	}
	for i, id := range jobs {
		j, err := r.LoadJob(ctx, id)
		if err != nil {
			return errors.WithHint(
				errors.Wrapf(err, "job %d could not be loaded", jobs[i]),
				"The job may not have succeeded.")
		}
		if j.Payload().FinalResumeError != nil {
			decodedErr := errors.DecodeError(ctx, *j.Payload().FinalResumeError)
			return decodedErr
		}
		st, err := j.CurrentStatus(ctx, nil)
		if err != nil {
			return err
		}
		if st == StatusPaused {
			if reason := j.Payload().PauseReason; reason != "" {
				return errors.Newf("job %d was paused before it completed with reason: %s", jobs[i], reason)
			}
			return errors.Newf("job %d was paused before it completed", jobs[i])
		}
		if j.Payload().Error != "" {
			return errors.Newf("job %d failed with error: %s", jobs[i], j.Payload().Error)
		}
	}
	return nil
}

// Run starts previously unstarted jobs from a list of scheduled
// jobs. Canceling ctx interrupts the waiting but doesn't cancel the jobs.
func (r *Registry) Run(
	ctx context.Context, ex sqlutil.InternalExecutor, jobs []jobspb.JobID,
) error {
	if len(jobs) == 0 {
		return nil
	}
	log.Infof(ctx, "scheduled jobs %+v", jobs)
	r.NotifyToAdoptJobs()
	err := r.WaitForJobs(ctx, ex, jobs)
	if err != nil {
		return err
	}
	return nil
}
