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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// NotifyToAdoptJobs notifies the job adoption loop to start claimed jobs.
func (r *Registry) NotifyToAdoptJobs() {
	select {
	case r.adoptionCh <- resumeClaimedJobs:
	default:
	}
}

// NotifyToResume is used to notify the registry that it should attempt
// to resume the specified jobs. The assumption is that these jobs were
// created by this registry and thus are pre-claimed by it. This bypasses
// the loop to discover jobs already claimed by this registry. If the jobs
// turn out to not be claimed by this registry, it's not a problem.
func (r *Registry) NotifyToResume(ctx context.Context, jobs ...jobspb.JobID) {
	m := newJobIDSet(jobs...)
	_ = r.stopper.RunAsyncTask(ctx, "resume-jobs", func(ctx context.Context) {
		r.withSession(ctx, func(ctx context.Context, s sqlliveness.Session) {
			r.filterAlreadyRunningAndCancelFromPreviousSessions(ctx, s, m)
			r.resumeClaimedJobs(ctx, s, m)
		})
	})
}

// WaitForJobs waits for a given list of jobs to reach some sort
// of terminal state.
func (r *Registry) WaitForJobs(
	ctx context.Context, ex sqlutil.InternalExecutor, jobs []jobspb.JobID,
) error {
	log.Infof(ctx, "waiting for %d %v queued jobs to complete", len(jobs), jobs)
	jobFinishedLocally, cleanup := r.installWaitingSet(jobs...)
	defer cleanup()
	return r.waitForJobs(ctx, ex, jobs, jobFinishedLocally)
}

func (r *Registry) waitForJobs(
	ctx context.Context,
	ex sqlutil.InternalExecutor,
	jobs []jobspb.JobID,
	jobFinishedLocally <-chan struct{},
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
	start := timeutil.Now()
	ret := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		Multiplier:     1.5,
		Closer:         jobFinishedLocally,
	})
	ret.Next() // wait at least one InitialBackoff
	for ret.Next() {
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
	defer func() {
		log.Infof(ctx, "waited for %d %v queued jobs to complete %v",
			len(jobs), jobs, timeutil.Since(start))
	}()
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
	done, cleanup := r.installWaitingSet(jobs...)
	defer cleanup()
	r.NotifyToResume(ctx, jobs...)
	return r.waitForJobs(ctx, ex, jobs, done)
}

type jobWaitingSets map[jobspb.JobID]map[*waitingSet]struct{}

// waitingSet is a set of job IDs that a local client is waiting to complete.
type waitingSet struct {
	// jobDoneCh is closed when the set becomes empty.
	jobDoneCh chan struct{}
	// Note that the set itself is only ever mutated under the registry's mu.
	// This choice is made to remove any concerns regarding lock ordering.
	set jobIDSet
}

// jobIDSet is a set of job IDs.
type jobIDSet map[jobspb.JobID]struct{}

func newJobIDSet(ids ...jobspb.JobID) jobIDSet {
	m := make(map[jobspb.JobID]struct{}, len(ids))
	for _, j := range ids {
		m[j] = struct{}{}
	}
	return m
}

// installWaitingSet constructs a waiting set and installs it in the registry.
// If all the jobs execute to a terminal status in this registry, the done
// channel will be closed. The cleanup function must be called to avoid
// leaking memory.
func (r *Registry) installWaitingSet(
	ids ...jobspb.JobID,
) (jobDoneCh <-chan struct{}, cleanup func()) {
	ws := &waitingSet{
		jobDoneCh: make(chan struct{}),
		set:       newJobIDSet(ids...),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		sets, ok := r.mu.waiting[id]
		if !ok {
			sets = make(map[*waitingSet]struct{}, 1)
			r.mu.waiting[id] = sets
		}
		sets[ws] = struct{}{}
	}
	return ws.jobDoneCh, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for id := range ws.set {
			set, ok := r.mu.waiting[id]
			if !ok {
				log.Fatalf(
					r.ac.AnnotateCtx(context.Background()),
					"corruption detected in waiting set for id %d", id,
				)
			}
			delete(set, ws)
			delete(ws.set, id)
			if len(set) == 0 {
				delete(r.mu.waiting, id)
			}
		}
	}
}

func (r *Registry) removeFromWaitingSets(id jobspb.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sets := r.mu.waiting[id]
	for ws := range sets {
		delete(ws.set, id)
		if len(ws.set) == 0 {
			close(ws.jobDoneCh)
		}
	}
	delete(r.mu.waiting, id)
}
