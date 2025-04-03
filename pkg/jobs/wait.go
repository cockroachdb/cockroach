// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
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

// NotifyToResume is used to notify the registry that it should attempt to
// resume the specified jobs. The assumption is that these jobs were created by
// this registry and thus are pre-claimed by it. This bypasses the loop to
// discover jobs already claimed by this registry. Jobs that are not claimed by
// this registry are silently ignored.
func (r *Registry) NotifyToResume(ctx context.Context, jobs ...jobspb.JobID) {
	m := newJobIDSet(jobs...)
	_ = r.stopper.RunAsyncTask(ctx, "resume-jobs", func(ctx context.Context) {
		ctx, cancel := r.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		r.withSession(ctx, func(ctx context.Context, s sqlliveness.Session) {
			r.filterAlreadyRunningAndCancelFromPreviousSessions(ctx, s, m)
			if !r.adoptionDisabled(ctx) {
				r.resumeClaimedJobs(ctx, s, m)
			}
		})
	})
}

// WaitForJobs waits for a given list of jobs to reach some sort
// of terminal state.
func (r *Registry) WaitForJobs(ctx context.Context, jobs []jobspb.JobID) error {
	log.Infof(ctx, "waiting for %d %v queued jobs to complete", len(jobs), jobs)
	jobFinishedLocally, cleanup := r.installWaitingSet(jobs...)
	defer cleanup()
	return r.waitForJobs(ctx, jobs, jobFinishedLocally)
}

// WaitForJobsIgnoringJobErrors is like WaitForJobs but it only
// returns an error in the case that polling the jobs table fails.
func (r *Registry) WaitForJobsIgnoringJobErrors(ctx context.Context, jobs []jobspb.JobID) error {
	log.Infof(ctx, "waiting for %d %v queued jobs to complete", len(jobs), jobs)
	jobFinishedLocally, cleanup := r.installWaitingSet(jobs...)
	defer cleanup()
	return r.waitForJobsToBeTerminalOrPaused(ctx, jobs, jobFinishedLocally)
}

func (r *Registry) waitForJobsToBeTerminalOrPaused(
	ctx context.Context, jobs []jobspb.JobID, jobFinishedLocally <-chan struct{},
) error {
	if len(jobs) == 0 {
		return nil
	}
	query := makeWaitForJobsQuery(jobs)
	// Manually retry instead of using SHOW JOBS WHEN COMPLETE so we have greater
	// control over retries. Also, avoiding SHOW JOBS prevents us from having to
	// populate the crdb_internal.jobs vtable.
	initialBackoff := 500 * time.Millisecond
	maxBackoff := 3 * time.Second

	if r.knobs.IntervalOverrides.WaitForJobsInitialDelay != nil {
		initialBackoff = *r.knobs.IntervalOverrides.WaitForJobsInitialDelay
	}

	if r.knobs.IntervalOverrides.WaitForJobsMaxDelay != nil {
		maxBackoff = *r.knobs.IntervalOverrides.WaitForJobsMaxDelay
	}

	ret := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
		Multiplier:     1.5,

		// Setting the closer here will terminate the loop if the job finishes
		// before the first InitialBackoff period.
		Closer: jobFinishedLocally,
	})
	ret.Next() // wait at least one InitialBackoff, first Next() doesn't block
	for ret.Next() {
		// We poll the number of queued jobs that aren't finished. As with SHOW JOBS
		// WHEN COMPLETE, if one of the jobs is missing from the jobs table for
		// whatever reason, we'll fail later when we try to load the job.
		if fn := r.knobs.BeforeWaitForJobsQuery; fn != nil {
			fn(jobs)
		}
		row, err := r.db.Executor().QueryRowEx(
			ctx,
			"poll-show-jobs",
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			query,
		)
		if err != nil {
			// While polling, we could encounter retryable errors, which may
			// not get internally retried in the connection executor. So retry,
			// here. When querying for jobs we will restart with a new transaction,
			// which will hopefully not hit this retryable error.
			if errors.HasInterface(err, (*pgerror.ClientVisibleRetryError)(nil)) {
				continue
			}
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
			return nil
		}
	}
	return nil
}

func (r *Registry) waitForJobs(
	ctx context.Context, jobs []jobspb.JobID, jobFinishedLocally <-chan struct{},
) error {
	if len(jobs) == 0 {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		log.Infof(ctx, "waited for %d %v queued jobs to complete %v",
			len(jobs), jobs, timeutil.Since(start))
	}()

	if err := r.waitForJobsToBeTerminalOrPaused(ctx, jobs, jobFinishedLocally); err != nil {
		return err
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
		if j.State() == StatePaused {
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

func makeWaitForJobsQuery(jobs []jobspb.JobID) string {
	var buf strings.Builder
	buf.WriteString(`SELECT count(*) FROM system.jobs WHERE status NOT IN ( ` +
		`'` + string(StateSucceeded) + `', ` +
		`'` + string(StateFailed) + `',` +
		`'` + string(StateCanceled) + `',` +
		`'` + string(StateRevertFailed) + `',` +
		`'` + string(StatePaused) + `'` +
		` ) AND id IN (`)
	for i, id := range jobs {
		if i > 0 {
			buf.WriteString(",")
		}
		_, _ = fmt.Fprintf(&buf, " %d", id)
	}
	buf.WriteString(")")
	return buf.String()
}

// Run starts previously unstarted jobs from a list of scheduled
// jobs. Canceling ctx interrupts the waiting but doesn't cancel the jobs.
func (r *Registry) Run(ctx context.Context, jobs []jobspb.JobID) error {
	if len(jobs) == 0 {
		return nil
	}
	done, cleanup := r.installWaitingSet(jobs...)
	defer cleanup()
	r.NotifyToResume(ctx, jobs...)
	return r.waitForJobs(ctx, jobs, done)
}

// jobWaitingSets stores the set of waitingSets currently waiting on a job ID.
type jobWaitingSets map[jobspb.JobID]map[*waitingSet]struct{}

// waitingSet is a set of job IDs that a local client is waiting to complete.
// It is an optimization for the Registry.Run() method which allows completion
// when a job is scheduled and run to completion locally to be communicated
// directly, without requiring the waiting goroutine to poll KV. This allows
// the jobs package to be responsive to job termination with much less
// contention on the table itself.
//
// The waitingSet is installed in Registry.installWaitingSet(), which returns
// a cleanup function which will remove the waiter from the relevant
// jobWaitingSets when the caller is no longer waiting. This cleanup makes any
// call to notify the waiter purely an optimization. If no calls to
// Registry.removeFromWaitingSet() were placed anywhere in the package, no
// resources would be wasted. In order to deal with the fact that the waiting
// set is an optimization, the caller still polls the job state to wait for it
// to transition to a terminal state (or paused). This is unavoidable: the job
// may end up running elsewhere.
type waitingSet struct {
	// jobDoneCh is closed when the set becomes empty because all
	// jobs in the set have entered a terminal state.
	jobDoneCh chan struct{}
	// Note that the set itself is only ever mutated under the registry's mu.
	// This choice is made to remove any concerns regarding lock ordering.
	// See Registry.removeFromWaitingSet() for the one place where this is
	// mutated. The code in the cleanup function returned from
	// Registry.installWaitingSet() uses this set to determine which entries in
	// the Registry's jobsWaitingSets which still need to be cleaned up.
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
// If all the jobs execute to a terminal state in this registry, the done
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
	cleanup = func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for id := range ws.set {
			set, ok := r.mu.waiting[id]
			if !ok {
				// This should never happen and indicates a programming error.
				log.Errorf(
					r.ac.AnnotateCtx(context.Background()),
					"corruption detected in waiting set for id %d", id,
				)
				continue
			}
			delete(set, ws)
			delete(ws.set, id)
			if len(set) == 0 {
				delete(r.mu.waiting, id)
			}
		}
	}
	return ws.jobDoneCh, cleanup
}

func (r *Registry) removeFromWaitingSets(id jobspb.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sets, ok := r.mu.waiting[id]
	if !ok {
		return
	}
	for ws := range sets {

		// Note that the set is only ever mutated underneath this mutex, so it's
		// not possible for any other goroutine to have observed the set become
		// empty.
		delete(ws.set, id)
		if len(ws.set) == 0 {
			close(ws.jobDoneCh)
		}
	}
	delete(r.mu.waiting, id)
}
