// Copyright 2020 The Cockroach Authors.
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
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const (
	claimableStatusList = `'` + string(StatusRunning) + `', ` +
		`'` + string(StatusPending) + `', ` +
		`'` + string(StatusCancelRequested) + `', ` +
		`'` + string(StatusPauseRequested) + `', ` +
		`'` + string(StatusReverting) + `'`

	claimableStatusTupleString = `(` + claimableStatusList + `)`

	nonTerminalStatusList = claimableStatusList + `, ` +
		`'` + string(StatusPaused) + `'`

	// NonTerminalStatusTupleString is a sql tuple corresponding to statuses of
	// non-terminal jobs.
	NonTerminalStatusTupleString = `(` + nonTerminalStatusList + `)`
)

const claimQuery = `
   UPDATE system.jobs
      SET claim_session_id = $1, claim_instance_id = $2
    WHERE (claim_session_id IS NULL)
      AND (status IN ` + claimableStatusTupleString + `)
 ORDER BY created DESC
    LIMIT $3
RETURNING id;`

// claimJobs places a claim with the given SessionID to job rows that are
// available.
func (r *Registry) claimJobs(ctx context.Context, s sqlliveness.Session) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Run the claim transaction at low priority to ensure that it does not
		// contend with foreground reads.
		if err := txn.SetUserPriority(roachpb.MinUserPriority); err != nil {
			return errors.WithAssertionFailure(err)
		}
		numRows, err := r.ex.Exec(
			ctx, "claim-jobs", txn, claimQuery,
			s.ID().UnsafeBytes(), r.ID(), maxAdoptionsPerLoop,
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		if log.ExpensiveLogEnabled(ctx, 1) || numRows > 0 {
			log.Infof(ctx, "claimed %d jobs", numRows)
		}
		return nil
	})
}

// processClaimedJobs processes all jobs currently claimed by the registry.
func (r *Registry) processClaimedJobs(ctx context.Context, s sqlliveness.Session) error {
	it, err := r.ex.QueryIteratorEx(
		ctx, "select-running/get-claimed-jobs", nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()}, `
SELECT id FROM system.jobs
WHERE (status = $1 OR status = $2) AND (claim_session_id = $3 AND claim_instance_id = $4)`,
		StatusRunning, StatusReverting, s.ID().UnsafeBytes(), r.ID(),
	)
	if err != nil {
		return errors.Wrapf(err, "could not query for claimed jobs")
	}

	// This map will eventually contain the job ids that must be resumed.
	claimedToResume := make(map[jobspb.JobID]struct{})
	// Initially all claimed jobs are supposed to be resumed but some may be
	// running on this registry already so we will filter them out later.
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		id := jobspb.JobID(*row[0].(*tree.DInt))
		claimedToResume[id] = struct{}{}
	}
	if err != nil {
		return errors.Wrapf(err, "could not query for claimed jobs")
	}

	r.filterAlreadyRunningAndCancelFromPreviousSessions(ctx, s, claimedToResume)
	r.resumeClaimedJobs(ctx, s, claimedToResume)
	return nil
}

// resumeClaimedJobs invokes r.resumeJob for each job in claimedToResume. It
// does so concurrently.
func (r *Registry) resumeClaimedJobs(
	ctx context.Context, s sqlliveness.Session, claimedToResume map[jobspb.JobID]struct{},
) {
	const resumeConcurrency = 64
	sem := make(chan struct{}, resumeConcurrency)
	var wg sync.WaitGroup
	add := func() { sem <- struct{}{}; wg.Add(1) }
	done := func() { <-sem; wg.Done() }
	for id := range claimedToResume {
		add()
		go func(id jobspb.JobID) {
			defer done()
			if err := r.resumeJob(ctx, id, s); err != nil && ctx.Err() == nil {
				log.Errorf(ctx, "could not run claimed job %d: %v", id, err)
			}
		}(id)
	}
	wg.Wait()
}

// filterAlreadyRunningAndCancelFromPreviousSessions will lock the registry and
// inspect the set of currently running jobs, removing those entries from
// claimedToResume. Additionally it verifies that the session associated with the
// running job matches the current session, canceling the job if not.
func (r *Registry) filterAlreadyRunningAndCancelFromPreviousSessions(
	ctx context.Context, s sqlliveness.Session, claimedToResume map[jobspb.JobID]struct{},
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Process all current adopted jobs in our in-memory jobs map.
	for id, aj := range r.mu.adoptedJobs {
		if aj.sid != s.ID() {
			log.Warningf(ctx, "job %d: running without having a live claim; canceling", id)
			aj.cancel()
			delete(r.mu.adoptedJobs, id)
		} else {
			if _, ok := claimedToResume[id]; ok {
				// job id is already running no need to resume it then.
				delete(claimedToResume, id)
				continue
			}
		}
	}
}

// resumeJob resumes a claimed job.
func (r *Registry) resumeJob(ctx context.Context, jobID jobspb.JobID, s sqlliveness.Session) error {
	log.Infof(ctx, "job %d: resuming execution", jobID)
	row, err := r.ex.QueryRowEx(
		ctx, "get-job-row", nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()}, `
SELECT status, payload, progress, crdb_internal.sql_liveness_is_alive(claim_session_id)
FROM system.jobs WHERE id = $1 AND claim_session_id = $2`,
		jobID, s.ID().UnsafeBytes(),
	)
	if err != nil {
		return errors.Wrapf(err, "job %d: could not query job table row", jobID)
	}
	if row == nil {
		return errors.Errorf("job %d: claim with session id %s does not exist", jobID, s.ID())
	}

	status := Status(*row[0].(*tree.DString))
	if status == StatusSucceeded {
		// A concurrent registry could have already executed the job.
		return nil
	}
	if status != StatusRunning && status != StatusReverting {
		// A concurrent registry could have requested the job to be paused or canceled.
		return errors.Errorf("job %d: status changed to %s which is not resumable`", jobID, status)
	}

	if isAlive := *row[3].(*tree.DBool); !isAlive {
		return errors.Errorf("job %d: claim with session id %s has expired", jobID, s.ID())
	}

	payload, err := UnmarshalPayload(row[1])
	if err != nil {
		return err
	}

	// In version 20.1, the registry must not adopt 19.2-style schema change jobs
	// until they've undergone a migration.
	// TODO(lucy): Remove this in 20.2.
	if deprecatedIsOldSchemaChangeJob(payload) {
		log.VEventf(ctx, 2, "job %d: skipping adoption because schema change job has not been migrated", jobID)
		return nil
	}

	progress, err := UnmarshalProgress(row[2])
	if err != nil {
		return err
	}
	job := &Job{id: jobID, registry: r}
	job.mu.payload = *payload
	job.mu.progress = *progress
	job.sessionID = s.ID()

	resumer, err := r.createResumer(job, r.settings)
	if err != nil {
		return err
	}
	resumeCtx, cancel := r.makeCtx()

	aj := &adoptedJob{sid: s.ID(), cancel: cancel}
	r.addAdoptedJob(jobID, aj)
	if err := r.stopper.RunAsyncTask(ctx, job.taskName(), func(ctx context.Context) {
		// Wait for the job to finish. No need to print the error because if there
		// was one it's been set in the job status already.
		_ = r.runJob(resumeCtx, resumer, job, status, job.taskName())
	}); err != nil {
		r.removeAdoptedJob(jobID)
		return err
	}
	return nil
}

func (r *Registry) removeAdoptedJob(jobID jobspb.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.adoptedJobs, jobID)
}

func (r *Registry) addAdoptedJob(jobID jobspb.JobID, aj *adoptedJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.adoptedJobs[jobID] = aj
}

func (r *Registry) runJob(
	ctx context.Context, resumer Resumer, job *Job, status Status, taskName string,
) error {
	job.mu.Lock()
	var finalResumeError error
	if job.mu.payload.FinalResumeError != nil {
		finalResumeError = errors.DecodeError(ctx, *job.mu.payload.FinalResumeError)
	}
	username := job.mu.payload.UsernameProto.Decode()
	typ := job.mu.payload.Type()
	job.mu.Unlock()

	// Bookkeeping.
	execCtx, cleanup := r.execCtx("resume-"+taskName, username)
	defer cleanup()
	spanName := fmt.Sprintf(`%s-%d`, typ, job.ID())
	var span *tracing.Span

	// Create a new root span to trace the execution of the current instance of
	// `job`. Creating a root span allows us to track all the spans linked to this
	// job using the traceID allotted to the root span.
	//
	// A new root span will be created on every resumption of the job.
	var spanOptions []tracing.SpanOption
	if _, ok := resumer.(TraceableJob); ok {
		spanOptions = append(spanOptions, tracing.WithForceRealSpan())
	}
	ctx, span = r.settings.Tracer.StartSpanCtx(ctx, spanName, spanOptions...)
	defer span.Finish()
	if err := job.Update(ctx, nil /* txn */, func(txn *kv.Txn, md JobMetadata,
		ju *JobUpdater) error {
		md.Progress.TraceID = span.TraceID()
		ju.UpdateProgress(md.Progress)
		return nil
	}); err != nil {
		return err
	}

	// Run the actual job.
	err := r.stepThroughStateMachine(ctx, execCtx, resumer, job, status, finalResumeError)
	// If the context has been canceled, disregard errors for the sake of logging
	// as presumably they are due to the context cancellation which commonly
	// happens during shutdown.
	if err != nil && ctx.Err() == nil {
		log.Errorf(ctx, "job %d: adoption completed with error %v", job.ID(), err)
	}
	r.unregister(job.ID())
	return err
}

const cancelQuery = `
UPDATE system.jobs
SET status =
    CASE
      WHEN status = $1 THEN $2
      WHEN status = $3 THEN $4
      ELSE status
    END
WHERE (status IN ($1, $3)) AND ((claim_session_id = $5) AND (claim_instance_id = $6))
RETURNING id, status`

func (r *Registry) servePauseAndCancelRequests(ctx context.Context, s sqlliveness.Session) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Run the claim transaction at low priority to ensure that it does not
		// contend with foreground reads.
		if err := txn.SetUserPriority(roachpb.MinUserPriority); err != nil {
			return errors.WithAssertionFailure(err)
		}
		// Note that we have to buffer all rows first - before processing each
		// job - because we have to make sure that the query executes without an
		// error (otherwise, the system.jobs table might diverge from the jobs
		// registry).
		rows, err := r.ex.QueryBufferedEx(
			ctx, "cancel/pause-requested", txn, sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			cancelQuery,
			StatusPauseRequested, StatusPaused,
			StatusCancelRequested, StatusReverting,
			s.ID().UnsafeBytes(), r.ID(),
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		for _, row := range rows {
			id := jobspb.JobID(*row[0].(*tree.DInt))
			job := &Job{id: id, registry: r}
			statusString := *row[1].(*tree.DString)
			switch Status(statusString) {
			case StatusPaused:
				r.unregister(id)
				log.Infof(ctx, "job %d, session %s: paused", id, s.ID())
			case StatusReverting:
				if err := job.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
					r.unregister(id)
					md.Payload.Error = errJobCanceled.Error()
					encodedErr := errors.EncodeError(ctx, errJobCanceled)
					md.Payload.FinalResumeError = &encodedErr
					ju.UpdatePayload(md.Payload)
					return nil
				}); err != nil {
					return errors.Wrapf(err, "job %d: tried to cancel but could not mark as reverting: %s", id, err)
				}
				log.Infof(ctx, "job %d, session id: %s canceled: the job is now reverting",
					id, s.ID())
			default:
				return errors.AssertionFailedf("unexpected job status %s: %v", statusString, job)
			}
		}
		return nil
	})
}
