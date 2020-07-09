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
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// SQL WHERE filter for jobs that could be adopted.
var statusIsAdoptable = fmt.Sprintf("status IN ('%s', '%s', '%s', '%s')",
	StatusRunning, StatusCancelRequested, StatusPauseRequested, StatusReverting)

// claimJobs places a claim with the given SessionID to job rows that are
// available.
func (r *Registry) claimJobs(ctx context.Context, s sqlliveness.Session) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rows, err := r.ex.Query(
			ctx, "claim-jobs", txn, `
UPDATE system.jobs SET claim_session_id = $1, claim_instance_id = $2
WHERE claim_session_id IS NULL ORDER BY created DESC LIMIT $3 RETURNING id`,
			s.ID(), r.ID(), maxAdoptionsPerLoop,
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		log.Infof(ctx, "claimed %d jobs", len(rows))
		return nil
	})
}

// processClaimedJobs processes all jobs currently claimed by the registry.
func (r *Registry) processClaimedJobs(ctx context.Context, s sqlliveness.Session) error {
	rows, err := r.ex.QueryEx(
		ctx, "select-running/reverting-jobs", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
SELECT id FROM system.jobs
WHERE (status = $1 OR status = $2) AND (claim_session_id = $3 AND claim_instance_id = $4)`,
		StatusRunning, StatusReverting, s.ID(), r.ID(),
	)
	if err != nil {
		return errors.Wrapf(err, "could query for claimed jobs")
	}

	// This map will eventually contain the job ids that must be resumed.
	claimedToResume := make(map[int64]bool, len(rows))
	// Initially all claimed jobs are supposed to be resumed but some may be
	// running on this registry already so we will filter them out later.
	for _, row := range rows {
		id := int64(*row[0].(*tree.DInt))
		claimedToResume[id] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Process all current adopted jobs in our in-memory jobs map.
	for id, aj := range r.mu.adoptedJobs {
		if !aj.sid.Equals(s.ID()) {
			log.Warningf(ctx, "job %d: running without having a live claim; killed.", id)
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

	for id := range claimedToResume {
		if err := r.resumeJob(ctx, id, s); err != nil {
			log.Errorf(ctx, "could not run claimed job: %v", err)
		}
	}
	return nil
}

func (r *Registry) resumeJob(ctx context.Context, jobID int64, s sqlliveness.Session) error {
	log.Infof(ctx, "job %d: attempting to resume", jobID)
	row, err := r.ex.QueryRowEx(
		ctx, "get-job-row", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
SELECT status, payload, progress FROM system.jobs WHERE id = $1 AND claim_session_id = $2`,
		jobID, s.ID(),
	)
	if err != nil {
		return errors.Wrapf(err, "job %d: could not query job table row", jobID)
	}
	if row == nil {
		return errors.Errorf("job %d: epoch has changed, no longer eligible for resuming", jobID)
	}

	status := Status(*row[0].(*tree.DString))
	if status != StatusRunning && status != StatusReverting {
		// A concurrent registry could have requested the job to be paused or cancelled.
		return errors.Errorf("job %d: status changed to %s which is not resumable`", jobID, status)
	}

	payload, err := UnmarshalPayload(row[1])
	if err != nil {
		return err
	}
	progress, err := UnmarshalProgress(row[2])
	if err != nil {
		return err
	}
	job := &Job{id: &jobID, registry: r}
	job.mu.payload = *payload
	job.mu.progress = *progress

	resumer, err := r.createResumer(job, r.settings)
	if err != nil {
		return err
	}
	resumeCtx, cancel := r.makeCtx()
	resultsCh := make(chan tree.Datums)

	errCh := make(chan error, 1)
	taskName := fmt.Sprintf(`job-%d`, jobID)
	r.mu.adoptedJobs[jobID] = &adoptedJob{sid: s.ID(), cancel: cancel}
	if err := r.stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		r.runJob(resumeCtx, resumer, resultsCh, errCh, job, status, taskName)
	}); err != nil {
		delete(r.mu.adoptedJobs, jobID)
		return err
	}
	go func() {
		// Drain and ignore results.
		for range resultsCh {
		}
	}()
	go func() {
		// Wait for the job to finish. No need to print the error because if there
		// was one it's been set in the job status already.
		<-errCh
		close(resultsCh)
	}()
	return nil
}

func (r *Registry) runJob(
	ctx context.Context,
	resumer Resumer,
	resultsCh chan<- tree.Datums,
	errCh chan<- error,
	job *Job,
	status Status,
	taskName string,
) {
	// Bookkeeping.
	phs, cleanup := r.planFn("resume-"+taskName, job.mu.payload.Username)
	defer cleanup()
	spanName := fmt.Sprintf(`%s-%d`, job.mu.payload.Type(), *job.ID())
	var span opentracing.Span
	ctx, span = r.ac.AnnotateCtxWithSpan(ctx, spanName)
	defer span.Finish()

	// Run the actual job.
	var finalResumeError error
	if job.mu.payload.FinalResumeError != nil {
		finalResumeError = errors.DecodeError(ctx, *job.mu.payload.FinalResumeError)
	}
	err := r.stepThroughStateMachine(ctx, phs, resumer, resultsCh, job, status, finalResumeError)
	if err != nil {
		// TODO (lucy): This needs to distinguish between assertion errors in
		// the job registry and assertion errors in job execution returned from
		// Resume() or OnFailOrCancel(), and only fail on the former. We have
		// tests that purposely introduce bad state in order to produce
		// assertion errors, which shouldn't cause the test to panic. For now,
		// comment this out.
		// if errors.HasAssertionFailure(err) {
		// 	log.ReportOrPanic(ctx, nil, err.Error())
		// }
		log.Errorf(ctx, "job %d: adoption completed with error %v", *job.ID(), err)
	}
	r.unregister(*job.ID())
	errCh <- err
}

func (r *Registry) servePauseAndCancelRequests(ctx context.Context, s sqlliveness.Session) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rows, err := r.ex.QueryEx(
			ctx, "cancel/pause-requested", txn, sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
UPDATE system.jobs
SET status =
		CASE
			WHEN status = $1 THEN $2
			WHEN status = $3 THEN $4
			ELSE status
		END 
WHERE (status IN ($1, $3)) AND (claim_session_id = $5 AND claim_instance_id = $6)
RETURNING id, status`,
			StatusPauseRequested, StatusPaused,
			StatusCancelRequested, StatusReverting,
			s.ID(), r.ID(),
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		for _, row := range rows {
			id := int64(*row[0].(*tree.DInt))
			job := &Job{id: &id, registry: r}
			switch Status(*row[1].(*tree.DString)) {
			case StatusPaused:
				r.unregister(id)
				log.Infof(ctx, "job %d, session %s: paused", id, hex.EncodeToString(s.ID()))
			case StatusReverting:
				if err := job.WithTxn(txn).Update(ctx, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
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
					id, hex.EncodeToString(s.ID()))
			default:
				log.ReportOrPanic(ctx, nil, "unexpected job status")
			}
		}
		return nil
	})
}
