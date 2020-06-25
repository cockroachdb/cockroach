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

// claimJobs places a claim with the given epoch to job rows that are expired,
// ie whose claims are not in `liveClaims`.
// TODO(spaskob): instead of passing liveClaims and epoch we can use the claim
// manager embedded in the registry to get these values and possibly cache them.
func (r *Registry) claimJobs(
	ctx context.Context, epoch int64, liveClaims []sqlliveness.Claim,
) error {
	// TODO(spaskob): instead of treating NULL as absence of claim, we need a
	// special non-claimed value to be able to distinguish from legacy jobs.
	whereClaimExpired := "(sqlliveness_name IS NULL)"
	for _, c := range liveClaims {
		// TODO expand on what to do if there is claim with a name which not in the sqlliveness table.
		// Claims with epoch less than a liveness claim are expired.
		whereClaimExpired = fmt.Sprintf(
			"%s OR (sqlliveness_name = '%s' AND sqlliveness_epoch < %d)",
			whereClaimExpired, c.Name, c.Epoch)
	}
	return r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rows, err := r.ex.Query(
			ctx, "claim-jobs", txn, fmt.Sprintf(`
UPDATE system.jobs SET sqlliveness_name = $1, sqlliveness_epoch = $2
WHERE (%s) AND (%s) ORDER BY created DESC LIMIT $3 RETURNING id`,
				statusIsAdoptable, whereClaimExpired,
			), r.ID(), epoch, maxAdoptionsPerLoop,
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		log.Infof(ctx, "claimed %d jobs", len(rows))
		return nil
	})
}

// processClaimedJobs processes all jobs currently claimed by the registry.
// TODO(spaskob): instead of passing epoch we can use the claim manager embedded
// in the registry to get this value and possibly cache it.
func (r *Registry) processClaimedJobs(ctx context.Context, epoch int64) error {
	rows, err := r.ex.QueryEx(
		ctx, "select-running/reverting-jobs", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
SELECT id FROM system.jobs
WHERE (status = $1 OR status = $2) AND (sqlliveness_name = $3 AND sqlliveness_epoch = $4)`,
		StatusRunning, StatusReverting, r.ID(), epoch,
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
		if aj.epoch < epoch {
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
		if err := r.resumeJob(ctx, id, epoch); err != nil {
			log.Errorf(ctx, "could not run claimed job: %v", err)
		}
	}
	return nil
}

func (r *Registry) resumeJob(ctx context.Context, jobID, epoch int64) error {
	log.Infof(ctx, "job %d: attempting to resume", jobID)
	row, err := r.ex.QueryRowEx(
		ctx, "get-job-row", nil, sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT status, payload, progress FROM system.jobs WHERE id = $1 AND sqlliveness_epoch = $2`,
		jobID, epoch,
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
	r.mu.adoptedJobs[jobID] = &adoptedJob{epoch: epoch, cancel: cancel}
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

func (r *Registry) servePauseAndCancelRequests(ctx context.Context, epoch int64) error {
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
WHERE (status IN ($1, $3)) AND (sqlliveness_name = $5 AND sqlliveness_epoch = $6)
RETURNING id, status, sqlliveness_epoch`,
			StatusPauseRequested, StatusPaused,
			StatusCancelRequested, StatusReverting,
			r.ID(), epoch,
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		for _, row := range rows {
			id := int64(*row[0].(*tree.DInt))
			e := int64(*row[2].(*tree.DInt))
			job := &Job{id: &id, registry: r}
			switch Status(*row[1].(*tree.DString)) {
			case StatusPaused:
				r.unregister(id)
				log.Infof(ctx, "job %d, epoch %d: paused", id, e)
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
				log.Infof(ctx, "job %d, epoch: %d: canceled: the job is now reverting", id, e)
			default:
				log.ReportOrPanic(ctx, nil, "unexpected job status")
			}
		}
		return nil
	})
}
