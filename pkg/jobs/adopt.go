// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
)

const (
	claimableStateList = `'` + string(StateRunning) + `', ` +
		`'` + string(StatePending) + `', ` +
		`'` + string(StateCancelRequested) + `', ` +
		`'` + string(StatePauseRequested) + `', ` +
		`'` + string(StateReverting) + `'`

	claimableStateTupleString = `(` + claimableStateList + `)`

	nonTerminalStateList = claimableStateList + `, ` +
		`'` + string(StatePaused) + `'`

	// NonTerminalStateTupleString is a sql tuple corresponding to states of
	// non-terminal jobs.
	NonTerminalStateTupleString = `(` + nonTerminalStateList + `)`

	claimQuery = `
   UPDATE system.jobs
      SET claim_session_id = $1, claim_instance_id = $2
    WHERE ((claim_session_id IS NULL)
      AND (status IN ` + claimableStateTupleString + `))
 ORDER BY created DESC
    LIMIT $3
RETURNING id;`
)

// maybeDumpTrace will conditionally persist the trace recording of the job's
// current resumer for consumption by job profiler tools. This method must be
// invoked before the tracing span corresponding to the job's current resumer is
// Finish()'ed.
func (r *Registry) maybeDumpTrace(resumerCtx context.Context, resumer Resumer, jobID jobspb.JobID) {
	if tj, ok := resumer.(TraceableJob); !ok || !tj.DumpTraceAfterRun() {
		return
	}

	// Make a new ctx to use in the trace dumper. This is because the resumerCtx
	// could have been canceled at this point.
	dumpCtx, _ := r.makeCtx()
	sp := tracing.SpanFromContext(resumerCtx)
	if sp == nil {
		// Should never be true since TraceableJobs force real tracing spans to be
		// attached to the context.
		return
	}

	resumerTraceFilename := fmt.Sprintf("%s/resumer-trace/%s",
		r.ID().String(), timeutil.Now().Format("20060102_150405.00"))
	td := jobspb.TraceData{CollectedSpans: sp.GetConfiguredRecording()}
	if err := r.db.Txn(dumpCtx, func(ctx context.Context, txn isql.Txn) error {
		return WriteProtobinExecutionDetailFile(dumpCtx, resumerTraceFilename, &td, txn, jobID)
	}); err != nil {
		log.Warningf(dumpCtx, "failed to write trace on resumer trace file: %v", err)
	}
}

// claimJobs places a claim with the given SessionID to job rows that are
// available.
func (r *Registry) claimJobs(ctx context.Context, s sqlliveness.Session) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Run the claim transaction at low priority to ensure that it does not
		// contend with foreground reads.
		if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
			return errors.WithAssertionFailure(err)
		}
		numRows, err := txn.Exec(
			ctx, "claim-jobs", txn.KV(), claimQuery,
			s.ID().UnsafeBytes(), r.ID(), maxAdoptionsPerLoop)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		r.metrics.ClaimedJobs.Inc(int64(numRows))
		if log.ExpensiveLogEnabled(ctx, 1) || numRows > 0 {
			log.Infof(ctx, "claimed %d jobs", numRows)
		}
		return nil
	})
}

const (
	// processQueryStateTupleString includes the states of a job in which a
	// job can be claimed and resumed.
	processQueryStateTupleString = `(` +
		`'` + string(StateRunning) + `', ` +
		`'` + string(StateReverting) + `'` +
		`)`

	// processQuery select IDs of the jobs that can be processed among the claimed jobs.
	processQuery = `SELECT id FROM system.jobs ` +
		` WHERE status IN ` + processQueryStateTupleString +
		` AND (claim_session_id = $1 AND claim_instance_id = $2)`

	// resumeQuery retrieves the job record for a job we intend to resume.
	resumeQuery = `SELECT status, crdb_internal.sql_liveness_is_alive(claim_session_id), created_by_type, created_by_id ` +
		"FROM system.jobs " +
		"WHERE id = $1 AND claim_session_id = $2"
)

// processClaimedJobs processes all jobs currently claimed by the registry.
func (r *Registry) processClaimedJobs(ctx context.Context, s sqlliveness.Session) error {
	it, err := r.db.Executor().QueryIteratorEx(
		ctx, "select-running/get-claimed-jobs", nil,
		sessiondata.NodeUserSessionDataOverride, processQuery, s.ID().UnsafeBytes(), r.ID(),
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
		if _, skip := testingIgnoreIDs[id]; skip {
			log.Warningf(ctx, "skipping execution of job %d as it is ignored by testing knob", id)
			continue
		}
		claimedToResume[id] = struct{}{}
	}
	if err != nil {
		return errors.Wrapf(err, "could not query for claimed jobs")
	}
	r.filterAlreadyRunningAndCancelFromPreviousSessions(ctx, s, claimedToResume)
	r.resumeClaimedJobs(ctx, s, claimedToResume)
	return nil
}

var testingIgnoreIDs map[jobspb.JobID]struct{}

// TestingSetIDsToIgnore is a test-only knob to set a list of job IDs that will
// not be executed even after being claimed.
func TestingSetIDsToIgnore(ignore map[jobspb.JobID]struct{}) func() {
	orig := testingIgnoreIDs
	testingIgnoreIDs = ignore
	return func() { testingIgnoreIDs = orig }
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
		if aj.session.ID() != s.ID() {
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
func (r *Registry) resumeJob(
	ctx context.Context, jobID jobspb.JobID, s sqlliveness.Session,
) (retErr error) {
	ctx = logtags.AddTag(ctx, "job", jobID)
	log.Infof(ctx, "job %d: resuming execution", jobID)

	job, err := r.loadJobForResume(ctx, jobID, s)
	if err != nil {
		return err
	}

	// A nil job return means we loaded the job but it isn't time
	// to resume it.
	if job == nil {
		return nil
	}

	resumer, err := r.createResumer(job)
	if err != nil {
		return err
	}
	resumeCtx, cancel := r.makeCtx()
	resumeCtx = logtags.AddTag(resumeCtx, "job", jobID)

	// If the job's type was registered to disable tenant cost control, then
	// exclude the job's costs from tenant accounting.
	payload := job.Payload()
	if opts, ok := getRegisterOptions(payload.Type()); ok && opts.disableTenantCostControl {
		resumeCtx = multitenant.WithTenantCostControlExemption(resumeCtx)
	}
	if alreadyAdopted := r.addAdoptedJob(jobID, s, cancel, resumer); alreadyAdopted {
		// Not needing the context after all. Avoid leaking resources.
		cancel()
		return nil
	}

	r.metrics.ResumedJobs.Inc(1)
	if err := r.stopper.RunAsyncTask(resumeCtx, string(job.taskName()), func(ctx context.Context) {
		// Wait for the job to finish. No need to print the error because if there
		// was one it's been set in the job state already.
		var cleanup func()
		ctx, cleanup = r.stopper.WithCancelOnQuiesce(ctx)
		defer cleanup()
		_ = r.runJob(ctx, resumer, job, job.State(), job.taskName())
	}); err != nil {
		r.unregister(jobID)
		// Also avoid leaking a goroutine in this case.
		cancel()
		return err
	}
	return nil
}

func (r *Registry) loadJobForResume(
	ctx context.Context, jobID jobspb.JobID, s sqlliveness.Session,
) (*Job, error) {
	ctx, sp := tracing.ChildSpan(ctx, "load-job-for-resume")
	defer sp.Finish()

	row, err := r.db.Executor().QueryRowEx(
		ctx, "get-job-row", nil,
		sessiondata.NodeUserSessionDataOverride, resumeQuery,
		jobID, s.ID().UnsafeBytes(),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "job %d: could not query job table row", jobID)
	}
	if row == nil {
		return nil, errors.Errorf("job %d: claim with session id %s does not exist", jobID, s.ID())
	}

	state := State(*row[0].(*tree.DString))
	if state == StateSucceeded {
		// A concurrent registry could have already executed the job.
		return nil, nil
	}
	if state != StateRunning && state != StateReverting {
		// A concurrent registry could have requested the job to be paused or canceled.
		return nil, errors.Errorf("job %d: state changed to %s which is not resumable", jobID, state)
	}

	if isAlive := *row[1].(*tree.DBool); !isAlive {
		return nil, errors.Errorf("job %d: claim with session id %s has expired", jobID, s.ID())
	}

	createdBy, err := unmarshalCreatedBy(row[2], row[3])
	if err != nil {
		return nil, err
	}
	job := &Job{id: jobID, registry: r, createdBy: createdBy}

	payload := &jobspb.Payload{}
	progress := &jobspb.Progress{}
	if err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job.InfoStorage(txn)
		payloadBytes, exists, err := infoStorage.GetLegacyPayload(ctx, "loadForResume")
		if err != nil {
			return err
		}
		if !exists {
			return errors.Wrap(&JobNotFoundError{jobID: jobID}, "job payload not found in system.job_info")
		}
		if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
			return err
		}

		progressBytes, exists, err := infoStorage.GetLegacyProgress(ctx, "loadForResume")
		if err != nil {
			return err
		}
		if !exists {
			return errors.Wrap(&JobNotFoundError{jobID: jobID}, "job progress not found in system.job_info")
		}
		return protoutil.Unmarshal(progressBytes, progress)
	}); err != nil {
		return nil, err
	}

	job.mu.payload = *payload
	job.mu.progress = *progress
	job.mu.state = state
	job.session = s
	return job, nil
}

// addAdoptedJob adds the job to the set of currently running jobs. This set is
// used for introspection, and, importantly, to serve as a lock to prevent
// concurrent executions. Removal occurs in runJob or in the case that we were
// unable to launch the goroutine to call runJob. If the returned boolean is
// false, it means that the job is already registered as running and should not
// be run again.
func (r *Registry) addAdoptedJob(
	jobID jobspb.JobID, session sqlliveness.Session, cancel context.CancelFunc, resumer Resumer,
) (alreadyAdopted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, alreadyAdopted = r.mu.adoptedJobs[jobID]; alreadyAdopted {
		return true
	}

	r.mu.adoptedJobs[jobID] = &adoptedJob{
		session: session,
		cancel:  cancel,
		isIdle:  false,
		resumer: resumer,
	}
	return false
}

func (r *Registry) runJob(
	ctx context.Context, resumer Resumer, job *Job, state State, taskName redact.SafeString,
) error {
	if r.IsDraining() {
		return errors.Newf("refusing to start %q; job registry is draining", taskName)
	}

	var finalResumeError error
	username, typ := func() (username.SQLUsername, jobspb.Type) {
		job.mu.Lock()
		defer job.mu.Unlock()
		if job.mu.payload.FinalResumeError != nil {
			finalResumeError = errors.DecodeError(ctx, *job.mu.payload.FinalResumeError)
		}
		return job.mu.payload.UsernameProto.Decode(), job.mu.payload.Type()
	}()

	// Make sure that we remove the job from the running set when this returns.
	defer r.unregister(job.ID())

	// Bookkeeping.
	execCtx, cleanup := r.execCtx(ctx, "resume-"+taskName, username)
	defer cleanup()

	// Create a new root span to trace the execution of the current instance of
	// `job`. Creating a root span allows us to track all the spans linked to this
	// job using the traceID allotted to the root span.
	//
	// A new root span will be created on every resumption of the job.
	var spanOptions []tracing.SpanOption
	if tj, ok := resumer.(TraceableJob); ok && tj.ForceRealSpan() {
		spanOptions = append(spanOptions, tracing.WithRecording(tracingpb.RecordingStructured))
	}

	ctx, span := r.ac.Tracer.StartSpanCtx(ctx,
		fmt.Sprintf("%s-%d", typ.String(), job.ID()), spanOptions...)
	span.SetTag("job-id", attribute.Int64Value(int64(job.ID())))
	defer span.Finish()

	// Run the actual job.
	err := r.stepThroughStateMachine(ctx, execCtx, resumer, job, state, finalResumeError)
	// If the context has been canceled, disregard errors for the sake of logging
	// as presumably they are due to the context cancellation which commonly
	// happens during shutdown.
	if err != nil && ctx.Err() == nil {
		log.Errorf(ctx, "job %d: adoption completed with error %v", job.ID(), err)
	}

	r.maybeRecordExecutionFailure(ctx, err, job)
	// NB: After this point, the job may no longer have the claim
	// and further updates to the job record from this node may
	// fail.
	r.maybeClearLease(job, err)
	r.maybeDumpTrace(ctx, resumer, job.ID())
	if r.knobs.AfterJobStateMachine != nil {
		r.knobs.AfterJobStateMachine(job.ID())
	}
	return err
}

const clearClaimQuery = `
   UPDATE system.jobs
      SET claim_session_id = NULL, claim_instance_id = NULL
    WHERE id = $1
      AND claim_session_id = $2
      AND claim_instance_id = $3`

// maybeClearLease clears the claim on the given job, provided that
// the current lease matches our liveness Session.
func (r *Registry) maybeClearLease(job *Job, jobErr error) {
	if jobErr == nil {
		return
	}
	r.clearLeaseForJobID(job.ID(), r.db.Executor(), nil /* txn */)
}

func (r *Registry) clearLeaseForJobID(jobID jobspb.JobID, ex isql.Executor, txn *kv.Txn) {
	// We use the serverCtx here rather than the context from the
	// caller since the caller's context may have been canceled.
	r.withSession(r.serverCtx, func(ctx context.Context, s sqlliveness.Session) {
		n, err := ex.ExecEx(ctx, "clear-job-claim", txn,
			sessiondata.NodeUserSessionDataOverride,
			clearClaimQuery, jobID, s.ID().UnsafeBytes(), r.ID())
		if err != nil {
			log.Warningf(ctx, "could not clear job claim: %s", err.Error())
			return
		}
		log.VEventf(ctx, 2, "cleared leases for %d jobs", n)
	})
}

const pauseAndCancelUpdate = `
   UPDATE system.jobs
      SET status =
          CASE
						 WHEN status = '` + string(StatePauseRequested) + `' THEN '` + string(StatePaused) + `'
						 WHEN status = '` + string(StateCancelRequested) + `' THEN '` + string(StateReverting) + `'
						 ELSE status
          END,
					num_runs = 0,
					last_run = NULL
    WHERE (status IN ('` + string(StatePauseRequested) + `', '` + string(StateCancelRequested) + `'))
      AND ((claim_session_id = $1) AND (claim_instance_id = $2))
RETURNING id, status, job_type
`

func (r *Registry) servePauseAndCancelRequests(ctx context.Context, s sqlliveness.Session) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Run the claim transaction at low priority to ensure that it does not
		// contend with foreground reads.
		if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
			return errors.WithAssertionFailure(err)
		}
		// Note that we have to buffer all rows first - before processing each
		// job - because we have to make sure that the query executes without an
		// error (otherwise, the system.jobs table might diverge from the jobs
		// registry).
		rows, err := txn.QueryBufferedEx(
			ctx, "cancel/pause-requested", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			pauseAndCancelUpdate, s.ID().UnsafeBytes(), r.ID(),
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		for _, row := range rows {
			id := jobspb.JobID(*row[0].(*tree.DInt))
			job := &Job{id: id, registry: r}
			stateString := *row[1].(*tree.DString)
			jobTypeString := *row[2].(*tree.DString)
			switch State(stateString) {
			case StatePaused:
				if !r.cancelRegisteredJobContext(id) {
					// If we didn't already have a running job for this lease,
					// clear out the lease here since it won't be cleared be
					// cleared out on Resume exit.
					r.clearLeaseForJobID(id, txn, txn.KV())
				}
				txn.KV().AddCommitTrigger(func(ctx context.Context) {
					LogStateChangeStructured(ctx,
						id,
						string(jobTypeString),
						nil,
						StatePauseRequested,
						StatePaused)
				})
				log.Infof(ctx, "job %d, session %s: paused", id, s.ID())
			case StateReverting:
				if err := job.WithTxn(txn).Update(ctx, func(
					txn isql.Txn, md JobMetadata, ju *JobUpdater,
				) error {
					if !r.cancelRegisteredJobContext(id) {
						// If we didn't already have a running job for this
						// lease, clear out the lease here since it won't be
						// cleared be cleared out on Resume exit.
						//
						// NB: This working as part of the update depends on
						// the fact that the job struct does not have a
						// claim set and thus won't validate the claim on
						// update.
						r.clearLeaseForJobID(id, txn, txn.KV())
					}
					if md.Payload.Error == "" {
						// Set default cancellation reason.
						md.Payload.Error = errJobCanceled.Error()
					}
					encodedErr := errors.EncodeError(ctx, errJobCanceled)
					md.Payload.FinalResumeError = &encodedErr
					ju.UpdatePayload(md.Payload)
					txn.KV().AddCommitTrigger(func(ctx context.Context) {
						LogStateChangeStructured(ctx,
							id,
							md.Payload.Type().String(),
							md.Payload,
							StateCancelRequested,
							StateReverting)
					})
					return nil
				}); err != nil {
					return errors.Wrapf(err, "job %d: tried to cancel but could not mark as reverting", id)
				}
				log.Infof(ctx, "job %d, session id: %s canceled: the job is now reverting",
					id, s.ID())
			default:
				return errors.AssertionFailedf("unexpected job state %s: %v", stateString, job)
			}
		}
		return nil
	})
}
