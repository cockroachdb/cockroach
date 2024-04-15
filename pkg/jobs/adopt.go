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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	"go.opentelemetry.io/otel/attribute"
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

	claimQuery = `
   UPDATE system.jobs
      SET claim_session_id = $1, claim_instance_id = $2
    WHERE ((claim_session_id IS NULL)
      AND (status IN ` + claimableStatusTupleString + `))
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
	if sp == nil || sp.IsNoop() {
		// Should never be true since TraceableJobs force real tracing spans to be
		// attached to the context.
		return
	}

	if !r.settings.Version.IsActive(dumpCtx, clusterversion.TODODelete_V23_1) {
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
	// processQueryStatusTupleString includes the states of a job in which a
	// job can be claimed and resumed.
	processQueryStatusTupleString = `(` +
		`'` + string(StatusRunning) + `', ` +
		`'` + string(StatusReverting) + `'` +
		`)`

	// canRunArgs are used in canRunClause, which specify whether a job can be
	// run now or not.
	canRunArgs = `(SELECT $3::TIMESTAMP AS ts, $4::FLOAT AS initial_delay, $5::FLOAT AS max_delay) args`
	// NextRunClause calculates the next execution time of a job with exponential backoff delay, calculated
	// using last_run and num_runs values.
	NextRunClause = `
COALESCE(last_run::timestamptz, created::timestamptz) + least(
	IF(
		args.initial_delay * (power(2, least(62, COALESCE(num_runs, 0))) - 1)::FLOAT >= 0.0,
		args.initial_delay * (power(2, least(62, COALESCE(num_runs, 0))) - 1)::FLOAT,
		args.max_delay
	),
	args.max_delay
)::INTERVAL`
	canRunClause = `args.ts >= ` + NextRunClause

	// processQueryWithBackoff select IDs of the jobs that can be
	// processed among the claimed jobs.
	processQueryWithBackoff = `SELECT id FROM system.jobs, ` + canRunArgs +
		` WHERE status IN ` + processQueryStatusTupleString +
		` AND (claim_session_id = $1 AND claim_instance_id = $2)` +
		` AND ` + canRunClause

	// resumeQueryWithBackoff retrieves the job record for a job
	// we intend to resume.
	resumeQueryWithBackoff = `SELECT status, crdb_internal.sql_liveness_is_alive(claim_session_id), created_by_type, created_by_id, ` +
		canRunClause + " AS can_run" +
		" FROM system.jobs, " + canRunArgs +
		" WHERE id = $1 AND claim_session_id = $2"
)

// getProcessQuery returns the query that selects the jobs that are claimed
// by this node.
func getProcessQuery(
	ctx context.Context, s sqlliveness.Session, r *Registry,
) (string, []interface{}) {
	// Select the running or reverting jobs that this node has claimed that can be
	// executed right now.
	query := processQueryWithBackoff
	args := []interface{}{s.ID().UnsafeBytes(), r.ID(),
		r.clock.Now().GoTime(), r.RetryInitialDelay(), r.RetryMaxDelay()}
	return query, args
}

// processClaimedJobs processes all jobs currently claimed by the registry.
func (r *Registry) processClaimedJobs(ctx context.Context, s sqlliveness.Session) error {
	query, args := getProcessQuery(ctx, s, r)

	it, err := r.db.Executor().QueryIteratorEx(
		ctx, "select-running/get-claimed-jobs", nil,
		sessiondata.NodeUserSessionDataOverride, query, args...,
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
	if err := r.stopper.RunAsyncTask(resumeCtx, job.taskName(), func(ctx context.Context) {
		// Wait for the job to finish. No need to print the error because if there
		// was one it's been set in the job status already.
		var cleanup func()
		ctx, cleanup = r.stopper.WithCancelOnQuiesce(ctx)
		defer cleanup()
		_ = r.runJob(ctx, resumer, job, job.Status(), job.taskName())
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
		sessiondata.NodeUserSessionDataOverride, resumeQueryWithBackoff,
		jobID, s.ID().UnsafeBytes(),
		r.clock.Now().GoTime(), r.RetryInitialDelay(), r.RetryMaxDelay(),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "job %d: could not query job table row", jobID)
	}
	if row == nil {
		return nil, errors.Errorf("job %d: claim with session id %s does not exist", jobID, s.ID())
	}

	status := Status(*row[0].(*tree.DString))
	if status == StatusSucceeded {
		// A concurrent registry could have already executed the job.
		return nil, nil
	}
	if status != StatusRunning && status != StatusReverting {
		// A concurrent registry could have requested the job to be paused or canceled.
		return nil, errors.Errorf("job %d: status changed to %s which is not resumable", jobID, status)
	}

	if isAlive := *row[1].(*tree.DBool); !isAlive {
		return nil, errors.Errorf("job %d: claim with session id %s has expired", jobID, s.ID())
	}

	// It's too soon to run the job.
	//
	// We need this check to address a race between adopt-loop and an existing
	// resumer, e.g., in the following schedule:
	// Adopt loop: Cl(j,n1) St(r1)     Cl(j, n1)                       St(r2)
	// Resumer 1:                Rg(j)          Up(n1->n2) Fl(j) Ur(j)
	// Resumer 2:                                                            x-| Starting too soon
	// Where:
	//  - Cl(j,nx): claim job j when num_runs is x
	//  - St(r1): start resumer r1
	//  - Rg(j): Add jobID of j in adoptedJobs, disabling further resumers
	//  - Ur(j): Remove jobID of j from adoptedJobs, enabling further resumers
	//  - Up(n1->2): Update number of runs from 1 to 2
	//  - Fl(j): Job j fails
	if canRun := *row[4].(*tree.DBool); !canRun {
		return nil, nil
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
		payloadBytes, exists, err := infoStorage.GetLegacyPayload(ctx)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Wrap(&JobNotFoundError{jobID: jobID}, "job payload not found in system.job_info")
		}
		if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
			return err
		}

		progressBytes, exists, err := infoStorage.GetLegacyProgress(ctx)
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
	job.mu.status = status
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
	ctx context.Context, resumer Resumer, job *Job, status Status, taskName string,
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
	err := r.stepThroughStateMachine(ctx, execCtx, resumer, job, status, finalResumeError)
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
						 WHEN status = '` + string(StatusPauseRequested) + `' THEN '` + string(StatusPaused) + `'
						 WHEN status = '` + string(StatusCancelRequested) + `' THEN '` + string(StatusReverting) + `'
						 ELSE status
          END,
					num_runs = 0,
					last_run = NULL
    WHERE (status IN ('` + string(StatusPauseRequested) + `', '` + string(StatusCancelRequested) + `'))
      AND ((claim_session_id = $1) AND (claim_instance_id = $2))
RETURNING id, status
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
			statusString := *row[1].(*tree.DString)
			switch Status(statusString) {
			case StatusPaused:
				if !r.cancelRegisteredJobContext(id) {
					// If we didn't already have a running job for this lease,
					// clear out the lease here since it won't be cleared be
					// cleared out on Resume exit.
					r.clearLeaseForJobID(id, txn, txn.KV())
				}
				log.Infof(ctx, "job %d, session %s: paused", id, s.ID())
			case StatusReverting:
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
					// When we cancel a job, we want to reset its last_run and num_runs
					// so that the job can be picked-up in the next adopt-loop, sooner
					// than its current next-retry time.
					ju.UpdateRunStats(0 /* numRuns */, r.clock.Now().GoTime() /* lastRun */)
					return nil
				}); err != nil {
					return errors.Wrapf(err, "job %d: tried to cancel but could not mark as reverting", id)
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
