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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// Functions deprecated in release 20.2 that should be removed in release 21.1.

var (
	nodeLivenessLogLimiter = log.Every(5 * time.Second)
	// LeniencySetting is the amount of time to defer any attempts to
	// reschedule a job.  Visible for testing.
	LeniencySetting = settings.RegisterDurationSetting(
		"jobs.registry.leniency",
		"the amount of time to defer any attempts to reschedule a job",
		defaultLeniencySetting)
)

// lenientNow returns the timestamp after which we should attempt
// to steal a job from a node whose liveness is failing.  This allows
// jobs coordinated by a node which is temporarily saturated to continue.
func (r *Registry) lenientNow() time.Time {
	// We see this in tests.
	var offset time.Duration
	if r.settings == cluster.NoSettings {
		offset = defaultLeniencySetting
	} else {
		offset = LeniencySetting.Get(&r.settings.SV)
	}

	return r.clock.Now().GoTime().Add(-offset)
}

func (r *Registry) deprecatedMaybeAdoptJob(
	ctx context.Context, nlw optionalnodeliveness.Container, randomizeJobOrder bool,
) error {
	const stmt = `
SELECT id, payload, progress IS NULL, status
FROM system.jobs
WHERE status IN ($1, $2, $3, $4, $5) ORDER BY created DESC`
	rows, err := r.ex.Query(
		ctx, "adopt-job", nil /* txn */, stmt,
		StatusPending, StatusRunning, StatusCancelRequested, StatusPauseRequested, StatusReverting,
	)
	if err != nil {
		return errors.Wrap(err, "failed querying for jobs")
	}

	if randomizeJobOrder {
		rand.Seed(timeutil.Now().UnixNano())
		rand.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	}

	type nodeStatus struct {
		isLive bool
	}
	nodeStatusMap := map[roachpb.NodeID]*nodeStatus{
		// We treat invalidNodeID as an always-dead node so that
		// the empty lease (Lease{}) is always considered expired.
		invalidNodeID: {isLive: false},
	}
	// If no liveness is available, adopt all jobs. This is reasonable because this
	// only affects SQL tenants, which have at most one SQL server running on their
	// behalf at any given time.
	if nl, ok := nlw.Optional(54251); ok {
		// We subtract the leniency interval here to artificially
		// widen the range of times over which the job registry will
		// consider the node to be alive.  We rely on the fact that
		// only a live node updates its own expiration.  Thus, the
		// expiration time can be used as a reasonable measure of
		// when the node was last seen.
		now := r.lenientNow()
		for _, liveness := range nl.GetLivenesses() {
			nodeStatusMap[liveness.NodeID] = &nodeStatus{
				isLive: liveness.IsLive(now),
			}

			// NB: this code path should never be hit on tenants.
			nodeID, _ := r.nodeID.OptionalNodeID()
			// Don't try to start any more jobs unless we're really live,
			// otherwise we'd just immediately cancel them.
			if liveness.NodeID == nodeID {
				if !liveness.IsLive(r.clock.Now().GoTime()) {
					return errors.Errorf(
						"trying to adopt jobs on node %d which is not live", liveness.NodeID)
				}
			}
		}
	}

	if log.V(3) {
		log.Infof(ctx, "evaluating %d jobs for adoption", len(rows))
	}

	var adopted int
	for _, row := range rows {
		if adopted >= maxAdoptionsPerLoop {
			// Leave excess jobs for other nodes to get their fair share.
			break
		}

		id := (*int64)(row[0].(*tree.DInt))

		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		status := Status(tree.MustBeDString(row[3]))
		if log.V(3) {
			log.Infof(ctx, "job %d: evaluating for adoption with status `%s` and lease %v",
				*id, status, payload.Lease)
		}

		// In version 20.1, the registry must not adopt 19.2-style schema change
		// jobs until they've undergone a migration.
		// TODO (lucy): Remove this in 20.2.
		if deprecatedIsOldSchemaChangeJob(payload) {
			log.VEventf(ctx, 2, "job %d: skipping adoption because schema change job has not been migrated", id)
			continue
		}

		if payload.Lease == nil {
			// If the lease is missing, it simply means the job does not yet support
			// resumability.
			if log.V(2) {
				log.Infof(ctx, "job %d: skipping: nil lease", *id)
			}
			continue
		}

		// If the job has no progress it is from a 2.0 cluster. If the entire cluster
		// has been upgraded to 2.1 then we know nothing is running the job and it
		// can be safely failed.
		if nullProgress, ok := row[2].(*tree.DBool); ok && bool(*nullProgress) {
			log.Warningf(ctx, "job %d predates cluster upgrade and must be re-run", *id)
			versionErr := errors.New("job predates cluster upgrade and must be re-run")
			payload.Error = versionErr.Error()
			payloadBytes, err := protoutil.Marshal(payload)
			if err != nil {
				return err
			}

			// We can't use job.update here because it fails while attempting to unmarshal
			// the progress. Setting the status to failed is idempotent so we don't care
			// if multiple nodes execute this.
			const updateStmt = `UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3`
			updateArgs := []interface{}{StatusFailed, payloadBytes, *id}
			err = r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := r.ex.Exec(ctx, "job-update", txn, updateStmt, updateArgs...)
				return err
			})
			if err != nil {
				log.Warningf(ctx, "job %d: has no progress but unable to mark failed: %s", *id, err)
			}
			continue
		}

		r.mu.Lock()
		_, runningOnNode := r.mu.deprecatedJobs[*id]
		r.mu.Unlock()

		// If we're running as a tenant (!ok), then we are the sole SQL server in
		// charge of its jobs and ought to adopt all of them. Otherwise, look more
		// closely at who is running the job and whether to adopt.
		if nodeID, ok := r.nodeID.OptionalNodeID(); ok && nodeID != payload.Lease.NodeID {
			// Another node holds the lease on the job, see if we should steal it.
			if runningOnNode {
				// If we are currently running a job that another node has the lease on,
				// stop running it.
				log.Warningf(ctx, "job %d: node %d owns lease; canceling", *id, payload.Lease.NodeID)
				r.unregister(*id)
				continue
			}
			nodeStatus, ok := nodeStatusMap[payload.Lease.NodeID]
			if !ok {
				// This case can happen when a node first starts up and runs schema
				// migrations.
				log.Warningf(ctx, "job %d: skipping: no liveness record for the job's node %d",
					log.Safe(*id), payload.Lease.NodeID)
				continue
			}
			if nodeStatus.isLive {
				if log.V(2) {
					log.Infof(ctx, "job %d: skipping: another node is live and holds the lease", *id)
				}
				continue
			}
		}

		// Below we know that this node holds the lease on the job, or that we want
		// to adopt it anyway because the leaseholder seems dead.
		job := &Job{id: id, registry: r}
		resumeCtx, cancel := r.makeCtx()

		if pauseRequested := status == StatusPauseRequested; pauseRequested {
			if err := job.paused(ctx, func(context.Context, *kv.Txn) error {
				r.unregister(*id)
				return nil
			}); err != nil {
				log.Errorf(ctx, "job %d: could not set to paused: %v", *id, err)
				continue
			}
			log.Infof(ctx, "job %d: paused", *id)
			continue
		}

		if cancelRequested := status == StatusCancelRequested; cancelRequested {
			if err := job.reverted(ctx, errJobCanceled, func(context.Context, *kv.Txn) error {
				// Unregister the job in case it is running on the node.
				// Unregister is a no-op for jobs that are not running.
				r.unregister(*id)
				return nil
			}); err != nil {
				log.Errorf(ctx, "job %d: could not set to reverting: %v", *id, err)
				continue
			}
			log.Infof(ctx, "job %d: canceled: the job is now reverting", *id)
		} else if currentlyRunning := r.deprecatedRegister(*id, cancel) != nil; currentlyRunning {
			if log.V(3) {
				log.Infof(ctx, "job %d: skipping: the job is already running/reverting on this node", *id)
			}
			continue
		}

		// Check if job status has changed in the meanwhile.
		currentStatus, err := job.CurrentStatus(ctx)
		if err != nil {
			return err
		}
		if status != currentStatus {
			continue
		}
		// Adopt job and resume/revert it.
		if err := job.deprecatedAdopt(ctx, payload.Lease); err != nil {
			r.unregister(*id)
			return errors.Wrap(err, "unable to acquire lease")
		}

		resultsCh := make(chan tree.Datums)
		resumer, err := r.createResumer(job, r.settings)
		if err != nil {
			r.unregister(*id)
			return err
		}
		log.Infof(ctx, "job %d: resuming execution", *id)
		errCh, err := r.deprecatedResume(resumeCtx, resumer, resultsCh, job, nil)
		if err != nil {
			r.unregister(*id)
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

		adopted++
	}

	return nil
}

func (r *Registry) deprecatedNewLease() *jobspb.Lease {
	// This code path should never be hit on tenants.
	nodeID, _ := r.nodeID.OptionalNodeID()
	if nodeID == 0 {
		panic("jobs.Registry has empty node ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &jobspb.Lease{NodeID: nodeID, Epoch: r.mu.deprecatedEpoch}
}

func (r *Registry) deprecatedCancelAllLocked(ctx context.Context) {
	r.mu.AssertHeld()
	for jobID, cancel := range r.mu.deprecatedJobs {
		log.Warningf(ctx, "job %d: canceling due to liveness failure", jobID)
		cancel()
	}
	for jobID := range r.mu.deprecatedJobs {
		delete(r.mu.deprecatedJobs, jobID)
	}
}

// deprecatedRegister registers an about to be resumed job in memory so that it can be
// killed and that no one else tries to resume it. This essentially works as a
// barrier that only one function can cross and try to resume the job.
func (r *Registry) deprecatedRegister(jobID int64, cancel func()) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// We need to prevent different routines trying to adopt and resume the job.
	if _, alreadyRegistered := r.mu.deprecatedJobs[jobID]; alreadyRegistered {
		return errors.Errorf("job %d: already registered", jobID)
	}
	r.mu.deprecatedJobs[jobID] = cancel
	return nil
}

func (r *Registry) deprecatedCancelAll(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deprecatedCancelAllLocked(ctx)
}

// deprecatedResume starts or resumes a job. If no error is returned then the
// job was asynchronously executed. The job is executed with the ctx, so ctx
// must only by canceled if the job should also be canceled. resultsCh is passed
// to the resumable func and should be closed by the caller after errCh sends
// a value. The onDone function is called when the async task completes or if
// an error is returned.
func (r *Registry) deprecatedResume(
	ctx context.Context, resumer Resumer, resultsCh chan<- tree.Datums, job *Job, onDone func(),
) (<-chan error, error) {
	errCh := make(chan error, 1)
	if err := r.stopper.RunAsyncTask(ctx, job.taskName(), func(ctx context.Context) {
		if onDone != nil {
			defer onDone()
		}
		// Bookkeeping.
		payload := job.Payload()
		execCtx, cleanup := r.execCtx("resume-"+job.taskName(), payload.UsernameProto.Decode())
		defer cleanup()
		spanName := fmt.Sprintf(`%s-%d`, payload.Type(), *job.ID())
		var span *tracing.Span
		ctx, span = r.ac.AnnotateCtxWithSpan(ctx, spanName)
		defer span.Finish()

		// Run the actual job.
		status, err := job.CurrentStatus(ctx)
		if err == nil {
			var finalResumeError error
			if job.Payload().FinalResumeError != nil {
				finalResumeError = errors.DecodeError(ctx, *job.Payload().FinalResumeError)
			}
			err = r.stepThroughStateMachine(ctx, execCtx, resumer, resultsCh, job, status, finalResumeError)
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
			status, err := job.CurrentStatus(ctx)
			if err != nil {
				log.Errorf(ctx, "job %d: failed querying status: %v", *job.ID(), err)
			} else {
				log.Infof(ctx, "job %d: status %s after adoption finished", *job.ID(), status)
			}
		}
		r.unregister(*job.ID())
		errCh <- err
	}); err != nil {
		if onDone != nil {
			onDone()
		}
		return nil, err
	}
	return errCh, nil
}

func (j *Job) deprecatedInsert(
	ctx context.Context, id int64, lease *jobspb.Lease, session sqlliveness.Session,
) error {
	if j.id != nil {
		// Already created - do nothing.
		return nil
	}

	j.mu.payload.Lease = lease

	if err := j.runInTxn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Note: although the following uses ReadTimestamp and
		// ReadTimestamp can diverge from the value of now() throughout a
		// transaction, this may be OK -- we merely required ModifiedMicro
		// to be equal *or greater* than previously inserted timestamps
		// computed by now(). For now ReadTimestamp can only move forward
		// and the assertion ReadTimestamp >= now() holds at all times.
		j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
		payloadBytes, err := protoutil.Marshal(&j.mu.payload)
		if err != nil {
			return err
		}
		progressBytes, err := protoutil.Marshal(&j.mu.progress)
		if err != nil {
			return err
		}
		// If there's no session then we know that the sqlliveness subsystem hasn't
		// been started and thus there's no claim that the corresponding columns
		// exist.
		if session == nil && !j.registry.startUsingSQLLivenessAdoption(ctx) {
			const stmt = `INSERT
  INTO system.jobs (
                    id,
                    status,
                    payload,
                    progress
                   )
VALUES ($1, $2, $3, $4);`
			_, err = j.registry.ex.Exec(ctx, "job-insert", txn, stmt,
				id, StatusRunning, payloadBytes, progressBytes)
			return err
		}

		var claimInstanceID, sessionID, createdByType, createdByID interface{}
		if session != nil {
			j.sessionID = session.ID()
			sessionID = j.sessionID.UnsafeBytes()
			claimInstanceID = int64(j.registry.ID())
		}
		if j.createdBy != nil {
			createdByType = j.createdBy.Name
			createdByID = j.createdBy.ID
		}
		const stmt = `
INSERT
  INTO system.jobs (
                    id,
                    status,
                    payload,
                    progress,
                    created_by_type,
                    created_by_id,
                    claim_session_id,
                    claim_instance_id
                   )
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
		_, err = j.registry.ex.Exec(ctx, "job-insert", txn, stmt,
			id, StatusRunning, payloadBytes, progressBytes,
			createdByType, createdByID,
			sessionID, claimInstanceID)
		return err
	}); err != nil {
		return err
	}
	j.id = &id
	return nil
}

func (j *Job) deprecatedAdopt(ctx context.Context, oldLease *jobspb.Lease) error {
	return j.Update(ctx, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if !md.Payload.Lease.Equal(oldLease) {
			return errors.Errorf("current lease %v did not match expected lease %v",
				md.Payload.Lease, oldLease)
		}
		md.Payload.Lease = j.registry.deprecatedNewLease()
		if md.Payload.StartedMicros == 0 {
			md.Payload.StartedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		}
		ju.UpdatePayload(md.Payload)
		return nil
	})
}
