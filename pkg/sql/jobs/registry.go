// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package jobs

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var nodeLivenessLogLimiter = log.Every(5 * time.Second)

// nodeLiveness is the subset of storage.NodeLiveness's interface needed
// by Registry.
type nodeLiveness interface {
	Self() (*storage.Liveness, error)
	GetLivenesses() []storage.Liveness
}

// Registry creates Jobs and manages their leases and cancelation.
type Registry struct {
	ac        log.AmbientContext
	db        *client.DB
	ex        sqlutil.InternalExecutor
	gossip    *gossip.Gossip
	clock     *hlc.Clock
	nodeID    *base.NodeIDContainer
	clusterID func() uuid.UUID
	settings  *cluster.Settings

	mu struct {
		syncutil.Mutex
		epoch int64
		// jobs holds a map from job id to its context cancel func. This should
		// be populated with jobs that are currently being run (and owned) by
		// this registry. Calling the func will cancel the context the job was
		// started/resumed with. This should only be called by the registry when
		// it is attempting to halt its own jobs due to liveness problems. Jobs
		// are normally canceled on any node by the CANCEL JOB statement, which is
		// propogated to jobs via the .Progressed call. This function should not be
		// used to cancel a job in that way.
		jobs map[int64]context.CancelFunc
	}
}

// MakeRegistry creates a new Registry.
func MakeRegistry(
	ac log.AmbientContext,
	clock *hlc.Clock,
	db *client.DB,
	ex sqlutil.InternalExecutor,
	gossip *gossip.Gossip,
	nodeID *base.NodeIDContainer,
	clusterID func() uuid.UUID,
	settings *cluster.Settings,
) *Registry {
	r := &Registry{
		ac:        ac,
		clock:     clock,
		db:        db,
		ex:        ex,
		gossip:    gossip,
		nodeID:    nodeID,
		clusterID: clusterID,
		settings:  settings,
	}
	r.mu.epoch = 1
	r.mu.jobs = make(map[int64]context.CancelFunc)
	return r
}

// makeCtx returns a new context from r's ambient context and an associated
// cancel func.
func (r *Registry) makeCtx() (context.Context, func()) {
	return context.WithCancel(r.ac.AnnotateCtx(context.Background()))
}

func (r *Registry) makeJobID() int64 {
	return int64(builtins.GenerateUniqueInt(r.nodeID.Get()))
}

// StartJob creates and asynchronously starts a job from record. An error is
// returned if the job type has not been registered with AddResumeHook. The
// ctx passed to this function is not the context the job will be started
// with (canceling ctx will not causing the job to cancel).
func (r *Registry) StartJob(
	ctx context.Context, resultsCh chan<- tree.Datums, record Record,
) (*Job, <-chan error, error) {
	resumer, err := getResumeHook(detailsType(WrapPayloadDetails(record.Details)), r.settings)
	if err != nil {
		return nil, nil, err
	}
	j := r.NewJob(record)
	// We create the job with a known ID, rather than relying on the column's
	// DEFAULT unique_rowid(), to avoid a race condition where the job exists
	// in the jobs table but is not yet present in our registry, which would
	// allow another node to adopt it.
	id := r.makeJobID()
	resumeCtx, cancel := r.makeCtx()
	r.register(id, cancel)
	if err := j.created(ctx, id, r.newLease()); err != nil {
		r.unregister(id)
		return nil, nil, err
	}
	if err := j.Started(ctx); err != nil {
		r.unregister(id)
		return nil, nil, err
	}
	errCh := r.resume(resumeCtx, resumer, resultsCh, j)
	return j, errCh, nil
}

// NewJob creates a new Job.
func (r *Registry) NewJob(record Record) *Job {
	return &Job{
		Record:   record,
		registry: r,
	}
}

// LoadJob loads an existing job with the given jobID from the system.jobs
// table.
func (r *Registry) LoadJob(ctx context.Context, jobID int64) (*Job, error) {
	return r.LoadJobWithTxn(ctx, jobID, nil)
}

// LoadJobWithTxn does the same as above, but using the transaction passed in
// the txn argument. Passing a nil transaction is equivalent to calling LoadJob
// in that a transaction will be automatically created.
func (r *Registry) LoadJobWithTxn(ctx context.Context, jobID int64, txn *client.Txn) (*Job, error) {
	j := &Job{
		id:       &jobID,
		registry: r,
	}
	if err := j.WithTxn(txn).load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// DefaultCancelInterval is a reasonable interval at which to poll this node
// for liveness failures and cancel running jobs.
const DefaultCancelInterval = base.DefaultHeartbeatInterval

// DefaultAdoptInterval is a reasonable interval at which to poll system.jobs
// for jobs with expired leases.
//
// DefaultAdoptInterval is mutable for testing. NB: Updates to this value after
// Registry.Start has been called will not have any effect.
var DefaultAdoptInterval = 30 * time.Second

// Start polls the current node for liveness failures and cancels all registered
// jobs if it observes a failure.
func (r *Registry) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	nl nodeLiveness,
	cancelInterval, adoptInterval time.Duration,
) error {
	// Calling maybeCancelJobs once at the start ensures we have an up-to-date
	// liveness epoch before we wait out the first cancelInterval.
	r.maybeCancelJobs(ctx, nl)

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-time.After(cancelInterval):
				r.maybeCancelJobs(ctx, nl)
			case <-stopper.ShouldStop():
				return
			}
		}
	})

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-time.After(adoptInterval):
				if err := r.maybeAdoptJob(ctx, nl); err != nil {
					log.Errorf(ctx, "error while adopting jobs: %+v", err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

func (r *Registry) maybeCancelJobs(ctx context.Context, nl nodeLiveness) {
	liveness, err := nl.Self()
	if err != nil {
		if nodeLivenessLogLimiter.ShouldLog() {
			log.Warningf(ctx, "unable to get node liveness: %s", err)
		}
		// Conservatively assume our lease has expired. Abort all jobs.
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cancelAll(ctx)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO(benesch): this logic is correct but too aggressive. Jobs created
	// immediately after a liveness failure but before we've updated our cached
	// epoch will be unnecessarily canceled.
	//
	// Additionally consider handling the case where our locally-cached liveness
	// record is expired, but a non-expired liveness record has, in fact, been
	// successfully persisted. Rather than canceling all jobs immediately, we
	// could instead wait to see if we managed a successful heartbeat at the
	// current epoch. The additional complexity this requires is not clearly
	// worthwhile.
	sameEpoch := liveness.Epoch == r.mu.epoch
	if !sameEpoch || !liveness.IsLive(r.clock.Now(), r.clock.MaxOffset()) {
		r.cancelAll(ctx)
		r.mu.epoch = liveness.Epoch
	}
}

// getJobFn attempts to get a resumer from the given job id. If the job id
// does not have a resumer then it returns an error message suitable for users.
func (r *Registry) getJobFn(ctx context.Context, txn *client.Txn, id int64) (*Job, Resumer, error) {
	job, err := r.LoadJobWithTxn(ctx, id, txn)
	if err != nil {
		return nil, nil, err
	}
	payload := job.Payload()
	resumer, err := getResumeHook(payload.Type(), r.settings)
	if err != nil {
		return job, nil, errors.Errorf("job %d is not controllable", id)
	}
	return job, resumer, nil
}

// Cancel marks the job with id as canceled using the specified txn (may be nil).
func (r *Registry) Cancel(ctx context.Context, txn *client.Txn, id int64) error {
	job, resumer, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		// Special case IMPORT jobs to mark the job as canceled.
		if job != nil {
			payload := job.Payload()
			if payload.Type() == TypeImport {
				return job.WithTxn(txn).canceled(ctx, NoopFn)
			}
		}
		return err
	}
	return job.WithTxn(txn).canceled(ctx, resumer.OnFailOrCancel)
}

// Pause marks the job with id as paused using the specified txn (may be nil).
func (r *Registry) Pause(ctx context.Context, txn *client.Txn, id int64) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).paused(ctx)
}

// Resume resumes the paused job with id using the specified txn (may be nil).
func (r *Registry) Resume(ctx context.Context, txn *client.Txn, id int64) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).resumed(ctx)
}

// Resumer is a resumable Job. Jobs can be paused or canceled at any time. Jobs
// should call their .Progressed method, which will return an error if the
// job has been paused or canceled. Functions that take a client.Txn argument
// must ensure that any work they do is still correct if the txn is aborted
// at a later time.
type Resumer interface {
	// Resume is called when a job is started or resumed. Sending results on
	// the chan will return them to a user, if a user's session is connected.
	Resume(context.Context, *Job, chan<- tree.Datums) error
	// OnSuccess is called when a job has completed successfully, and is called
	// with the same txn that will mark the job as successful. The txn will
	// only be committed if this doesn't return an error and the job state was
	// successfully changed to successful. If OnSuccess returns an error, the
	// job will be marked as failed.
	OnSuccess(context.Context, *client.Txn, *Job) error
	// OnTerminal is called after a job has successfully been marked as
	// terminal. It should be used to perform optional cleanup and return final
	// results to the user. There is no guranatee that this function is ever run
	// (for example, if a node died immediately after Success commits).
	OnTerminal(context.Context, *Job, Status, chan<- tree.Datums)

	// OnFailOrCancel is called when a job fails or is canceled, and is called
	// with the same txn that will mark the job as failed or canceled. The txn
	// will only be committed if this doesn't return an error and the job state
	// was successfully changed to failed or canceled. This is done so that
	// transactional cleanup can be guaranteed to have happened.
	//
	// Since this method can be called during cancellation, which is not
	// guaranteed to run on the node where the job is running, it is not safe
	// for it to use state from the Resumer.
	OnFailOrCancel(context.Context, *client.Txn, *Job) error
}

// ResumeHookFn returns a resumable job based on the Type, or nil if the
// hook cannot serve the Type. The Resumer is created on the coordinator
// each time the job is started/resumed, so it can hold state. The Resume
// method is always ran, and can set state on the Resumer that can be used
// by the other methods.
type ResumeHookFn func(Type, *cluster.Settings) Resumer

var resumeHooks []ResumeHookFn

func getResumeHook(typ Type, settings *cluster.Settings) (Resumer, error) {
	for _, hook := range resumeHooks {
		if resumer := hook(typ, settings); resumer != nil {
			return resumer, nil
		}
	}
	return nil, errors.Errorf("no resumer are available for %s", typ)
}

// resume starts or resumes a job. If no error is returned then the job was
// asynchronously executed. The job is executed with the ctx, so ctx must
// only by canceled if the job should also be canceled. resultsCh is passed
// to the resumable func and should be closed by the caller after errCh sends
// a value. errCh returns an error if the job was not completed with success.
func (r *Registry) resume(
	ctx context.Context, resumer Resumer, resultsCh chan<- tree.Datums, job *Job,
) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		resumeErr := resumer.Resume(ctx, job, resultsCh)
		terminal := true
		var status Status
		defer r.unregister(*job.id)
		if err, ok := errors.Cause(resumeErr).(*InvalidStatusError); ok &&
			(err.status == StatusPaused || err.status == StatusCanceled) {
			if err.status == StatusPaused {
				terminal = false
			}
			// If we couldn't operate on the job because it was paused or canceled, send
			// the more understandable "job paused" or "job canceled" error message to
			// the user.
			resumeErr = errors.Errorf("job %s", err.status)
			status = err.status
		} else {
			// Attempt to mark the job as succeeded.
			if resumeErr == nil {
				status = StatusSucceeded
				if err := job.Succeeded(ctx, resumer.OnSuccess); err != nil {
					// If it didn't succeed, mark it failed.
					resumeErr = errors.Wrapf(err, "could not mark job %d as succeeded", *job.id)
				}
			}
			if resumeErr != nil {
				status = StatusFailed
				if err := job.Failed(ctx, resumeErr, resumer.OnFailOrCancel); err != nil {
					// If we can't transactionally mark the job as failed then it will be
					// restarted during the next adopt loop and retried.
					resumeErr = errors.Wrapf(err, "could not mark job %d as failed: %s", *job.id, resumeErr)
					terminal = false
				}
			}
		}

		if terminal {
			resumer.OnTerminal(ctx, job, status, resultsCh)
		}
		errCh <- resumeErr
	}()
	return errCh
}

// AddResumeHook adds a resume hook.
func AddResumeHook(fn ResumeHookFn) {
	resumeHooks = append(resumeHooks, fn)
}

func (r *Registry) maybeAdoptJob(ctx context.Context, nl nodeLiveness) error {
	var rows []tree.Datums
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		const stmt = `SELECT id, payload FROM system.public.jobs WHERE status IN ($1, $2) ORDER BY created DESC`
		rows, _ /* cols */, err = r.ex.QueryRowsInTransaction(
			ctx, "adopt-job", txn, stmt, StatusPending, StatusRunning)
		return err
	}); err != nil {
		return err
	}

	type nodeStatus struct {
		isLive bool
		epoch  int64
	}
	nodeStatusMap := map[roachpb.NodeID]*nodeStatus{
		// 0 is not a valid node ID, but we treat it as an always-dead node so that
		// the empty lease (Lease{}) is always considered expired.
		0: {isLive: false},
	}
	{
		now, maxOffset := r.clock.Now(), r.clock.MaxOffset()
		for _, liveness := range nl.GetLivenesses() {
			nodeStatusMap[liveness.NodeID] = &nodeStatus{
				isLive: liveness.IsLive(now, maxOffset),
				epoch:  liveness.Epoch,
			}
		}
	}

	for _, row := range rows {
		id := (*int64)(row[0].(*tree.DInt))
		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		if log.V(2) {
			log.Infof(ctx, "evaluating job %d with lease %#v", *id, payload.Lease)
		}

		if payload.Lease == nil {
			// If the lease is missing, it simply means the job does not yet support
			// resumability.
			if log.V(2) {
				log.Infof(ctx, "job %d: skipping: nil lease", *id)
			}
			continue
		}

		r.mu.Lock()
		_, running := r.mu.jobs[*id]
		r.mu.Unlock()

		var needsResume bool
		if payload.Lease.NodeID == r.nodeID.Get() {
			// If we hold the lease for a job, check to see if we're actually running
			// that job, and resume it if we're not. Otherwise, the job will be stuck
			// until this node is restarted, as the other nodes in the cluster see
			// that we hold a valid lease and assume we're running the job.
			//
			// We end up in this state—a valid lease for a canceled job—when we
			// overcautiously cancel all jobs due to e.g. a slow heartbeat response.
			// If that heartbeat managed to successfully extend the liveness lease,
			// we'll have stopped running jobs on which we still had valid leases.
			needsResume = !running
		} else {
			// If we are currently running a job that another node has the lease on,
			// stop running it.
			if running {
				log.Warningf(ctx, "job %d: node %d owns lease; canceling", *id, payload.Lease.NodeID)
				r.unregister(*id)
				continue
			}
			nodeStatus, ok := nodeStatusMap[payload.Lease.NodeID]
			if !ok {
				log.Warningf(ctx, "job %d: skipping: no liveness record for node %d",
					*id, payload.Lease.NodeID)
				continue
			}
			needsResume = nodeStatus.epoch > payload.Lease.Epoch || !nodeStatus.isLive
		}

		if !needsResume {
			continue
		}

		job := Job{id: id, registry: r}
		resumeCtx, cancel := r.makeCtx()
		if err := job.adopt(ctx, payload.Lease); err != nil {
			return errors.Wrap(err, "unable to acquire lease")
		}
		r.register(*id, cancel)

		resultsCh := make(chan tree.Datums)
		resumer, err := getResumeHook(payload.Type(), r.settings)
		if err != nil {
			return err
		}
		errCh := r.resume(resumeCtx, resumer, resultsCh, &job)
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

		// Only adopt one job per turn to allow other nodes their fair share.
		break
	}

	return nil
}

func (r *Registry) newLease() *Lease {
	nodeID := r.nodeID.Get()
	if nodeID == 0 {
		panic("jobs.Registry has empty node ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &Lease{NodeID: nodeID, Epoch: r.mu.epoch}
}

func (r *Registry) cancelAll(ctx context.Context) {
	r.mu.AssertHeld()
	for jobID, cancel := range r.mu.jobs {
		log.Warningf(ctx, "job %d: canceling due to liveness failure", jobID)
		cancel()
	}
	r.mu.jobs = make(map[int64]context.CancelFunc)
}

func (r *Registry) register(jobID int64, cancel func()) {
	r.mu.Lock()
	r.mu.jobs[jobID] = cancel
	r.mu.Unlock()
}

func (r *Registry) unregister(jobID int64) {
	r.mu.Lock()
	cancel, ok := r.mu.jobs[jobID]
	// It is possible for a job to be double unregistered. unregister is always
	// called at the end of resume. But it can also be called during cancelAll
	// and in the adopt loop under certain circumstances.
	if ok {
		cancel()
		delete(r.mu.jobs, jobID)
	}
	r.mu.Unlock()
}
