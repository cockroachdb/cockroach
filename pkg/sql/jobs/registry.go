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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// nodeLiveness is the subset of storage.NodeLiveness's interface needed
// by Registry.
type nodeLiveness interface {
	Self() (*storage.Liveness, error)
	GetLivenesses() []storage.Liveness
}

// Registry creates Jobs and manages their leases and cancelation.
type Registry struct {
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
		jobs  map[int64]*Job
	}
}

// MakeRegistry creates a new Registry.
func MakeRegistry(
	clock *hlc.Clock,
	db *client.DB,
	ex sqlutil.InternalExecutor,
	gossip *gossip.Gossip,
	nodeID *base.NodeIDContainer,
	clusterID func() uuid.UUID,
	settings *cluster.Settings,
) *Registry {
	r := &Registry{
		clock:     clock,
		db:        db,
		ex:        ex,
		gossip:    gossip,
		nodeID:    nodeID,
		clusterID: clusterID,
		settings:  settings,
	}
	r.mu.epoch = 1
	r.mu.jobs = make(map[int64]*Job)
	return r
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
		log.Warningf(ctx, "unable to get node liveness: %s", err)
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

type resumeHookFn func(Type, *cluster.Settings) func(context.Context, *Job) error

var resumeHooks []resumeHookFn

// AddResumeHook adds a resume hook.
func AddResumeHook(fn resumeHookFn) {
	resumeHooks = append(resumeHooks, fn)
}

func (r *Registry) maybeAdoptJob(ctx context.Context, nl nodeLiveness) error {
	var rows []parser.Datums
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		const stmt = `SELECT id, payload FROM system.jobs WHERE status IN ($1, $2) ORDER BY created DESC`
		rows, err = r.ex.QueryRowsInTransaction(ctx, "adopt-job", txn, stmt, StatusPending, StatusRunning)
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
		id := (*int64)(row[0].(*parser.DInt))
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
			r.mu.Lock()
			_, running := r.mu.jobs[*id]
			r.mu.Unlock()
			needsResume = !running
		} else {
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

		var resumeFn func(context.Context, *Job) error
		for _, hook := range resumeHooks {
			if resumeFn = hook(payload.Type(), r.settings); resumeFn != nil {
				break
			}
		}
		if resumeFn == nil {
			if log.V(2) {
				log.Infof(ctx, "job %d: skipping: no resume functions are available", *id)
			}
			continue
		}

		job := Job{id: id, registry: r}
		if err := job.adopt(ctx, payload.Lease); err != nil {
			if log.V(2) {
				log.Infof(ctx, "skipping job %d: unable to acquire lease: %s", *id, err)
			}
			continue
		}

		go func() {
			log.Infof(ctx, "job %d: resuming", *id)
			err := resumeFn(ctx, &job)
			if _, isDuplicate := errors.Cause(err).(*duplicateRegistrationError); isDuplicate {
				// Another turn of the adoption loop already resumed this job. Swallow
				// the error, as the job is properly resumed.
				//
				// This happens because job registration is asynchronous. This
				// goroutine, not the adoption loop's goroutine, is responsible for
				// calling Registry.register. There's a window where resumeFn has not
				// yet registered the job with the registry, so the next turn of the
				// adoption loop will see it holds the lease on a job that's not
				// running, and attempt to resume it again. This likely never happens in
				// practice because DefaultAdoptInterval is several orders of magnitude
				// larger than the delay between resuming a job and that job registering
				// itself. In tests, though, double resumption is a real possibility, as
				// the DefaultAdoptInterval gets turned down an order of magnitude.
				//
				// TODO(benesch): make the adoption loop synchronously register the jobs
				// it resumes. This requires API changes to "invert control"; see the
				// TODOs in jobs.go for details.
			} else if err := job.FinishedWith(ctx, err); err != nil {
				// Nowhere to report this error but the log.
				log.Errorf(ctx, "job %d: ignoring FinishedWith error: %+v", *id, err)
			}
		}()

		// Only adopt one job per turn to allow other nodes their fair share.
		break
	}

	return nil
}

func (r *Registry) cancelAll(ctx context.Context) {
	r.mu.AssertHeld()
	for jobID, job := range r.mu.jobs {
		log.Warningf(ctx, "job %d: canceling due to liveness failure", jobID)
		job.cancel()
	}
	r.mu.jobs = make(map[int64]*Job)
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

type duplicateRegistrationError struct {
	jobID int64
}

func (e *duplicateRegistrationError) Error() string {
	return fmt.Sprintf("job %d is already registered", e.jobID)
}

func (r *Registry) register(jobID int64, j *Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mu.jobs[jobID]; ok {
		return &duplicateRegistrationError{jobID: jobID}
	}
	r.mu.jobs[jobID] = j
	return nil
}

func (r *Registry) unregister(jobID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.jobs, jobID)
}
