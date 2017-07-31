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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package jobs

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
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
) *Registry {
	r := &Registry{clock: clock, db: db, ex: ex, gossip: gossip, nodeID: nodeID, clusterID: clusterID}
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
	j := &Job{
		id:       &jobID,
		registry: r,
	}
	if err := j.load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// DefaultCancelInterval is a reasonable interval at which to poll this node
// for liveness failures and cancel running jobs.
const DefaultCancelInterval = base.DefaultHeartbeatInterval

// DefaultAdoptInterval is a reasonable interval at which to poll system.jobs
// for jobs with expired leases. Mutable for testing.
var DefaultAdoptInterval = 30 * time.Second

// Start polls the current node for liveness failures and cancels all registered
// jobs if it observes a failure.
func (r *Registry) Start(
	stopper *stop.Stopper, nl nodeLiveness, cancelInterval, adoptInterval time.Duration,
) error {
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
	// TODO(benesch): cancel only jobs from previous epochs. This logic is correct
	// but too aggressive, as it might cancel jobs created immediately after a
	// liveness failure.
	sameEpoch := liveness.Epoch == r.mu.epoch
	if !sameEpoch || !liveness.IsLive(r.clock.Now(), r.clock.MaxOffset()) {
		r.cancelAll(ctx)
		r.mu.epoch = liveness.Epoch
	}
}

type resumeHookFn func(Type) func(context.Context, *Job) error

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
	nodeStatusMap := make(map[roachpb.NodeID]*nodeStatus)
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
			log.Infof(ctx, "evaluating job %d", *id)
		}

		if payload.Lease == nil {
			// If the lease is missing, it simply means the job does not yet support
			// resumability.
			continue
		}

		nodeStatus, ok := nodeStatusMap[payload.Lease.NodeID]
		if !ok {
			log.Warningf(ctx, "no liveness record for node %d", payload.Lease.NodeID)
			continue
		}

		if nodeStatus.epoch > payload.Lease.Epoch || !nodeStatus.isLive {
			var resumeFn func(context.Context, *Job) error
			for _, hook := range resumeHooks {
				if resumeFn = hook(payload.Type()); resumeFn != nil {
					break
				}
			}
			if resumeFn == nil {
				if log.V(2) {
					log.Infof(ctx, "skipping job %d as no resume functions are available", *id)
				}
				continue
			}

			job := Job{id: id, registry: r}
			if err := job.adopt(ctx, payload.Lease); err != nil {
				if log.V(2) {
					log.Infof(ctx, "unable to acquire lease on %d: %s", id, err)
				}
				continue
			}

			go func() {
				err := resumeFn(ctx, &job)
				if err := job.FinishedWith(ctx, err); err != nil {
					// Nowhere to report this error but the log.
					log.Errorf(ctx, "ignoring error while marking job %d finished with: %+v", *job.ID(), err)
				}
			}()

			// Only adopt one job per turn to allow other nodes their fair share.
			break
		}
	}

	return nil
}

func (r *Registry) cancelAll(ctx context.Context) {
	log.Warningf(ctx, "canceling all jobs due to liveness failure")
	r.mu.AssertHeld()
	for jobID, job := range r.mu.jobs {
		if log.V(2) {
			log.Warningf(ctx, "canceling job %d", jobID)
		}
		job.doCancel()
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

func (r *Registry) register(jobID int64, j *Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mu.jobs[jobID]; ok {
		return errors.Errorf("already tracking job ID %d", jobID)
	}
	r.mu.jobs[jobID] = j
	return nil
}

func (r *Registry) unregister(jobID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.jobs, jobID)
}
