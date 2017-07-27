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
	"fmt"
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

// Registry creates Jobs, and will soon manage their leases and cancelation.
type Registry struct {
	db        *client.DB
	ex        sqlutil.InternalExecutor
	gossip    *gossip.Gossip
	clock     *hlc.Clock
	nodeID    *base.NodeIDContainer
	clusterID func() uuid.UUID

	mu struct {
		syncutil.Mutex
		epoch   int64
		cancels map[int64]func()
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
	r.mu.cancels = make(map[int64]func())
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
				r.maybeAdoptJob(ctx, nl)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

func (r *Registry) maybeCancelJobs(ctx context.Context, nl nodeLiveness) {
	liveness, err := nl.Self()
	fmt.Printf("NIKHIL SELF %v\n", liveness)
	if err != nil {
		log.Errorf(ctx, "jobs.Registry: unable to get node liveness: %s", err)
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
		log.Errorf(ctx, "jobs.Registry: unable to load running jobs: %+v", err)
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
			log.Errorf(ctx, "jobs.Registry: unable to unmarshal payload: %+v", err)
			return err
		}

		log.Infof(ctx, "jobs.Registry: evaluating job %d", *id)

		if payload.Lease == nil {
			// If the lease is missing, it simply means the job does not yet support
			// resumability.
			continue
		}

		nodeStatus, ok := nodeStatusMap[payload.Lease.NodeID]
		if !ok {
			log.Warningf(ctx, "jobs.Registry: no liveness record for node %d", payload.Lease.NodeID)
			continue
		}

		log.Infof(ctx, "jobs.Registry: lease %+v, nodeStatus %+v", payload.Lease, nodeStatus)

		if nodeStatus.epoch > payload.Lease.Epoch || !nodeStatus.isLive {
			var resumeFn func(context.Context, *Job) error
			for _, hook := range resumeHooks {
				if resumeFn = hook(payload.Type()); resumeFn != nil {
					break
				}
			}
			if resumeFn == nil {
				log.Infof(ctx, "jobs.Registry: skipping job %d as no resume functions are available", *id)
				continue
			}

			job := Job{id: id, registry: r}
			if err := job.adopt(ctx); err != nil {
				log.Warningf(ctx, "jobs.Registry: unable to acquire lease on %d: %s", id, err)
				continue
			}

			go func() {
				if err := resumeFn(ctx, &job); err != nil {
					job.Failed(ctx, err)
					return
				}
				if err := job.Succeeded(ctx); err != nil {
					log.Errorf(ctx, "ignoring error while marking job %d (%s) as successful: %+v",
						job.ID(), job.Record.Description, err)
				}
			}()

			// Only adopt one job per turn to allow other nodes their fair share.
			break
		}
	}

	return nil
}

func (r *Registry) cancelAll(ctx context.Context) {
	r.mu.AssertHeld()
	for jobID, cancel := range r.mu.cancels {
		log.Warningf(ctx, "jobs.Registry: canceling job %d", jobID)
		cancel()
	}
	r.mu.cancels = make(map[int64]func())
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

func (r *Registry) register(jobID int64, cancel func()) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mu.cancels[jobID]; ok {
		return errors.Errorf("jobs.Registry: already tracking job ID %d", jobID)
	}
	r.mu.cancels[jobID] = cancel
	return nil
}

func (r *Registry) unregister(jobID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.cancels, jobID)
}

type nodeLiveness interface {
	Self() (*storage.Liveness, error)
	GetLivenesses() []storage.Liveness
}
