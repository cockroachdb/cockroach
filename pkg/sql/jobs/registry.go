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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// Registry creates Jobs, and will soon manage their leases and cancelation.
type Registry struct {
	db     *client.DB
	ex     sqlutil.InternalExecutor
	clock  *hlc.Clock
	nodeID roachpb.NodeID

	mu struct {
		syncutil.Mutex
		epoch   int64
		cancels map[int64]func()
	}
}

// MakeRegistry creates a new Registry.
func MakeRegistry(clock *hlc.Clock, db *client.DB, ex sqlutil.InternalExecutor) *Registry {
	r := &Registry{clock: clock, db: db, ex: ex}
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

// WatchLiveness polls for liveness failures for the specified nodeID and
// cancels all registered jobs if it observes a failure.
func (r *Registry) WatchLiveness(
	stopper *stop.Stopper, nl nodeLiveness, heartbeatInterval time.Duration,
) error {
	liveness, err := nl.Self()
	if err != nil {
		return err
	}
	fmt.Printf("NIKHIL THE LIVENESS IS %+v\n", liveness)
	r.nodeID = liveness.NodeID
	r.mu.Lock()
	r.mu.epoch = liveness.Epoch
	r.mu.Unlock()

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-time.After(heartbeatInterval):
				liveness, err := nl.Self()
				if err != nil {
					log.Errorf(ctx, "jobs.Registry: unable to get node liveness: %s", err)
					// Conservatively assume our lease has expired. Abort all jobs.
					r.cancelAll(ctx)
				}

				r.mu.Lock()
				sameEpoch := liveness.Epoch == r.mu.epoch
				if !sameEpoch || !liveness.IsLive(r.clock.Now(), r.clock.MaxOffset()) {
					// Our lease has definitely expired. Abort all jobs.
					r.cancelAll(ctx)
					r.mu.epoch = liveness.Epoch
				}
				r.mu.Unlock()
			case <-stopper.ShouldStop():
				return
			}
		}
	})

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
	if r.nodeID == 0 {
		panic("jobs.Registry has empty node ID")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return &Lease{NodeID: r.nodeID, Epoch: r.mu.epoch}
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
}
