// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// decommissionMonitor is responsible for telling other nodes in the system to
// proactively enqueue the decommissioning node's ranges into their
// replicateQueues for decommissioning.
type decommissionMonitor struct {
	log.AmbientContext
	adminServer   *adminServer
	stopper       *stop.Stopper
	stores        *kvserver.Stores
	nudgeInterval time.Duration

	// sem is used to ensure that only one decommission monitor task is ever
	// running.
	sem    *quotapool.IntPool
	logger log.EveryN
}

func newDecommissionMonitor(
	ambient log.AmbientContext, server *adminServer, stopper *stop.Stopper, stores *kvserver.Stores,
) *decommissionMonitor {
	sem := quotapool.NewIntPool("decommission monitor", 1 /* capacity */)
	dm := &decommissionMonitor{
		AmbientContext: ambient,
		adminServer:    server,
		stopper:        stopper,
		stores:         stores,
		// TODO(aayush): This nudge interval should be configurable. We set a very
		// high default for now, effectively disabling the nudging.
		nudgeInterval: 1e6 * time.Minute,
		sem:           sem,
		logger:        log.Every(1 * time.Second),
	}
	dm.AddLogTag("decom-monitor", nil)

	return dm
}

// start starts the decommission monitor async task that proactively enqueues
// this node's ranges into their leaseholders' `replicateQueue`s. start is
// idempotent in that subsequent calls to start don't spin up more async tasks.
func (m *decommissionMonitor) start() error {
	ctx := m.AnnotateCtx(context.Background())

	// Ensure that only one decommission monitor task is ever started.
	alloc, err := m.sem.TryAcquire(ctx, 1)
	if err != nil {
		return err
	}
	defer m.sem.Release(alloc)

	log.Infof(ctx, "starting decommission monitor")
	err = m.stopper.RunAsyncTask(ctx, "decommission-monitor", func(ctx context.Context) {
		m.enqueueAllReplicas(ctx)

		for {
			select {
			case <-time.After(m.nudgeInterval):
				// TODO(aayush): This is a placeholder. We probably want to do something
				// more verbose (i.e. get a trace) and synchronous when we hit our nudge
				// interval.
				m.enqueueAllReplicas(ctx)
			case <-m.stopper.ShouldQuiesce():
				return
			}
		}
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *decommissionMonitor) enqueueAllReplicas(ctx context.Context) {
	if err := m.stores.VisitStores(func(s *kvserver.Store) error {
		// For each range on this node, we try to enqueue all its replicas into
		// their respective stores' replicateQueues. However, only the leaseholder
		// will actually end up processing the range, and the rest of the stores
		// will simply ignore this `EnqueueRangeRequest`.
		//
		// TODO(aayush): This races with the gossip propagation of this node's
		// DECOMMISSIONING status, so it is possible that the ranges enqueued
		// below aren't immediately acted upon by the replicateQueues of other
		// nodes. Consider introducing a new AdminServer / StatusServer RPC that
		// let's us ask a node for its (gossip-driven) understanding of a node's
		// decommissioning status. We could then wait for the gossip update to
		// propagate fully before we fire these EnqueueRangeRequests off.
		s.VisitReplicas(func(r *kvserver.Replica) bool {
			for _, repl := range r.Desc().Replicas().Descriptors() {
				if _, err := m.adminServer.EnqueueRange(ctx, &serverpb.EnqueueRangeRequest{
					NodeID:  repl.NodeID,
					Queue:   "replicate",
					RangeID: r.RangeID,
					Async:   true,
				}); err != nil {
					if m.logger.ShouldLog() {
						log.Errorf(
							ctx, "failed to enqueue decommissioning replica for r%d: %s", r.RangeID, err,
						)
					}
					return true
				}
			}
			return true
		})
		return nil
	}); err != nil {
		// We're swallowing any errors above, so we shouldn't see any error
		// bubbled up to here.
		panic(err)
	}
}
