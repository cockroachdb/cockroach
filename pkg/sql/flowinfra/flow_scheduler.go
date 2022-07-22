// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// FlowScheduler manages running flows and decides when to queue and when to
// start flows. The main interface it presents is ScheduleFlow which starts a
// flow and lets it run asynchronously.
// TODO: comment.
type FlowScheduler struct {
	log.AmbientContext
	stopper *stop.Stopper
	metrics *execinfra.DistSQLMetrics

	mu struct {
		syncutil.Mutex
		// runningFlows keeps track of all flows that are currently running via
		// this FlowScheduler. The mapping is from flow ID to the timestamp when
		// the flow started running, in the UTC timezone.
		//
		// The memory usage of this map is not accounted for because it is
		// limited by maxRunningFlows in size.
		// TODO: memory accounting.
		runningFlows map[execinfrapb.FlowID]execinfrapb.DistSQLRemoteFlowInfo
	}
}

// NewFlowScheduler creates a new FlowScheduler which must be initialized before
// use.
func NewFlowScheduler(
	ambient log.AmbientContext, stopper *stop.Stopper, settings *cluster.Settings,
) *FlowScheduler {
	fs := &FlowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
	}
	fs.mu.runningFlows = make(map[execinfrapb.FlowID]execinfrapb.DistSQLRemoteFlowInfo)
	return fs
}

// Init initializes the FlowScheduler.
func (fs *FlowScheduler) Init(metrics *execinfra.DistSQLMetrics) {
	fs.metrics = metrics
}

// runFlow starts the given flow; does not wait for the flow to complete.
func (fs *FlowScheduler) runFlow(ctx context.Context, f Flow) error {
	log.VEventf(ctx, 1, "flow scheduler running flow %s", f.GetID())
	fs.metrics.FlowStart()
	fs.mu.Lock()
	fs.mu.runningFlows[f.GetID()] = execinfrapb.DistSQLRemoteFlowInfo{
		FlowID:       f.GetID(),
		Timestamp:    timeutil.Now(),
		StatementSQL: f.StatementSQL(),
	}
	fs.mu.Unlock()
	if err := f.Start(ctx); err != nil {
		f.Cleanup(ctx)
		return err
	}
	// TODO(radu): we could replace the WaitGroup with a structure that keeps a
	// refcount and automatically runs Cleanup() when the count reaches 0.
	go func() {
		f.Wait()
		fs.mu.Lock()
		delete(fs.mu.runningFlows, f.GetID())
		fs.mu.Unlock()
		f.Cleanup(ctx)
	}()
	return nil
}

// ScheduleFlow is the main interface of the flow scheduler: it runs or enqueues
// the given flow. If the flow is not enqueued, it is guaranteed to be cleaned
// up when this function returns.
//
// If the flow can start immediately, errors encountered when starting the flow
// are returned. If the flow is enqueued, these error will be later ignored.
// TODO: comment.
func (fs *FlowScheduler) ScheduleFlow(ctx context.Context, f Flow) error {
	err := fs.stopper.RunTaskWithErr(
		ctx, "flowinfra.FlowScheduler: running flow", func(ctx context.Context) error {
			return fs.runFlow(ctx, f)
		})
	if err != nil && errors.Is(err, stop.ErrUnavailable) {
		// If the server is quiescing, we have to explicitly clean up the flow.
		f.Cleanup(ctx)
	}
	return err
}

// NumRunningFlows returns the number of flows scheduled via fs that are
// currently running.
func (fs *FlowScheduler) NumRunningFlows() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.mu.runningFlows)
}

// Serialize returns all currently running flows that were scheduled on behalf
// of other nodes. Notably the returned slices don't contain the "local" flows
// from the perspective of the gateway node of the query because such flows
// don't go through the flow scheduler.
func (fs *FlowScheduler) Serialize() (flows []execinfrapb.DistSQLRemoteFlowInfo) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	flows = make([]execinfrapb.DistSQLRemoteFlowInfo, 0, len(fs.mu.runningFlows))
	for _, info := range fs.mu.runningFlows {
		flows = append(flows, info)
	}
	return flows
}
