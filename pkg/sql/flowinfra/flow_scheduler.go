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
	"container/list"
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const flowDoneChanSize = 8

var settingMaxRunningFlows = settings.RegisterPublicIntSetting(
	"sql.distsql.max_running_flows",
	"maximum number of concurrent flows that can be run on a node",
	500,
)

// FlowScheduler manages running flows and decides when to queue and when to
// start flows. The main interface it presents is ScheduleFlows, which passes a
// flow to be run.
type FlowScheduler struct {
	log.AmbientContext
	stopper    *stop.Stopper
	flowDoneCh chan Flow
	metrics    *execinfra.DistSQLMetrics

	mu struct {
		syncutil.Mutex
		queue *list.List
	}

	atomics struct {
		numRunning      int32
		maxRunningFlows int32
	}
}

// flowWithCtx stores a flow to run and a context to run it with.
// TODO(asubiotto): Figure out if asynchronous flow execution can be rearranged
// to avoid the need to store the context.
type flowWithCtx struct {
	ctx         context.Context
	flow        Flow
	enqueueTime time.Time
}

// NewFlowScheduler creates a new FlowScheduler.
func NewFlowScheduler(
	ambient log.AmbientContext,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	metrics *execinfra.DistSQLMetrics,
) *FlowScheduler {
	fs := &FlowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan Flow, flowDoneChanSize),
		metrics:        metrics,
	}
	fs.mu.queue = list.New()
	fs.atomics.maxRunningFlows = int32(settingMaxRunningFlows.Get(&settings.SV))
	settingMaxRunningFlows.SetOnChange(&settings.SV, func() {
		atomic.StoreInt32(&fs.atomics.maxRunningFlows, int32(settingMaxRunningFlows.Get(&settings.SV)))
	})
	return fs
}

// canRunFlow returns whether the FlowScheduler can run the flow. If true is
// returned, numRunning is also incremented.
// TODO(radu): we will have more complex resource accounting (like memory).
//  For now we just limit the number of concurrent flows.
func (fs *FlowScheduler) canRunFlow(_ Flow) bool {
	// Optimistically increase numRunning to account for this new flow.
	newNumRunning := atomic.AddInt32(&fs.atomics.numRunning, 1)
	if newNumRunning <= atomic.LoadInt32(&fs.atomics.maxRunningFlows) {
		// Happy case. This flow did not bring us over the limit, so return that the
		// flow can be run and is accounted for in numRunning.
		return true
	}
	atomic.AddInt32(&fs.atomics.numRunning, -1)
	return false
}

// runFlowNow starts the given flow; does not wait for the flow to complete. The
// caller is responsible for incrementing numRunning.
func (fs *FlowScheduler) runFlowNow(ctx context.Context, f Flow) error {
	log.VEventf(
		ctx, 1, "flow scheduler running flow %s, currently running %d", f.GetID(), atomic.LoadInt32(&fs.atomics.numRunning)-1,
	)
	fs.metrics.FlowStart()
	if err := f.Start(ctx, func() { fs.flowDoneCh <- f }); err != nil {
		return err
	}
	// TODO(radu): we could replace the WaitGroup with a structure that keeps a
	// refcount and automatically runs Cleanup() when the count reaches 0.
	go func() {
		f.Wait()
		f.Cleanup(ctx)
	}()
	return nil
}

// ScheduleFlow is the main interface of the flow scheduler: it runs or enqueues
// the given flow.
//
// If the flow can start immediately, errors encountered when starting the flow
// are returned. If the flow is enqueued, these error will be later ignored.
func (fs *FlowScheduler) ScheduleFlow(ctx context.Context, f Flow) error {
	return fs.stopper.RunTaskWithErr(
		ctx, "flowinfra.FlowScheduler: scheduling flow", func(ctx context.Context) error {
			if fs.canRunFlow(f) {
				return fs.runFlowNow(ctx, f)
			}
			fs.mu.Lock()
			defer fs.mu.Unlock()
			log.VEventf(ctx, 1, "flow scheduler enqueuing flow %s to be run later", f.GetID())
			fs.metrics.FlowsQueued.Inc(1)
			fs.mu.queue.PushBack(&flowWithCtx{
				ctx:         ctx,
				flow:        f,
				enqueueTime: timeutil.Now(),
			})
			return nil

		})
}

// Start launches the main loop of the scheduler.
func (fs *FlowScheduler) Start() {
	ctx := fs.AnnotateCtx(context.Background())
	fs.stopper.RunWorker(ctx, func(context.Context) {
		stopped := false
		fs.mu.Lock()
		defer fs.mu.Unlock()

		for {
			if stopped && atomic.LoadInt32(&fs.atomics.numRunning) == 0 {
				// TODO(radu): somehow error out the flows that are still in the queue.
				return
			}
			fs.mu.Unlock()
			select {
			case <-fs.flowDoneCh:
				fs.mu.Lock()
				// Decrement numRunning lazily (i.e. only if there is no new flow to
				// run).
				decrementNumRunning := stopped
				fs.metrics.FlowStop()
				if !stopped {
					if frElem := fs.mu.queue.Front(); frElem != nil {
						n := frElem.Value.(*flowWithCtx)
						fs.mu.queue.Remove(frElem)
						wait := timeutil.Since(n.enqueueTime)
						log.VEventf(
							n.ctx, 1, "flow scheduler dequeued flow %s, spent %s in queue", n.flow.GetID(), wait,
						)
						fs.metrics.FlowsQueued.Dec(1)
						fs.metrics.QueueWaitHist.RecordValue(int64(wait))
						// Note: we use the flow's context instead of the worker
						// context, to ensure that logging etc is relative to the
						// specific flow.
						if err := fs.runFlowNow(n.ctx, n.flow); err != nil {
							log.Errorf(n.ctx, "error starting queued flow: %s", err)
						}
					} else {
						decrementNumRunning = true
					}
				}
				if decrementNumRunning {
					atomic.AddInt32(&fs.atomics.numRunning, -1)
				}

			case <-fs.stopper.ShouldStop():
				fs.mu.Lock()
				stopped = true
			}
		}
	})
}
