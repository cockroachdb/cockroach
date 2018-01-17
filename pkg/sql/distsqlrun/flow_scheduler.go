// Copyright 2016 The Cockroach Authors.
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

package distsqlrun

import (
	"container/list"
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const maxRunningFlows = 500
const flowDoneChanSize = 8

// flowScheduler manages running flows and decides when to queue and when to
// start flows. The main interface it presents is ScheduleFlows, which passes a
// flow to be run.
type flowScheduler struct {
	log.AmbientContext
	stopper    *stop.Stopper
	flowDoneCh chan *Flow
	metrics    *DistSQLMetrics

	mu struct {
		syncutil.Mutex
		numRunning int
		queue      *list.List
	}
}

// flowWithCtx stores a flow to run and a context to run it with.
// TODO(asubiotto): Figure out if asynchronous flow execution can be rearranged
// to avoid the need to store the context.
type flowWithCtx struct {
	ctx  context.Context
	flow *Flow
}

func newFlowScheduler(
	ambient log.AmbientContext, stopper *stop.Stopper, metrics *DistSQLMetrics,
) *flowScheduler {
	fs := &flowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan *Flow, flowDoneChanSize),
		metrics:        metrics,
	}
	fs.mu.queue = list.New()
	return fs
}

func (fs *flowScheduler) canRunFlow(_ *Flow) bool {
	// TODO(radu): we will have more complex resource accounting (like memory).
	// For now we just limit the number of concurrent flows.
	return fs.mu.numRunning < maxRunningFlows
}

// runFlowNow starts the given flow; does not wait for the flow to complete.
func (fs *flowScheduler) runFlowNow(ctx context.Context, f *Flow) error {
	fs.mu.numRunning++
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
func (fs *flowScheduler) ScheduleFlow(ctx context.Context, f *Flow) error {
	return fs.stopper.RunTaskWithErr(
		ctx, "distsqlrun.flowScheduler: scheduling flow", func(ctx context.Context) error {
			fs.mu.Lock()
			defer fs.mu.Unlock()

			if fs.canRunFlow(f) {
				return fs.runFlowNow(ctx, f)
			}
			fs.mu.queue.PushBack(&flowWithCtx{
				ctx:  ctx,
				flow: f,
			})
			return nil

		})
}

// Start launches the main loop of the scheduler.
func (fs *flowScheduler) Start() {
	ctx := fs.AnnotateCtx(context.Background())
	fs.stopper.RunWorker(ctx, func(context.Context) {
		stopped := false
		fs.mu.Lock()
		defer fs.mu.Unlock()

		for {
			if stopped && fs.mu.numRunning == 0 {
				// TODO(radu): somehow error out the flows that are still in the queue.
				return
			}
			fs.mu.Unlock()
			select {
			case <-fs.flowDoneCh:
				fs.mu.Lock()
				fs.mu.numRunning--
				fs.metrics.FlowStop()
				if !stopped {
					if frElem := fs.mu.queue.Front(); frElem != nil {
						n := frElem.Value.(*flowWithCtx)
						fs.mu.queue.Remove(frElem)
						// Note: we use the flow's context instead of the worker
						// context, to ensure that logging etc is relative to the
						// specific flow.
						if err := fs.runFlowNow(n.ctx, n.flow); err != nil {
							log.Errorf(n.ctx, "error starting queued flow: %s", err)
						}
					}
				}

			case <-fs.stopper.ShouldStop():
				fs.mu.Lock()
				stopped = true
			}
		}
	})
}
