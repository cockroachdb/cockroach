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
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const flowDoneChanSize = 8

// NB: Once upon a time there was a mechanism to queue flows above this
// max_running_flows which had a positive consequence of handling bursts of
// flows gracefully. Unfortunately it had the negative consequence that when
// queuing was occurring it would ultimately lead to connection timeout on the
// inbound side of the connection. This led to hard to interpret error messages.
// The decision was made that in lieu of a more complex mechanism we'd prefer to
// fail fast with a clear error message which is easy to triage. The removal of
// the queue however leaves open the likelihood of hitting an error case
// in workloads which before may have experienced some queueing but generally
// worked well. Removing the queuing exacerbates the importance of choosing an
// appropriate value for this setting. Unfortunately there do not exist
// sophisticated mechanisms to detect an appropriate value based on resource
// constraints. The intuition for now is to choose a value we expect to be
// conservative and educate customers who both hit this error and have a large
// amount of RAM headroom to increase it until either they are not hitting the
// limit or they begin to be squeezed on RAM at which point they likely need to
// scale out their cluster.

// TODO(ajwerner): devise more robust overload / resource allocation mechanisms.
// TODO(ajwerner): In lieu of above, justify the default max_running_flows.

var settingMaxRunningFlows = settings.RegisterIntSetting(
	"sql.distsql.max_running_flows",
	"maximum number of concurrent flows that can be run on a node",
	1000,
)

// flowScheduler manages running flows and decides whether to start flows.
// The main interface it presents is ScheduleFlows, which passes a flow to be
// run.
type flowScheduler struct {
	log.AmbientContext
	stopper    *stop.Stopper
	flowDoneCh chan *Flow
	metrics    *DistSQLMetrics

	mu struct {
		syncutil.Mutex
		numRunning      int
		maxRunningFlows int
	}
}

func newFlowScheduler(
	ambient log.AmbientContext,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	metrics *DistSQLMetrics,
) *flowScheduler {
	fs := &flowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan *Flow, flowDoneChanSize),
		metrics:        metrics,
	}
	fs.mu.maxRunningFlows = int(settingMaxRunningFlows.Get(&settings.SV))
	settingMaxRunningFlows.SetOnChange(&settings.SV, func() {
		fs.mu.Lock()
		fs.mu.maxRunningFlows = int(settingMaxRunningFlows.Get(&settings.SV))
		fs.mu.Unlock()
	})
	return fs
}

func (fs *flowScheduler) canRunFlow(_ *Flow) bool {
	// TODO(radu): we will have more complex resource accounting (like memory).
	// For now we just limit the number of concurrent flows.
	return fs.mu.numRunning < fs.mu.maxRunningFlows
}

// runFlowNow starts the given flow; does not wait for the flow to complete.
func (fs *flowScheduler) runFlowNow(ctx context.Context, f *Flow) error {
	log.VEventf(
		ctx, 1, "flow scheduler running flow %s, currently running %d", f.id, fs.mu.numRunning,
	)
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

var errTooManyFlows = pgerror.NewError(pgerror.CodeInsufficientResourcesError,
	"max_running_flows exceeded")

// ScheduleFlow is the main interface of the flow scheduler. It runs the given
// flow or rejects it with errTooManyFlows. Errors encountered when starting the
// flow are returned.
func (fs *flowScheduler) ScheduleFlow(ctx context.Context, f *Flow) error {
	return fs.stopper.RunTaskWithErr(
		ctx, "distsqlrun.flowScheduler: scheduling flow", func(ctx context.Context) error {
			fs.mu.Lock()
			defer fs.mu.Unlock()
			if fs.canRunFlow(f) {
				return fs.runFlowNow(ctx, f)
			}
			fs.metrics.FlowsRejected.Inc(1)
			return errTooManyFlows
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
				return
			}
			fs.mu.Unlock()
			select {
			case <-fs.flowDoneCh:
				fs.mu.Lock()
				fs.mu.numRunning--
				fs.metrics.FlowStop()

			case <-fs.stopper.ShouldStop():
				fs.mu.Lock()
				stopped = true
			}
		}
	})
}
