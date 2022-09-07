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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const flowDoneChanSize = 8

// We think that it makes sense to scale the default value for
// max_running_flows based on how beefy the machines are, so we make it a
// multiple of the number of available CPU cores.
//
// The choice of 128 as the default multiple is driven by the old default value
// of 500 and is such that if we have 4 CPUs, then we'll get the value of 512,
// pretty close to the old default.
// TODO(yuzefovich): we probably want to remove / disable this limit completely
// when we enable the admission control.
var settingMaxRunningFlows = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.distsql.max_running_flows",
	"the value - when positive - used as is, or the value - when negative - "+
		"multiplied by the number of CPUs on a node, to determine the "+
		"maximum number of concurrent remote flows that can be run on the node",
	-128,
).WithPublic()

// getMaxRunningFlows returns an absolute value that determines the maximum
// number of concurrent remote flows on this node.
func getMaxRunningFlows(settings *cluster.Settings) int64 {
	maxRunningFlows := settingMaxRunningFlows.Get(&settings.SV)
	if maxRunningFlows < 0 {
		// We use GOMAXPROCS instead of NumCPU because the former could be
		// adjusted based on cgroup limits (see cgroups.AdjustMaxProcs).
		return -maxRunningFlows * int64(runtime.GOMAXPROCS(0))
	}
	return maxRunningFlows
}

// FlowScheduler manages running flows and decides when to queue and when to
// start flows. The main interface it presents is ScheduleFlows, which passes a
// flow to be run.
type FlowScheduler struct {
	log.AmbientContext
	stopper    *stop.Stopper
	flowDoneCh chan Flow
	metrics    *execinfra.DistSQLMetrics
	sv         *settings.Values

	mu struct {
		syncutil.Mutex
		// queue keeps track of all scheduled flows that cannot be run at the
		// moment because the maximum number of running flows has been reached.
		queue *list.List
		// runningFlows keeps track of all flows that are currently running via
		// this FlowScheduler. The mapping is from flow ID to the timestamp when
		// the flow started running, in the UTC timezone.
		//
		// The memory usage of this map is not accounted for because it is
		// limited by maxRunningFlows in size.
		runningFlows map[execinfrapb.FlowID]execinfrapb.DistSQLRemoteFlowInfo
	}

	atomics struct {
		numRunning      int32
		maxRunningFlows int32
	}

	TestingKnobs struct {
		// CancelDeadFlowsCallback, if set, will be called at the end of every
		// CancelDeadFlows call with the number of flows that the call canceled.
		//
		// The callback must be concurrency-safe.
		CancelDeadFlowsCallback func(numCanceled int)
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

// cleanupBeforeRun cleans up the flow's resources in case this flow will never
// run.
func (f *flowWithCtx) cleanupBeforeRun() {
	// Note: passing f.ctx is important; that's the context that has the flow's
	// span in it, and that span needs Finish()ing.
	f.flow.Cleanup(f.ctx)
}

// NewFlowScheduler creates a new FlowScheduler which must be initialized before
// use.
func NewFlowScheduler(
	ambient log.AmbientContext, stopper *stop.Stopper, settings *cluster.Settings,
) *FlowScheduler {
	fs := &FlowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan Flow, flowDoneChanSize),
		sv:             &settings.SV,
	}
	fs.mu.queue = list.New()
	maxRunningFlows := getMaxRunningFlows(settings)
	fs.mu.runningFlows = make(map[execinfrapb.FlowID]execinfrapb.DistSQLRemoteFlowInfo, maxRunningFlows)
	fs.atomics.maxRunningFlows = int32(maxRunningFlows)
	settingMaxRunningFlows.SetOnChange(fs.sv, func(ctx context.Context) {
		atomic.StoreInt32(&fs.atomics.maxRunningFlows, int32(getMaxRunningFlows(settings)))
	})
	return fs
}

// Init initializes the FlowScheduler.
func (fs *FlowScheduler) Init(metrics *execinfra.DistSQLMetrics) {
	fs.metrics = metrics
}

var flowSchedulerQueueingEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.distsql.flow_scheduler_queueing.enabled",
	"determines whether the flow scheduler imposes the limit on the maximum "+
		"number of concurrent remote DistSQL flows that a single node can have "+
		"(the limit is determined by the sql.distsql.max_running_flows setting)",
	false,
)

// canRunFlow returns whether the FlowScheduler can run the flow. If true is
// returned, numRunning is also incremented.
// TODO(radu): we will have more complex resource accounting (like memory).
//
//	For now we just limit the number of concurrent flows.
func (fs *FlowScheduler) canRunFlow() bool {
	// Optimistically increase numRunning to account for this new flow.
	newNumRunning := atomic.AddInt32(&fs.atomics.numRunning, 1)
	if !flowSchedulerQueueingEnabled.Get(fs.sv) {
		// The queueing behavior of the flow scheduler is disabled, so we can
		// run this flow without checking against the maxRunningFlows counter).
		return true
	}
	if newNumRunning <= atomic.LoadInt32(&fs.atomics.maxRunningFlows) {
		// Happy case. This flow did not bring us over the limit, so return that the
		// flow can be run and is accounted for in numRunning.
		return true
	}
	atomic.AddInt32(&fs.atomics.numRunning, -1)
	return false
}

// runFlowNow starts the given flow; does not wait for the flow to complete. The
// caller is responsible for incrementing numRunning. locked indicates whether
// fs.mu is currently being held.
func (fs *FlowScheduler) runFlowNow(ctx context.Context, f Flow, locked bool) error {
	log.VEventf(
		ctx, 1, "flow scheduler running flow %s, currently running %d", f.GetID(), atomic.LoadInt32(&fs.atomics.numRunning)-1,
	)
	fs.metrics.FlowStart()
	if !locked {
		fs.mu.Lock()
	}
	fs.mu.runningFlows[f.GetID()] = execinfrapb.DistSQLRemoteFlowInfo{
		FlowID:       f.GetID(),
		Timestamp:    timeutil.Now(),
		StatementSQL: f.StatementSQL(),
	}
	if !locked {
		fs.mu.Unlock()
	}
	if err := f.Start(ctx, func() { fs.flowDoneCh <- f }); err != nil {
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
func (fs *FlowScheduler) ScheduleFlow(ctx context.Context, f Flow) error {
	err := fs.stopper.RunTaskWithErr(
		ctx, "flowinfra.FlowScheduler: scheduling flow", func(ctx context.Context) error {
			fs.metrics.FlowsScheduled.Inc(1)
			telemetry.Inc(sqltelemetry.DistSQLFlowsScheduled)
			if fs.canRunFlow() {
				return fs.runFlowNow(ctx, f, false /* locked */)
			}
			fs.mu.Lock()
			defer fs.mu.Unlock()
			log.VEventf(ctx, 1, "flow scheduler enqueuing flow %s to be run later", f.GetID())
			fs.metrics.FlowsQueued.Inc(1)
			telemetry.Inc(sqltelemetry.DistSQLFlowsQueued)
			fs.mu.queue.PushBack(&flowWithCtx{
				ctx:         ctx,
				flow:        f,
				enqueueTime: timeutil.Now(),
			})
			return nil

		})
	if err != nil && errors.Is(err, stop.ErrUnavailable) {
		// If the server is quiescing, we have to explicitly clean up the flow.
		f.Cleanup(ctx)
	}
	return err
}

// NumFlowsInQueue returns the number of flows currently in the queue to be
// scheduled.
func (fs *FlowScheduler) NumFlowsInQueue() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.mu.queue.Len()
}

// NumRunningFlows returns the number of flows scheduled via fs that are
// currently running.
func (fs *FlowScheduler) NumRunningFlows() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Note that we choose not to use fs.atomics.numRunning here because that
	// could be imprecise in an edge (when we optimistically increase that value
	// by 1 in canRunFlow only to decrement it later and NumRunningFlows is
	// called in between those two events).
	return len(fs.mu.runningFlows)
}

// CancelDeadFlows cancels all flows mentioned in the request that haven't been
// started yet (meaning they have been queued up).
func (fs *FlowScheduler) CancelDeadFlows(req *execinfrapb.CancelDeadFlowsRequest) {
	// Quick check whether the queue is empty. If it is, there is nothing to do.
	fs.mu.Lock()
	isEmpty := fs.mu.queue.Len() == 0
	fs.mu.Unlock()
	if isEmpty {
		return
	}

	ctx := fs.AnnotateCtx(context.Background())
	log.VEventf(ctx, 1, "flow scheduler will attempt to cancel %d dead flows", len(req.FlowIDs))
	// We'll be holding the lock over the queue, so we'll speed up the process
	// of looking up whether a particular queued flow needs to be canceled by
	// building a map of those that do. This map shouldn't grow larger than
	// thousands of UUIDs in size, so it is ok to not account for the memory
	// under it.
	toCancel := make(map[uuid.UUID]struct{}, len(req.FlowIDs))
	for _, f := range req.FlowIDs {
		toCancel[f.UUID] = struct{}{}
	}
	numCanceled := 0
	defer func() {
		log.VEventf(ctx, 1, "flow scheduler canceled %d dead flows", numCanceled)
		if fs.TestingKnobs.CancelDeadFlowsCallback != nil {
			fs.TestingKnobs.CancelDeadFlowsCallback(numCanceled)
		}
	}()

	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Iterate over the whole queue and remove the dead flows.
	var next *list.Element
	for e := fs.mu.queue.Front(); e != nil; e = next {
		// We need to call Next() before Remove() below because the latter
		// zeroes out the links between elements.
		next = e.Next()
		f := e.Value.(*flowWithCtx)
		if _, shouldCancel := toCancel[f.flow.GetID().UUID]; shouldCancel {
			fs.mu.queue.Remove(e)
			fs.metrics.FlowsQueued.Dec(1)
			numCanceled++
			f.cleanupBeforeRun()
		}
	}
}

// Start launches the main loop of the scheduler.
func (fs *FlowScheduler) Start() {
	ctx := fs.AnnotateCtx(context.Background())
	_ = fs.stopper.RunAsyncTask(ctx, "flow-scheduler", func(context.Context) {
		stopped := false
		fs.mu.Lock()
		defer fs.mu.Unlock()

		quiesceCh := fs.stopper.ShouldQuiesce()

		for {
			if stopped {
				// Drain the queue.
				if l := fs.mu.queue.Len(); l > 0 {
					log.Infof(ctx, "abandoning %d flows that will never run", l)
				}
				for {
					e := fs.mu.queue.Front()
					if e == nil {
						break
					}
					fs.mu.queue.Remove(e)
					n := e.Value.(*flowWithCtx)
					// TODO(radu): somehow send an error to whoever is waiting on this flow.
					n.cleanupBeforeRun()
				}

				if atomic.LoadInt32(&fs.atomics.numRunning) == 0 {
					return
				}
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
						if err := fs.runFlowNow(n.ctx, n.flow, true /* locked */); err != nil {
							log.Errorf(n.ctx, "error starting queued flow: %s", err)
						}
					} else {
						decrementNumRunning = true
					}
				}
				if decrementNumRunning {
					atomic.AddInt32(&fs.atomics.numRunning, -1)
				}

			case <-quiesceCh:
				fs.mu.Lock()
				stopped = true
				if l := atomic.LoadInt32(&fs.atomics.numRunning); l != 0 {
					log.Infof(ctx, "waiting for %d running flows", l)
				}
				// Inhibit this arm of the select so that we don't spin on it.
				quiesceCh = nil
			}
		}
	})
}

// Serialize returns all currently running and queued flows that were scheduled
// on behalf of other nodes. Notably the returned slices don't contain the
// "local" flows from the perspective of the gateway node of the query because
// such flows don't go through the flow scheduler.
func (fs *FlowScheduler) Serialize() (
	running []execinfrapb.DistSQLRemoteFlowInfo,
	queued []execinfrapb.DistSQLRemoteFlowInfo,
) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	running = make([]execinfrapb.DistSQLRemoteFlowInfo, 0, len(fs.mu.runningFlows))
	for _, info := range fs.mu.runningFlows {
		running = append(running, info)
	}
	if fs.mu.queue.Len() > 0 {
		queued = make([]execinfrapb.DistSQLRemoteFlowInfo, 0, fs.mu.queue.Len())
		for e := fs.mu.queue.Front(); e != nil; e = e.Next() {
			f := e.Value.(*flowWithCtx)
			queued = append(queued, execinfrapb.DistSQLRemoteFlowInfo{
				FlowID:       f.flow.GetID(),
				Timestamp:    f.enqueueTime,
				StatementSQL: f.flow.StatementSQL(),
			})
		}
	}
	return running, queued
}
