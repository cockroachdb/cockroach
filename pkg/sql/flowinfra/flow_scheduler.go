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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const flowDoneChanSize = 8

var settingMaxRunningFlows = settings.RegisterIntSetting(
	"sql.distsql.max_running_flows",
	"maximum number of concurrent flows that can be run on a node",
	500,
).WithPublic()

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
		// queue keeps track of all scheduled flows that cannot be run at the
		// moment because the maximum number of running flows has been reached.
		queue *list.List
		// runningFlows keeps track of all flows that are currently running via
		// this FlowScheduler. The mapping is from flow ID to the timestamp when
		// the flow started running, in the UTC timezone.
		//
		// The memory usage of this map is not accounted for because it is
		// limited by maxRunningFlows in size.
		runningFlows map[execinfrapb.FlowID]time.Time
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

// NewFlowScheduler creates a new FlowScheduler which must be initialized before
// use.
func NewFlowScheduler(
	ambient log.AmbientContext, stopper *stop.Stopper, settings *cluster.Settings,
) *FlowScheduler {
	fs := &FlowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan Flow, flowDoneChanSize),
	}
	fs.mu.queue = list.New()
	maxRunningFlows := settingMaxRunningFlows.Get(&settings.SV)
	fs.mu.runningFlows = make(map[execinfrapb.FlowID]time.Time, maxRunningFlows)
	fs.atomics.maxRunningFlows = int32(maxRunningFlows)
	settingMaxRunningFlows.SetOnChange(&settings.SV, func(ctx context.Context) {
		atomic.StoreInt32(&fs.atomics.maxRunningFlows, int32(settingMaxRunningFlows.Get(&settings.SV)))
	})
	return fs
}

// Init initializes the FlowScheduler.
func (fs *FlowScheduler) Init(metrics *execinfra.DistSQLMetrics) {
	fs.metrics = metrics
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
	fs.mu.runningFlows[f.GetID()] = timeutil.Now()
	if !locked {
		fs.mu.Unlock()
	}
	if err := f.Start(ctx, func() { fs.flowDoneCh <- f }); err != nil {
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
// the given flow.
//
// If the flow can start immediately, errors encountered when starting the flow
// are returned. If the flow is enqueued, these error will be later ignored.
func (fs *FlowScheduler) ScheduleFlow(ctx context.Context, f Flow) error {
	return fs.stopper.RunTaskWithErr(
		ctx, "flowinfra.FlowScheduler: scheduling flow", func(ctx context.Context) error {
			if fs.canRunFlow(f) {
				return fs.runFlowNow(ctx, f, false /* locked */)
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
		}
	}
}

// Start launches the main loop of the scheduler.
func (fs *FlowScheduler) Start() {
	ctx := fs.AnnotateCtx(context.Background())
	// TODO(radu): we may end up with a few flows in the queue that will
	// never be processed. Is that an issue?
	_ = fs.stopper.RunAsyncTask(ctx, "flow-scheduler", func(context.Context) {
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

			case <-fs.stopper.ShouldQuiesce():
				fs.mu.Lock()
				stopped = true
			}
		}
	})
}

// Serialize returns all currently running and queued flows that were scheduled
// on behalf of other nodes. Notably the returned slices don't contain the
// "local" flows from the perspective of the gateway node of the query because
// such flows don't go through the flow scheduler.
func (fs *FlowScheduler) Serialize() (
	running []execinfrapb.FlowID,
	runningSince []time.Time,
	queued []execinfrapb.FlowID,
	queuedSince []time.Time,
) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	running = make([]execinfrapb.FlowID, 0, len(fs.mu.runningFlows))
	runningSince = make([]time.Time, 0, len(fs.mu.runningFlows))
	for f, ts := range fs.mu.runningFlows {
		running = append(running, f)
		runningSince = append(runningSince, ts)
	}
	if fs.mu.queue.Len() > 0 {
		queued = make([]execinfrapb.FlowID, 0, fs.mu.queue.Len())
		queuedSince = make([]time.Time, 0, fs.mu.queue.Len())
		for e := fs.mu.queue.Front(); e != nil; e = e.Next() {
			f := e.Value.(*flowWithCtx)
			queued = append(queued, f.flow.GetID())
			queuedSince = append(queuedSince, f.enqueueTime)
		}
	}
	return running, runningSince, queued, queuedSince
}
