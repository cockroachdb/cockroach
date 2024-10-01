// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// RemoteFlowRunner manages running flows that are created on behalf of other
// nodes.
type RemoteFlowRunner struct {
	log.AmbientContext
	stopper *stop.Stopper
	metrics *execinfra.DistSQLMetrics

	mu struct {
		syncutil.Mutex
		// runningFlows keeps track of all flows that are currently running via
		// this RemoteFlowRunner.
		runningFlows map[execinfrapb.FlowID]flowWithTimestamp
		acc          *mon.BoundAccount
	}
}

type flowWithTimestamp struct {
	flow      Flow
	timestamp time.Time
}

const flowWithTimestampOverhead = int64(unsafe.Sizeof(flowWithTimestamp{}))

// NewRemoteFlowRunner creates a new RemoteFlowRunner which must be initialized
// before use.
func NewRemoteFlowRunner(
	ambient log.AmbientContext, stopper *stop.Stopper, acc *mon.BoundAccount,
) *RemoteFlowRunner {
	r := &RemoteFlowRunner{
		AmbientContext: ambient,
		stopper:        stopper,
	}
	r.mu.runningFlows = make(map[execinfrapb.FlowID]flowWithTimestamp)
	r.mu.acc = acc
	return r
}

// Init initializes the RemoteFlowRunner.
func (r *RemoteFlowRunner) Init(metrics *execinfra.DistSQLMetrics) {
	r.metrics = metrics
}

// RunFlow starts the given flow; does not wait for the flow to complete.
func (r *RemoteFlowRunner) RunFlow(ctx context.Context, f Flow) error {
	// cleanedUp is only accessed from the current goroutine.
	cleanedUp := false
	err := r.stopper.RunTaskWithErr(
		ctx, "flowinfra.RemoteFlowRunner: running flow", func(ctx context.Context) error {
			log.VEventf(ctx, 1, "flow runner running flow %s", f.GetID())
			// Add this flow into the runningFlows map after performing the
			// memory accounting.
			memUsage := memsize.MapEntryOverhead + flowWithTimestampOverhead + f.MemUsage()
			if err := func() error {
				r.mu.Lock()
				defer r.mu.Unlock()
				if err := r.mu.acc.Grow(ctx, memUsage); err != nil {
					return err
				}
				r.mu.runningFlows[f.GetID()] = flowWithTimestamp{
					flow:      f,
					timestamp: timeutil.Now(),
				}
				return nil
			}(); err != nil {
				// The memory reservation was denied, so we exit after cleaning
				// up the flow.
				f.Cleanup(ctx)
				return err
			}
			r.metrics.FlowStart()
			cleanup := func() {
				func() {
					r.mu.Lock()
					defer r.mu.Unlock()
					delete(r.mu.runningFlows, f.GetID())
					r.mu.acc.Shrink(ctx, memUsage)
				}()
				r.metrics.FlowStop()
				f.Cleanup(ctx)
			}
			// First, make sure that we can spin up a new async task whose job
			// is to wait for the flow to finish and perform the cleanup.
			//
			// However, we need to make sure that this task blocks until the
			// flow is started. True value will be sent on waiterShouldExit if
			// we couldn't start the flow and the async task must exit right
			// away, without waiting; when the channel is closed, the flow has
			// been started successfully, and the async task proceeds to waiting
			// for its completion.
			waiterShouldExit := make(chan bool)
			if err := r.stopper.RunAsyncTask(ctx, "flowinfra.RemoteFlowRunner: waiting for flow to finish", func(ctx context.Context) {
				if shouldExit := <-waiterShouldExit; shouldExit {
					return
				}
				f.Wait()
				cleanup()
			}); err != nil {
				cleanup()
				cleanedUp = true
				return err
			}
			// Now, start the flow to run concurrently.
			if err := f.Start(ctx); err != nil {
				cleanup()
				cleanedUp = true
				waiterShouldExit <- true
				return err
			}
			close(waiterShouldExit)
			return nil
		})
	if err != nil && errors.Is(err, stop.ErrUnavailable) && !cleanedUp {
		// If the server is quiescing, we have to explicitly clean up the flow
		// if it hasn't been cleaned up yet.
		f.Cleanup(ctx)
	}
	return err
}

// NumRunningFlows returns the number of flows that were kicked off via this
// flow runner that are still running.
func (r *RemoteFlowRunner) NumRunningFlows() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.runningFlows)
}

// CancelDeadFlows cancels all flows specified in req that are currently
// running.
func (r *RemoteFlowRunner) CancelDeadFlows(
	ctx context.Context, req *execinfrapb.CancelDeadFlowsRequest,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.VEventf(
		ctx, 1, "remote flow runner will attempt to cancel %d dead flows, "+
			"%d flows are running", len(req.FlowIDs), len(r.mu.runningFlows),
	)
	canceled := 0
	for _, flowID := range req.FlowIDs {
		if flow, ok := r.mu.runningFlows[flowID]; ok {
			flow.flow.Cancel()
			canceled++
		}
	}
	log.VEventf(ctx, 1, "remote flow runner canceled %d dead flows", canceled)
}

// Serialize returns all currently running flows that were kicked off on behalf
// of other nodes. Notably the returned slice doesn't contain the "local" flows
// from the perspective of the gateway node of the query because such flows
// don't go through the remote flow runner.
func (r *RemoteFlowRunner) Serialize() (flows []execinfrapb.DistSQLRemoteFlowInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	flows = make([]execinfrapb.DistSQLRemoteFlowInfo, 0, len(r.mu.runningFlows))
	for _, info := range r.mu.runningFlows {
		flows = append(flows, execinfrapb.DistSQLRemoteFlowInfo{
			FlowID:       info.flow.GetID(),
			Timestamp:    info.timestamp,
			StatementSQL: info.flow.StatementSQL(),
		})
	}
	return flows
}
