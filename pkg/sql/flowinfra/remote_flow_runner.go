// Copyright 2022 The Cockroach Authors.
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
		runningFlows map[execinfrapb.FlowID]flowWithInfo
		acc          *mon.BoundAccount
	}
}

var _ stop.Closer = &RemoteFlowRunner{}

type flowWithInfo struct {
	flow      Flow
	timestamp time.Time
	// cleanupLocked must be called while holding the RemoteFlowRunner.mu and
	// after flow exits.
	cleanupLocked func()
}

const flowWithInfoOverhead = int64(unsafe.Sizeof(flowWithInfo{}))

// NewRemoteFlowRunner creates a new RemoteFlowRunner which must be initialized
// before use. The runner is added as the stop.Closer to the stopper.
func NewRemoteFlowRunner(
	ambient log.AmbientContext, stopper *stop.Stopper, acc *mon.BoundAccount,
) *RemoteFlowRunner {
	r := &RemoteFlowRunner{
		AmbientContext: ambient,
		stopper:        stopper,
	}
	r.mu.runningFlows = make(map[execinfrapb.FlowID]flowWithInfo)
	r.mu.acc = acc
	stopper.AddCloser(r)
	return r
}

// Init initializes the RemoteFlowRunner.
func (r *RemoteFlowRunner) Init(metrics *execinfra.DistSQLMetrics) {
	r.metrics = metrics
}

// RunFlow starts the given flow; does not wait for the flow to complete.
func (r *RemoteFlowRunner) RunFlow(ctx context.Context, f Flow) error {
	err := r.stopper.RunTaskWithErr(
		ctx, "flowinfra.RemoteFlowRunner: running flow", func(ctx context.Context) error {
			flowID := f.GetID()
			log.VEventf(ctx, 1, "flow runner running flow %s", flowID)
			// Add this flow into the runningFlows map after performing the
			// memory accounting.
			memUsage := memsize.MapEntryOverhead + flowWithInfoOverhead + f.MemUsage()
			var cleanupLocked func()
			if err := func() error {
				r.mu.Lock()
				defer r.mu.Unlock()
				if err := r.mu.acc.Grow(ctx, memUsage); err != nil {
					return err
				}
				cleanupLocked = func() {
					delete(r.mu.runningFlows, flowID)
					r.mu.acc.Shrink(ctx, memUsage)
					r.metrics.FlowStop()
					f.Cleanup(ctx)
				}
				r.mu.runningFlows[flowID] = flowWithInfo{
					flow:          f,
					timestamp:     timeutil.Now(),
					cleanupLocked: cleanupLocked,
				}
				return nil
			}(); err != nil {
				// The memory reservation was denied, so we exit after cleaning
				// up the flow.
				f.Cleanup(ctx)
				return err
			}
			// The flow can be started.
			r.metrics.FlowStart()
			cleanup := func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				if _, ok := r.mu.runningFlows[flowID]; ok {
					// If the flow is not found, then it has already been
					// cleaned up in RemoteFlowRunner.Close.
					cleanupLocked()
				}
			}
			if err := f.Start(ctx); err != nil {
				cleanup()
				return err
			}
			go func() {
				f.Wait()
				cleanup()
			}()
			return nil
		})
	if err != nil && errors.Is(err, stop.ErrUnavailable) {
		// If the server is quiescing, we have to explicitly clean up the flow.
		f.Cleanup(ctx)
	}
	return err
}

// Close implements the stop.Closer interface.
//
// This function takes the responsibility of cleaning up all the flows that are
// still running. Notably, no new flows will be added because the stopper is
// quiescing.
//
// The reason for having this function is that we want to ensure that all the
// flows are cleaned up before the temporary file system is closed. The FS is
// added as the Closer, and we ensure that the RemoteFlowRunner is added as an
// "earlier" Closer.
func (r *RemoteFlowRunner) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, info := range r.mu.runningFlows {
		info.flow.Wait()
		info.cleanupLocked()
	}
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
