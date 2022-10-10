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
			// The flow can be started.
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
