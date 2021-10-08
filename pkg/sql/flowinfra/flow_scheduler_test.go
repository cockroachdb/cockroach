// Copyright 2020 The Cockroach Authors.
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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockFlow struct {
	flowID uuid.UUID
	// runCh is a chan that is closed when either Run or Start is called.
	runCh chan struct{}
	// doneCh is a chan that the flow blocks on when run through Start and Wait
	// or Run. Close this channel to unblock the flow.
	doneCh chan struct{}
	// doneCb is set when a caller calls Start and is executed at the end of the
	// Wait method.
	doneCb func()
	// waitCb is an optional callback set in the constructor of the flow that
	// will be executed in the end of the Wait method.
	waitCb func()
}

var _ Flow = &mockFlow{}

func newMockFlow(flowID uuid.UUID, waitCb func()) *mockFlow {
	return &mockFlow{
		flowID: flowID,
		runCh:  make(chan struct{}),
		doneCh: make(chan struct{}),
		waitCb: waitCb,
	}
}

func (m *mockFlow) Setup(
	_ context.Context, _ *execinfrapb.FlowSpec, _ FuseOpt,
) (context.Context, execinfra.OpChains, error) {
	panic("not implemented")
}

func (m *mockFlow) SetTxn(_ *kv.Txn) {
	panic("not implemented")
}

func (m *mockFlow) Start(_ context.Context, doneCb func()) error {
	close(m.runCh)
	m.doneCb = doneCb
	return nil
}

func (m *mockFlow) Run(_ context.Context, doneCb func()) {
	close(m.runCh)
	<-m.doneCh
	doneCb()
}

func (m *mockFlow) Wait() {
	<-m.doneCh
	m.doneCb()
	if m.waitCb != nil {
		m.waitCb()
	}
}

func (m *mockFlow) IsLocal() bool {
	panic("not implemented")
}

func (m *mockFlow) IsVectorized() bool {
	panic("not implemented")
}

func (m *mockFlow) GetFlowCtx() *execinfra.FlowCtx {
	panic("not implemented")
}

func (m *mockFlow) AddStartable(_ Startable) {
	panic("not implemented")
}

func (m *mockFlow) GetID() execinfrapb.FlowID {
	return execinfrapb.FlowID{UUID: m.flowID}
}

func (m *mockFlow) Cleanup(_ context.Context) {}

func (m *mockFlow) ConcurrentTxnUse() bool {
	return false
}

func TestFlowScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx      = context.Background()
		stopper  = stop.NewStopper()
		settings = cluster.MakeTestingClusterSettings()
		metrics  = execinfra.MakeDistSQLMetrics(base.DefaultHistogramWindowInterval())
	)
	defer stopper.Stop(ctx)

	scheduler := NewFlowScheduler(log.AmbientContext{}, stopper, settings)
	scheduler.Init(&metrics)
	scheduler.Start()
	getNumRunning := func() int {
		return int(atomic.LoadInt32(&scheduler.atomics.numRunning))
	}

	t.Run("scheduling flows", func(t *testing.T) {
		scheduler.atomics.maxRunningFlows = 1

		flow1 := newMockFlow(uuid.Nil, nil /* waitCb*/)
		require.NoError(t, scheduler.ScheduleFlow(ctx, flow1))
		require.Equal(t, 1, getNumRunning())

		flow2 := newMockFlow(uuid.Nil, nil /* waitCb*/)
		require.NoError(t, scheduler.ScheduleFlow(ctx, flow2))
		// numRunning should still be 1 because a maximum of 1 flow can run at a time
		// and flow1 has not finished yet.
		require.Equal(t, 1, getNumRunning())

		close(flow1.doneCh)
		// Now that flow1 has finished, flow2 should be run.
		<-flow2.runCh
		require.Equal(t, 1, getNumRunning())
		close(flow2.doneCh)
		testutils.SucceedsSoon(t, func() error {
			if getNumRunning() != 0 {
				return errors.New("expected numRunning to fall back to 0")
			}
			return nil
		})
	})

	t.Run("canceling dead flows", func(t *testing.T) {
		var numCompletedFlows int32
		waitCb := func() {
			atomic.AddInt32(&numCompletedFlows, 1)
		}

		rng, _ := randutil.NewPseudoRand()
		maxNumActiveFlows := rng.Intn(5) + 1
		scheduler.atomics.maxRunningFlows = int32(maxNumActiveFlows)
		numFlows := maxNumActiveFlows*(rng.Intn(3)+1) + rng.Intn(2)
		flows := make([]*mockFlow, numFlows)
		for i := range flows {
			flows[i] = newMockFlow(uuid.FastMakeV4(), waitCb)
		}

		// Schedule the flows in random order.
		flowIdxs := rng.Perm(numFlows)
		for _, idx := range flowIdxs {
			require.NoError(t, scheduler.ScheduleFlow(ctx, flows[idx]))
		}
		require.Equal(t, maxNumActiveFlows, getNumRunning())

		// Check that first maxNumActiveFlows are currently running.
		for _, idx := range flowIdxs[:maxNumActiveFlows] {
			<-flows[idx].runCh
		}

		// Cancel all other flows.
		req := &execinfrapb.CancelDeadFlowsRequest{}
		for _, idx := range flowIdxs[maxNumActiveFlows:] {
			req.FlowIDs = append(req.FlowIDs, flows[idx].GetID())
		}
		scheduler.CancelDeadFlows(req)

		// Finish all running flows.
		for _, idx := range flowIdxs[:maxNumActiveFlows] {
			close(flows[idx].doneCh)
		}

		// Check that all flows have finished and that the dead flows didn't
		// run.
		testutils.SucceedsSoon(t, func() error {
			if getNumRunning() != 0 {
				return errors.New("expected numRunning to fall back to 0")
			}
			if maxNumActiveFlows != int(atomic.LoadInt32(&numCompletedFlows)) {
				return errors.New("not all running flows have completed")
			}
			return nil
		})
	})

	t.Run("attempt to cancel non-existent dead flows", func(t *testing.T) {
		scheduler.atomics.maxRunningFlows = 0

		actualFlowID := uuid.FastMakeV4()
		flow := newMockFlow(actualFlowID, nil /* waitCb*/)
		require.NoError(t, scheduler.ScheduleFlow(ctx, flow))

		// Attempt to cancel a non-existent flow.
		req := &execinfrapb.CancelDeadFlowsRequest{
			FlowIDs: []execinfrapb.FlowID{{UUID: uuid.FastMakeV4()}},
		}
		scheduler.CancelDeadFlows(req)

		// Cancel the actual flow.
		req.FlowIDs[0].UUID = actualFlowID
		scheduler.CancelDeadFlows(req)
	})
}
