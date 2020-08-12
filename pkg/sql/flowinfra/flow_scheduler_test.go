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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockFlow struct {
	// runCh is a chan that is closed when either Run or Start is called.
	runCh chan struct{}
	// doneCh is a chan that the flow blocks on when run through Start and Wait
	// or Run. Close this channel to unblock the flow.
	doneCh chan struct{}
	// doneCb is set when a caller calls Start and is executed at the end of the
	// Wait method.
	doneCb func()
}

var _ Flow = &mockFlow{}

func newMockFlow() *mockFlow {
	return &mockFlow{runCh: make(chan struct{}), doneCh: make(chan struct{})}
}

func (m *mockFlow) Setup(
	_ context.Context, _ *execinfrapb.FlowSpec, _ FuseOpt,
) (context.Context, error) {
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

func (m *mockFlow) Run(_ context.Context, doneCb func()) error {
	close(m.runCh)
	<-m.doneCh
	doneCb()
	return nil
}

func (m *mockFlow) Wait() {
	<-m.doneCh
	m.doneCb()
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
	return execinfrapb.FlowID{UUID: uuid.Nil}
}

func (m *mockFlow) Cleanup(_ context.Context) {}

func (m *mockFlow) ConcurrentTxnUse() bool {
	return false
}

func TestFlowScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		ctx      = context.Background()
		stopper  = stop.NewStopper()
		settings = cluster.MakeTestingClusterSettings()
		metrics  = execinfra.MakeDistSQLMetrics(base.DefaultHistogramWindowInterval())
	)
	defer stopper.Stop(ctx)

	scheduler := NewFlowScheduler(log.AmbientContext{}, stopper, settings, &metrics)
	scheduler.Start()
	scheduler.atomics.maxRunningFlows = 1
	getNumRunning := func() int {
		return int(atomic.LoadInt32(&scheduler.atomics.numRunning))
	}

	flow1 := newMockFlow()
	require.NoError(t, scheduler.ScheduleFlow(ctx, flow1))
	require.Equal(t, 1, getNumRunning())

	flow2 := newMockFlow()
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
}
