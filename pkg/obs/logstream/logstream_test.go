// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstream

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var testLogMeta = log.StructuredLogMeta{
	EventType: "dummy",
	Version:   "1.9",
}

func TestRegisterProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	appTenantID := roachpb.MinTenantID
	ctx := roachpb.ContextWithClientTenant(context.Background(), appTenantID)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	t.Run("registers multiple processors", func(t *testing.T) {
		appTenantP1 := &dummyTestProcessor{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		RegisterProcessor(ctx, stopper, testLogMeta, appTenantP1)
		assertProcessorRegistered(t, appTenantID, "dummy", appTenantP1)

		appTenantP2 := &dummyTestProcessor{}
		RegisterProcessor(ctx, stopper, testLogMeta, appTenantP2)
		assertProcessorRegistered(t, appTenantID, "dummy", appTenantP2)

		sysTenantP1 := &dummyTestProcessor{}
		ctx := roachpb.ContextWithClientTenant(ctx, roachpb.SystemTenantID)
		RegisterProcessor(ctx, stopper, testLogMeta, sysTenantP1)
		assertProcessorRegistered(t, roachpb.SystemTenantID, "dummy", sysTenantP1)
	})

	t.Run("defaults to the system tenant when no tenant ID in context", func(t *testing.T) {
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		ctxWithoutTenantID, cancelTestCtx := context.WithCancel(context.Background())
		defer cancelTestCtx()
		processor := &dummyTestProcessor{}
		RegisterProcessor(ctxWithoutTenantID, stopper, testLogMeta, processor)
		assertProcessorRegistered(t, roachpb.SystemTenantID, "dummy", processor)
	})
}

func TestProcess(t *testing.T) {
	t.Run("exits gracefully if no processor registered for tenant", func(t *testing.T) {
		controller.Process(context.Background(), "test_event_type", "dummy event")
	})

	t.Run("calls the correct processor for tenant", func(t *testing.T) {
		ctxApp := roachpb.ContextWithClientTenant(context.Background(), roachpb.MinTenantID)
		ctxSystem := roachpb.ContextWithClientTenant(context.Background(), roachpb.SystemTenantID)

		appStopper := stop.NewStopper()
		defer appStopper.Stop(ctxApp)
		sysStopper := stop.NewStopper()
		defer sysStopper.Stop(ctxSystem)

		appBuffer := newAsyncProcessorRouter(
			10*time.Millisecond,
			1,  /*triggerLen*/
			10, /*maxLen*/
		)
		mockAppProcessor := registerMockRoute(t, appBuffer, type1)
		require.NoError(t, appBuffer.Start(ctxApp, appStopper))

		sysBuffer := newAsyncProcessorRouter(
			10*time.Millisecond,
			1,  /*triggerLen*/
			10, /*maxLen*/
		)
		// We register a mock processor for the system tenant, but we don't
		// make assertions against it. The test will fail if unexpected calls
		// are made to the mock.
		registerMockRoute(t, sysBuffer, type1)
		require.NoError(t, sysBuffer.Start(ctxSystem, sysStopper))

		func() {
			controller.rmu.Lock()
			defer controller.rmu.Unlock()
			controller.rmu.tenantRouters[roachpb.SystemTenantID] = sysBuffer
			controller.rmu.tenantRouters[roachpb.MinTenantID] = appBuffer
		}()

		event1 := makeTypedEvent(type1, "test_event")
		event2 := makeTypedEvent(type1, "test_event_2")
		mockAppProcessor.EXPECT().Process(gomock.Any(), event1.event)
		mockAppProcessor.EXPECT().Process(gomock.Any(), event2.event)
		controller.Process(ctxApp, event1.eventType, event1.event)
		controller.Process(ctxApp, event2.eventType, event2.event)
	})
}

func assertProcessorRegistered(
	t *testing.T, tID roachpb.TenantID, eventType log.EventType, p Processor,
) {
	controller.rmu.Lock()
	defer controller.rmu.Unlock()
	require.NotZero(t, len(controller.rmu.tenantRouters))
	processor, ok := controller.rmu.tenantRouters[tID]
	require.True(t, ok)
	processor.rwmu.Lock()
	defer processor.rwmu.Unlock()
	routes, ok := processor.rwmu.routes[eventType]
	require.True(t, ok)
	require.NotZero(t, len(routes))
	require.Contains(t, routes, p)
}

type dummyTestProcessor struct {
	mu struct {
		syncutil.Mutex
		callCount int
	}
}

var _ Processor = (*dummyTestProcessor)(nil)

func (d *dummyTestProcessor) Process(_ context.Context, _ any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.callCount++
	return nil
}

func (d *dummyTestProcessor) CallCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.callCount
}
