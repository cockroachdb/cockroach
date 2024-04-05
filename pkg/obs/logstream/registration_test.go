// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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
		RegisterProcessor(ctx, stopper, "dummy", appTenantP1)
		assertProcessorRegistered(t, appTenantID, "dummy", appTenantP1)

		appTenantP2 := &dummyTestProcessor{}
		RegisterProcessor(ctx, stopper, "dummy", appTenantP2)
		assertProcessorRegistered(t, appTenantID, "dummy", appTenantP2)

		sysTenantP1 := &dummyTestProcessor{}
		ctx := roachpb.ContextWithClientTenant(ctx, roachpb.SystemTenantID)
		RegisterProcessor(ctx, stopper, "dummy", sysTenantP1)
		assertProcessorRegistered(t, roachpb.SystemTenantID, "dummy", sysTenantP1)
	})

	t.Run("defaults to the system tenant when no tenant ID in context", func(t *testing.T) {
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		ctxWithoutTenantID, cancelTestCtx := context.WithCancel(context.Background())
		defer cancelTestCtx()
		processor := &dummyTestProcessor{}
		RegisterProcessor(ctxWithoutTenantID, stopper, "dummy", processor)
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

		appProcessor := &dummyTestProcessor{}
		appBuffer := newProcessorBuffer(
			appProcessor,
			10*time.Millisecond,
			1,  /*triggerLen*/
			10, /*maxLen*/
		)
		require.NoError(t, appBuffer.Start(ctxApp, appStopper))

		sysProcessor := &dummyTestProcessor{}
		sysBuffer := newProcessorBuffer(
			sysProcessor,
			10*time.Millisecond,
			1,  /*triggerLen*/
			10, /*maxLen*/
		)
		require.NoError(t, sysBuffer.Start(ctxSystem, sysStopper))

		func() {
			controller.rmu.Lock()
			defer controller.rmu.Unlock()
			controller.rmu.tenantProcessors[roachpb.SystemTenantID] = sysBuffer
			controller.rmu.tenantProcessors[roachpb.MinTenantID] = appBuffer
		}()

		eventType := log.EventType("test")
		controller.Process(ctxApp, eventType, "test_event")
		controller.Process(ctxApp, eventType, "test_event_2")

		testutils.SucceedsSoon(t, func() error {
			if appProcessor.CallCount() != 2 {
				return errors.New("still waiting for async buffer to call processor")
			}
			require.Equal(t, 2, appProcessor.CallCount())
			require.Equal(t, 0, sysProcessor.CallCount())
			return nil
		})
	})
}

func assertProcessorRegistered(
	t *testing.T, tID roachpb.TenantID, eventType log.EventType, p Processor,
) {
	controller.rmu.Lock()
	defer controller.rmu.Unlock()
	require.NotZero(t, len(controller.rmu.tenantProcessors))
	processor, ok := controller.rmu.tenantProcessors[tID]
	require.True(t, ok)
	child, ok := processor.child.(*LogTypeEventRouter)
	require.True(t, ok)
	child.mu.Lock()
	defer child.mu.Unlock()
	routes, ok := child.mu.routes[eventType]
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
