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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		RegisterProcessor(ctx, "dummy", appTenantP1)
		assertProcessorRegistered(t, appTenantID, "dummy", appTenantP1)

		appTenantP2 := &dummyTestProcessor{}
		RegisterProcessor(ctx, "dummy", appTenantP2)
		assertProcessorRegistered(t, appTenantID, "dummy", appTenantP2)

		sysTenantP1 := &dummyTestProcessor{}
		ctx := roachpb.ContextWithClientTenant(ctx, roachpb.SystemTenantID)
		RegisterProcessor(ctx, "dummy", sysTenantP1)
		assertProcessorRegistered(t, roachpb.SystemTenantID, "dummy", sysTenantP1)
	})

	t.Run("defaults to the system tenant when no tenant ID in context", func(t *testing.T) {
		ctxWithoutTenantID, cancelTestCtx := context.WithCancel(context.Background())
		defer cancelTestCtx()
		processor := &dummyTestProcessor{}
		RegisterProcessor(ctxWithoutTenantID, "dummy", processor)
		assertProcessorRegistered(t, roachpb.SystemTenantID, "dummy", processor)
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

type dummyTestProcessor struct{}

var _ Processor = (*dummyTestProcessor)(nil)

func (d dummyTestProcessor) Process(_ context.Context, _ any) error {
	return nil
}
