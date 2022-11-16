// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestServerAssignment(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	tenantID := roachpb.MustMakeTenantID(10)
	handle := &testConnHandle{}
	sa := NewServerAssignment(tenantID, tracker, handle, "127.0.0.10")
	require.Equal(t, handle, sa.Owner())
	require.Equal(t, "127.0.0.10", sa.Addr())
	require.Equal(t, map[string][]ConnectionHandle{
		sa.Addr(): {handle},
	}, tracker.GetConnsMap(tenantID))

	// Once Close gets invoked, assignments should be empty.
	sa.Close()
	require.Empty(t, tracker.GetConnsMap(tenantID))

	// Invoke Close again for idempotency.
	sa.Close()
	require.Empty(t, tracker.GetConnsMap(tenantID))
}
