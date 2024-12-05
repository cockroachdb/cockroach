// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colmem

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestAdjustMemoryUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	unlimitedMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer unlimitedMemMonitor.Stop(ctx)
	unlimitedMemAcc := unlimitedMemMonitor.MakeBoundAccount()
	defer unlimitedMemAcc.Close(ctx)

	limitedMemMonitorName := "test-limited"
	limit := int64(100000)
	limitedMemMonitor := mon.NewMonitorInheritWithLimit(
		redact.SafeString(limitedMemMonitorName), limit, unlimitedMemMonitor, false, /* longLiving */
	)
	limitedMemMonitor.StartNoReserved(ctx, unlimitedMemMonitor)
	defer limitedMemMonitor.Stop(ctx)
	limitedMemAcc := limitedMemMonitor.MakeBoundAccount()
	defer limitedMemAcc.Close(ctx)

	evalCtx := eval.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	allocator := NewLimitedAllocator(ctx, &limitedMemAcc, &unlimitedMemAcc, testColumnFactory)

	// Check that no error occurs if the limit is not exceeded.
	require.NotPanics(t, func() { allocator.AdjustMemoryUsage(limit / 2) })
	require.Equal(t, limit/2, limitedMemAcc.Used())
	require.Zero(t, unlimitedMemAcc.Used())

	// Exceed the limit "before" making an allocation and ensure that the
	// unlimited account has not been grown.
	err := colexecerror.CatchVectorizedRuntimeError(func() { allocator.AdjustMemoryUsage(limit) })
	require.NotNil(t, err)
	require.True(t, strings.Contains(err.Error(), limitedMemMonitorName))
	require.Equal(t, limit/2, limitedMemAcc.Used())
	require.Zero(t, unlimitedMemAcc.Used())

	// Now exceed the limit "after" making an allocation and ensure that the
	// unlimited account has been grown.
	err = colexecerror.CatchVectorizedRuntimeError(func() { allocator.AdjustMemoryUsageAfterAllocation(limit) })
	require.NotNil(t, err)
	require.True(t, strings.Contains(err.Error(), limitedMemMonitorName))
	require.Equal(t, limit/2, limitedMemAcc.Used())
	require.Equal(t, limit, unlimitedMemAcc.Used())

	// Ensure that the error from the unlimited memory account is returned when
	// it cannot be grown.
	err = colexecerror.CatchVectorizedRuntimeError(func() { allocator.AdjustMemoryUsageAfterAllocation(math.MaxInt64) })
	require.NotNil(t, err)
	require.False(t, strings.Contains(err.Error(), limitedMemMonitorName))
	require.Equal(t, limit/2, limitedMemAcc.Used())
	require.Equal(t, limit, unlimitedMemAcc.Used())

	// Verify that both accounts are cleared on ReleaseAll.
	allocator.ReleaseAll()
	require.Zero(t, limitedMemAcc.Used())
	require.Zero(t, unlimitedMemAcc.Used())
}
