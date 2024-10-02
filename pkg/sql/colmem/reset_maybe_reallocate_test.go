// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colmem

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestResetMaybeReallocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := eval.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := NewAllocator(ctx, &memAcc, testColumnFactory)

	t.Run("ResettingBehavior", func(t *testing.T) {
		if coldata.BatchSize() == 1 {
			skip.IgnoreLint(t, "the test assumes coldata.BatchSize() is at least 2")
		}

		var b coldata.Batch
		typs := []*types.T{types.Bytes}

		// Allocate a new batch and modify it.
		b, _, _ = testAllocator.resetMaybeReallocate(typs, b, coldata.BatchSize(), coldata.BatchSize(), math.MaxInt64, false /* desiredCapacitySufficient */, false /* alwaysReallocate */)
		b.SetSelection(true)
		b.Selection()[0] = 1
		b.ColVec(0).Bytes().Set(1, []byte("foo"))

		oldBatch := b
		b, _, _ = testAllocator.resetMaybeReallocate(typs, b, coldata.BatchSize(), coldata.BatchSize(), math.MaxInt64, false /* desiredCapacitySufficient */, false /* alwaysReallocate */)
		// We should have used the same batch, and now it should be in a "reset"
		// state.
		require.Equal(t, oldBatch, b)
		require.Nil(t, b.Selection())
		// We should be able to set in the Bytes vector using an arbitrary
		// position since the vector should have been reset.
		require.NotPanics(t, func() { b.ColVec(0).Bytes().Set(0, []byte("bar")) })
	})

	t.Run("LimitingByMemSize", func(t *testing.T) {
		if coldata.BatchSize() == 1 {
			skip.IgnoreLint(t, "the test assumes coldata.BatchSize() is at least 2")
		}

		var b coldata.Batch
		typs := []*types.T{types.Int}
		const minDesiredCapacity = 2
		const smallMemSize = 0
		const largeMemSize = math.MaxInt64

		// Allocate a batch with smaller capacity.
		smallBatch := testAllocator.NewMemBatchWithFixedCapacity(typs, minDesiredCapacity/2)

		// Allocate a new batch attempting to use the batch with too small of a
		// capacity - new batch should **not** be allocated because the memory
		// limit is already exceeded.
		b, _, _ = testAllocator.resetMaybeReallocate(typs, smallBatch, minDesiredCapacity, coldata.BatchSize(), smallMemSize, false /* desiredCapacitySufficient */, false /* alwaysReallocate */)
		require.Equal(t, smallBatch, b)
		require.Equal(t, minDesiredCapacity/2, b.Capacity())

		oldBatch := b

		// Reset the batch asking for the same small desired capacity when it is
		// sufficient - the same batch should be returned.
		b, _, _ = testAllocator.resetMaybeReallocate(typs, b, minDesiredCapacity/2, coldata.BatchSize(), smallMemSize, true /* desiredCapacitySufficient */, false /* alwaysReallocate */)
		require.Equal(t, smallBatch, b)
		require.Equal(t, minDesiredCapacity/2, b.Capacity())

		// Reset the batch and confirm that a new batch is allocated because we
		// have given larger memory limit.
		b, _, _ = testAllocator.resetMaybeReallocate(typs, b, minDesiredCapacity, coldata.BatchSize(), largeMemSize, false /* desiredCapacitySufficient */, false /* alwaysReallocate */)
		require.NotEqual(t, oldBatch, b)
		require.Equal(t, minDesiredCapacity, b.Capacity())

		if coldata.BatchSize() >= minDesiredCapacity*2 {
			// Now reset the batch with large memory limit - we should get a new
			// batch with the double capacity.
			//
			// resetMaybeReallocate truncates the capacity at
			// coldata.BatchSize(), so we run this part of the test only when
			// doubled capacity will not be truncated.
			b, _, _ = testAllocator.resetMaybeReallocate(typs, b, minDesiredCapacity, coldata.BatchSize(), largeMemSize, false /* desiredCapacitySufficient */, false /* alwaysReallocate */)
			require.NotEqual(t, oldBatch, b)
			require.Equal(t, 2*minDesiredCapacity, b.Capacity())
		}
	})
}
