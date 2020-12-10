// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colmem_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestMaybeAppendColumn(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	t.Run("VectorAlreadyPresent", func(t *testing.T) {
		b := testAllocator.NewMemBatchWithMaxCapacity([]*types.T{types.Int})
		b.SetLength(coldata.BatchSize())
		colIdx := 0

		// We expect an error to occur because of a type mismatch.
		err := colexecerror.CatchVectorizedRuntimeError(func() {
			testAllocator.MaybeAppendColumn(b, types.Float, colIdx)
		})
		require.NotNil(t, err)

		// We expect that the old vector is reallocated because the present one
		// is made to be of insufficient capacity.
		b.ReplaceCol(testAllocator.NewMemColumn(types.Int, 1 /* capacity */), colIdx)
		testAllocator.MaybeAppendColumn(b, types.Int, colIdx)
		require.Equal(t, coldata.BatchSize(), b.ColVec(colIdx).Capacity())

		// We expect that Bytes vector is reset when it is being reused (if it
		// isn't, a panic will occur when we try to set at the same positions).
		bytesColIdx := 1
		testAllocator.MaybeAppendColumn(b, types.Bytes, bytesColIdx)
		b.ColVec(bytesColIdx).Bytes().Set(0, []byte{0})
		b.ColVec(bytesColIdx).Bytes().Set(1, []byte{1})
		testAllocator.MaybeAppendColumn(b, types.Bytes, bytesColIdx)
		b.ColVec(bytesColIdx).Bytes().Set(0, []byte{0})
		b.ColVec(bytesColIdx).Bytes().Set(1, []byte{1})
	})

	t.Run("WindowedBatchZeroCapacity", func(t *testing.T) {
		b := testAllocator.NewMemBatchWithFixedCapacity([]*types.T{}, 0 /* capacity */)
		b.SetLength(coldata.BatchSize())
		colIdx := 0

		// We expect that although the batch is of zero capacity, the newly
		// appended vectors are allocated of coldata.BatchSize() capacity.
		testAllocator.MaybeAppendColumn(b, types.Int, colIdx)
		require.Equal(t, 1, b.Width())
		require.Equal(t, coldata.BatchSize(), b.ColVec(colIdx).Length())
		_ = b.ColVec(colIdx).Int64()[0]
	})
}
