// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestBatchReset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	resetAndCheck := func(b coldata.Batch, typs []*types.T, n int, shouldReuse bool) {
		t.Helper()
		// Use the data backing the ColVecs slice as a proxy for when things get
		// reallocated.
		vecsBefore := b.ColVecs()
		ptrBefore := (*reflect.SliceHeader)(unsafe.Pointer(&vecsBefore))
		b.Reset(typs, n, coldata.StandardColumnFactory)
		vecsAfter := b.ColVecs()
		ptrAfter := (*reflect.SliceHeader)(unsafe.Pointer(&vecsAfter))
		assert.Equal(t, shouldReuse, ptrBefore.Data == ptrAfter.Data)
		assert.Equal(t, n, b.Length())
		assert.Equal(t, len(typs), b.Width())

		assert.Nil(t, b.Selection())
		b.SetSelection(true)
		// Invariant: selection vector length matches batch length
		// Invariant: all cap(column) is >= cap(selection vector)
		assert.Equal(t, n, len(b.Selection()))
		selCap := cap(b.Selection())

		for i, vec := range b.ColVecs() {
			assert.False(t, vec.MaybeHasNulls())
			assert.False(t, vec.Nulls().NullAt(0))
			assert.True(t, typs[i].Identical(vec.Type()))
			// Sanity check that we can actually use the column. This is mostly for
			// making sure a flat bytes column gets reset.
			vec.Nulls().SetNull(0)
			assert.True(t, vec.Nulls().NullAt(0))
			switch vec.CanonicalTypeFamily() {
			case types.IntFamily:
				x := vec.Int64()
				assert.True(t, len(x) >= n)
				assert.True(t, cap(x) >= selCap)
				x[0] = 1
			case types.BytesFamily:
				x := vec.Bytes()
				assert.True(t, x.Len() >= n)
				x.Set(0, []byte{1})
			default:
				panic(vec.Type())
			}
		}
	}

	typsInt := []*types.T{types.Int}
	typsBytes := []*types.T{types.Bytes}
	typsIntBytes := []*types.T{types.Int, types.Bytes}
	var b coldata.Batch

	// Simple case, reuse
	b = coldata.NewMemBatch(typsInt, coldata.StandardColumnFactory)
	resetAndCheck(b, typsInt, 1, true)

	// Types don't match, don't reuse
	b = coldata.NewMemBatch(typsInt, coldata.StandardColumnFactory)
	resetAndCheck(b, typsBytes, 1, false)

	// Columns are a prefix, reuse
	b = coldata.NewMemBatch(typsIntBytes, coldata.StandardColumnFactory)
	resetAndCheck(b, typsInt, 1, true)

	// Exact length, reuse
	b = coldata.NewMemBatchWithCapacity(typsInt, 1, coldata.StandardColumnFactory)
	resetAndCheck(b, typsInt, 1, true)

	// Insufficient capacity, don't reuse
	b = coldata.NewMemBatchWithCapacity(typsInt, 1, coldata.StandardColumnFactory)
	resetAndCheck(b, typsInt, 2, false)

	// Selection vector gets reset
	b = coldata.NewMemBatchWithCapacity(typsInt, 1, coldata.StandardColumnFactory)
	b.SetSelection(true)
	b.Selection()[0] = 7
	resetAndCheck(b, typsInt, 1, true)

	// Nulls gets reset
	b = coldata.NewMemBatchWithCapacity(typsInt, 1, coldata.StandardColumnFactory)
	b.ColVec(0).Nulls().SetNull(0)
	resetAndCheck(b, typsInt, 1, true)

	// Bytes columns use a different impl than everything else
	b = coldata.NewMemBatch(typsBytes, coldata.StandardColumnFactory)
	resetAndCheck(b, typsBytes, 1, true)
}

// TestBatchWithBytesAndNulls verifies that the invariant of Bytes vectors is
// maintained when NULL values are present in the vector and there is a
// selection vector on the batch.
func TestBatchWithBytesAndNulls(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := coldata.NewMemBatch([]*types.T{types.Bytes}, coldata.StandardColumnFactory)
	// We will insert some garbage data into the Bytes vector in positions that
	// are not mentioned in the selection vector. All the values that are
	// selected are actually NULL values, so we don't set anything on the Bytes
	// vector there.
	if coldata.BatchSize() < 6 {
		return
	}
	sel := []int{1, 3, 5}
	vec := b.ColVec(0).Bytes()
	vec.Set(0, []byte("zero"))
	vec.Set(2, []byte("two"))
	b.SetSelection(true)
	copy(b.Selection(), sel)

	// This is where the invariant of non-decreasing offsets in the Bytes vector
	// should be updated.
	b.SetLength(len(sel))

	// In many cases in the vectorized execution engine, for performance
	// reasons, we will attempt to get something from the Bytes vector at
	// positions on which we have NULLs, so all of the Gets below should be
	// safe.
	for _, idx := range sel {
		assert.True(t, len(vec.Get(idx)) == 0)
	}
}

// Import colconv package in order to inject the implementation of
// coldata.VecsToStringWithRowPrefix.
var _ colconv.VecToDatumConverter

func TestBatchString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := coldata.NewMemBatch([]*types.T{types.String}, coldata.StandardColumnFactory)
	input := []string{"one", "two", "three"}
	for i := range input {
		b.ColVec(0).Bytes().Set(i, []byte(input[i]))
	}
	getExpected := func(length int, sel []int) string {
		var result string
		for i := 0; i < length; i++ {
			if i > 0 {
				result += "\n"
			}
			rowIdx := i
			if sel != nil {
				rowIdx = sel[i]
			}
			result += "['" + input[rowIdx] + "']"
		}
		return result
	}
	for _, tc := range []struct {
		length int
		sel    []int
	}{
		{length: 3},
		{length: 2, sel: []int{0, 2}},
	} {
		b.SetSelection(tc.sel != nil)
		copy(b.Selection(), tc.sel)
		b.SetLength(tc.length)
		assert.Equal(t, getExpected(tc.length, tc.sel), b.String())
	}
}
