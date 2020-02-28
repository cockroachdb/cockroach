// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMemColumnWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	c := NewMemColumn(coltypes.Int64, BatchSize())

	ints := c.Int64()
	for i := 0; i < BatchSize(); i++ {
		ints[i] = int64(i)
		if i%2 == 0 {
			// Set every other value to null.
			c.Nulls().SetNull(i)
		}
	}

	startWindow := 1
	endWindow := 0
	for startWindow > endWindow {
		startWindow = rng.Intn(BatchSize())
		endWindow = 1 + rng.Intn(BatchSize())
	}

	window := c.Window(coltypes.Int64, startWindow, endWindow)
	windowInts := window.Int64()
	// Verify that every other value is null.
	for i, j := startWindow, 0; i < endWindow; i, j = i+1, j+1 {
		if i%2 == 0 {
			if !window.Nulls().NullAt(j) {
				t.Fatalf("expected null at %d (original index: %d)", j, i)
			}
			continue
		}
		if ints[i] != windowInts[j] {
			t.Fatalf("unexected value at index %d (original index: %d): expected %d got %d", j, i, ints[i], windowInts[j])
		}
	}
}

func TestNullRanges(t *testing.T) {
	tcs := []struct {
		start int
		end   int
	}{
		{
			start: 1,
			end:   1,
		},
		{
			start: 50,
			end:   0,
		},
		{
			start: 0,
			end:   50,
		},
		{
			start: 0,
			end:   64,
		},
		{
			start: 25,
			end:   50,
		},
		{
			start: 0,
			end:   80,
		},
		{
			start: 20,
			end:   80,
		},
		{
			start: 0,
			end:   387,
		},
		{
			start: 385,
			end:   387,
		},
		{
			start: 0,
			end:   1023,
		},
		{
			start: 1022,
			end:   1023,
		}, {
			start: 1023,
			end:   1023,
		},
	}

	c := NewMemColumn(coltypes.Int64, BatchSize())
	for _, tc := range tcs {
		c.Nulls().UnsetNulls()
		c.Nulls().SetNullRange(tc.start, tc.end)

		for i := 0; i < BatchSize(); i++ {
			if i >= tc.start && i < tc.end {
				if !c.Nulls().NullAt(i) {
					t.Fatalf("expected null at %d, start: %d end: %d", i, tc.start, tc.end)
				}
			} else {
				if c.Nulls().NullAt(i) {
					t.Fatalf("expected non-null at %d, start: %d end: %d", i, tc.start, tc.end)
				}
			}
		}
	}
}

func TestAppend(t *testing.T) {
	// TODO(asubiotto): Test nulls.
	const typ = coltypes.Int64

	src := NewMemColumn(typ, BatchSize())
	sel := make([]int, len(src.Int64()))
	for i := range sel {
		sel[i] = i
	}

	testCases := []struct {
		name           string
		args           SliceArgs
		expectedLength int
	}{
		{
			name: "AppendSimple",
			args: SliceArgs{
				// DestIdx must be specified to append to the end of dest.
				DestIdx: BatchSize(),
			},
			expectedLength: BatchSize() * 2,
		},
		{
			name: "AppendOverwriteSimple",
			args: SliceArgs{
				// DestIdx 0, the default value, will start appending at index 0.
				DestIdx: 0,
			},
			expectedLength: BatchSize(),
		},
		{
			name: "AppendOverwriteSlice",
			args: SliceArgs{
				// Start appending at index 10.
				DestIdx: 10,
			},
			expectedLength: BatchSize() + 10,
		},
		{
			name: "AppendSlice",
			args: SliceArgs{
				DestIdx:     20,
				SrcStartIdx: 10,
				SrcEndIdx:   20,
			},
			expectedLength: 30,
		},
		{
			name: "AppendWithSel",
			args: SliceArgs{
				DestIdx:     5,
				SrcStartIdx: 10,
				SrcEndIdx:   20,
				Sel:         sel,
			},
			expectedLength: 15,
		},
		{
			name: "AppendWithHalfSel",
			args: SliceArgs{
				DestIdx:   5,
				Sel:       sel[:len(sel)/2],
				SrcEndIdx: len(sel) / 2,
			},
			expectedLength: 5 + (BatchSize())/2,
		},
	}

	for _, tc := range testCases {
		tc.args.Src = src
		tc.args.ColType = typ
		if tc.args.SrcEndIdx == 0 {
			// SrcEndIdx is always required.
			tc.args.SrcEndIdx = BatchSize()
		}
		t.Run(tc.name, func(t *testing.T) {
			dest := NewMemColumn(typ, BatchSize())
			dest.Append(tc.args)
			require.Equal(t, tc.expectedLength, len(dest.Int64()))
		})
	}
}

// TestAppendBytesWithLastNull makes sure that Append handles correctly the
// case when the last element of Bytes vector is NULL.
func TestAppendBytesWithLastNull(t *testing.T) {
	src := NewMemColumn(coltypes.Bytes, 4)
	sel := []int{0, 2, 3}
	src.Bytes().Set(0, []byte("zero"))
	src.Nulls().SetNull(1)
	src.Bytes().Set(2, []byte("two"))
	src.Nulls().SetNull(3)
	sliceArgs := SliceArgs{
		Src:         src,
		ColType:     coltypes.Bytes,
		DestIdx:     0,
		SrcStartIdx: 0,
		SrcEndIdx:   len(sel),
	}
	dest := NewMemColumn(coltypes.Bytes, 3)
	expected := NewMemColumn(coltypes.Bytes, 3)
	for _, withSel := range []bool{false, true} {
		t.Run(fmt.Sprintf("AppendBytesWithLastNull/sel=%t", withSel), func(t *testing.T) {
			expected.Nulls().UnsetNulls()
			expected.Bytes().Reset()
			if withSel {
				sliceArgs.Sel = sel
				for expIdx, srcIdx := range sel {
					if src.Nulls().NullAt(srcIdx) {
						expected.Nulls().SetNull(expIdx)
					} else {
						expected.Bytes().Set(expIdx, src.Bytes().Get(srcIdx))
					}
				}
			} else {
				sliceArgs.Sel = nil
				for expIdx := 0; expIdx < 3; expIdx++ {
					if src.Nulls().NullAt(expIdx) {
						expected.Nulls().SetNull(expIdx)
					} else {
						expected.Bytes().Set(expIdx, src.Bytes().Get(expIdx))
					}
				}
			}
			expected.Bytes().UpdateOffsetsToBeNonDecreasing(3)
			// require.Equal checks the "string-ified" versions of the vectors for
			// equality. Bytes uses maxSetIndex to print out "truncated"
			// representation, so we manually update it (Vec.Append will use
			// AppendVal function that updates maxSetIndex itself).
			expected.Bytes().maxSetIndex = 2
			dest.Append(sliceArgs)
			require.Equal(t, expected, dest)
		})
	}
}

func TestCopy(t *testing.T) {
	// TODO(asubiotto): Test nulls.
	const typ = coltypes.Int64

	src := NewMemColumn(typ, BatchSize())
	srcInts := src.Int64()
	for i := range srcInts {
		srcInts[i] = int64(i + 1)
	}
	sel := make([]int, len(src.Int64()))
	for i := range sel {
		sel[i] = i
	}

	sum := func(ints []int64) int {
		s := 0
		for _, i := range ints {
			s += int(i)
		}
		return s
	}

	testCases := []struct {
		name        string
		args        CopySliceArgs
		expectedSum int
	}{
		{
			name:        "CopyNothing",
			args:        CopySliceArgs{},
			expectedSum: 0,
		},
		{
			name: "CopyBatchSizeMinus1WithOffset1",
			args: CopySliceArgs{
				SliceArgs: SliceArgs{
					// Use DestIdx 1 to make sure that it is respected.
					DestIdx:   1,
					SrcEndIdx: BatchSize() - 1,
				},
			},
			// expectedSum uses sum of positive integers formula.
			expectedSum: (BatchSize() - 1) * BatchSize() / 2,
		},
		{
			name: "CopyWithSel",
			args: CopySliceArgs{
				SliceArgs: SliceArgs{
					Sel:         sel[1:],
					DestIdx:     25,
					SrcStartIdx: 1,
					SrcEndIdx:   2,
				},
			},
			// We'll have just the third element in the resulting slice.
			expectedSum: 3,
		},
	}

	for _, tc := range testCases {
		tc.args.Src = src
		tc.args.ColType = typ
		t.Run(tc.name, func(t *testing.T) {
			dest := NewMemColumn(typ, BatchSize())
			dest.Copy(tc.args)
			destInts := dest.Int64()
			firstNonZero := 0
			for i := range destInts {
				if destInts[i] != 0 {
					firstNonZero = i
					break
				}
			}
			// Verify that Copy started copying where we expected it to.
			require.Equal(t, tc.args.DestIdx, firstNonZero)
			require.Equal(t, tc.expectedSum, sum(destInts))
		})
	}
}

func TestCopyNulls(t *testing.T) {
	const typ = coltypes.Int64

	// Set up the destination vector.
	dst := NewMemColumn(typ, BatchSize())
	dstInts := dst.Int64()
	for i := range dstInts {
		dstInts[i] = int64(1)
	}
	// Set some nulls in the destination vector.
	for i := 0; i < 5; i++ {
		dst.Nulls().SetNull(i)
	}

	// Set up the source vector.
	src := NewMemColumn(typ, BatchSize())
	srcInts := src.Int64()
	for i := range srcInts {
		srcInts[i] = 2
	}
	// Set some nulls in the source.
	for i := 3; i < 8; i++ {
		src.Nulls().SetNull(i)
	}

	copyArgs := CopySliceArgs{
		SliceArgs: SliceArgs{
			ColType:     typ,
			Src:         src,
			DestIdx:     3,
			SrcStartIdx: 3,
			SrcEndIdx:   10,
		},
	}

	dst.Copy(copyArgs)

	// Verify that original nulls aren't deleted, and that
	// the nulls in the source have been copied over.
	for i := 0; i < 8; i++ {
		require.True(t, dst.Nulls().NullAt(i), "expected null at %d, found not null", i)
	}

	// Verify that the data from src has been copied over.
	for i := 8; i < 10; i++ {
		require.True(t, dstInts[i] == 2, "data from src was not copied over")
		require.True(t, !dst.Nulls().NullAt(i), "no extra nulls were added")
	}

	// Verify that the remaining elements in dst have not been touched.
	for i := 10; i < BatchSize(); i++ {
		require.True(t, dstInts[i] == 1, "data in dst outside copy range has been changed")
		require.True(t, !dst.Nulls().NullAt(i), "no extra nulls were added")
	}
}

func TestCopySelOnDestDoesNotUnsetOldNulls(t *testing.T) {
	const typ = coltypes.Int64

	// Set up the destination vector. It is all nulls except for a single
	// non-null at index 0.
	dst := NewMemColumn(typ, BatchSize())
	dstInts := dst.Int64()
	for i := range dstInts {
		dstInts[i] = 1
	}
	dst.Nulls().SetNulls()
	dst.Nulls().UnsetNull(0)

	// Set up the source vector with two nulls.
	src := NewMemColumn(typ, BatchSize())
	srcInts := src.Int64()
	for i := range srcInts {
		srcInts[i] = 2
	}
	src.Nulls().SetNull(0)
	src.Nulls().SetNull(3)

	// Using a small selection vector and SelOnDest, perform a copy and verify
	// that nulls in between the selected tuples weren't unset.
	copyArgs := CopySliceArgs{
		SelOnDest: true,
		SliceArgs: SliceArgs{
			ColType:     typ,
			Src:         src,
			SrcStartIdx: 1,
			SrcEndIdx:   3,
			Sel:         []int{0, 1, 3},
		},
	}

	dst.Copy(copyArgs)

	// 0 was not null in dest and null in source, but it wasn't selected. Not null.
	require.False(t, dst.Nulls().NullAt(0))
	// 1 was null in dest and not null in source: it becomes not null.
	require.False(t, dst.Nulls().NullAt(1))
	// 2 wasn't included in the selection vector: it stays null.
	require.True(t, dst.Nulls().NullAt(2))
	// 3 was null in dest and null in source: it stays null.
	require.True(t, dst.Nulls().NullAt(3))
	// 4 wasn't included: it stays null.
	require.True(t, dst.Nulls().NullAt(4))
}

func BenchmarkAppend(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	sel := rng.Perm(BatchSize())

	benchCases := []struct {
		name string
		args SliceArgs
	}{
		{
			name: "AppendSimple",
			args: SliceArgs{},
		},
		{
			name: "AppendWithSel",
			args: SliceArgs{
				Sel: sel,
			},
		},
	}

	for _, typ := range []coltypes.T{coltypes.Bytes, coltypes.Decimal, coltypes.Int64} {
		for _, nullProbability := range []float64{0, 0.2} {
			src := NewMemColumn(typ, BatchSize())
			RandomVec(rng, typ, 8 /* bytesFixedLength */, src, BatchSize(), nullProbability)
			for _, bc := range benchCases {
				bc.args.Src = src
				bc.args.ColType = typ
				bc.args.SrcEndIdx = BatchSize()
				dest := NewMemColumn(typ, BatchSize())
				b.Run(fmt.Sprintf("%s/%s/NullProbability=%.1f", typ, bc.name, nullProbability), func(b *testing.B) {
					b.SetBytes(8 * int64(BatchSize()))
					bc.args.DestIdx = 0
					for i := 0; i < b.N; i++ {
						dest.Append(bc.args)
						bc.args.DestIdx += BatchSize()
					}
				})
			}
		}
	}
}

func BenchmarkCopy(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	sel := rng.Perm(BatchSize())

	benchCases := []struct {
		name string
		args CopySliceArgs
	}{
		{
			name: "CopySimple",
			args: CopySliceArgs{},
		},
		{
			name: "CopyWithSel",
			args: CopySliceArgs{
				SliceArgs: SliceArgs{
					Sel: sel,
				},
			},
		},
	}

	for _, typ := range []coltypes.T{coltypes.Bytes, coltypes.Decimal, coltypes.Int64} {
		for _, nullProbability := range []float64{0, 0.2} {
			src := NewMemColumn(typ, BatchSize())
			RandomVec(rng, typ, 8 /* bytesFixedLength */, src, BatchSize(), nullProbability)
			for _, bc := range benchCases {
				bc.args.Src = src
				bc.args.ColType = typ
				bc.args.SrcEndIdx = BatchSize()
				dest := NewMemColumn(typ, BatchSize())
				b.Run(fmt.Sprintf("%s/%s/NullProbability=%.1f", typ, bc.name, nullProbability), func(b *testing.B) {
					b.SetBytes(8 * int64(BatchSize()))
					for i := 0; i < b.N; i++ {
						dest.Copy(bc.args)
						if typ == coltypes.Bytes {
							// We need to reset flat bytes so that we could copy into it
							// (otherwise it'll panic on the second copy due to maxSetIndex
							// being not zero).
							dest.Bytes().Reset()
						}
					}
				})
			}
		}
	}
}
