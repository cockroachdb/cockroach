// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package coldata

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMemColumnSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	c := NewMemColumn(types.Int64, BatchSize)

	ints := c.Int64()
	for i := uint16(0); i < BatchSize; i++ {
		ints[i] = int64(i)
		if i%2 == 0 {
			// Set every other value to null.
			c.Nulls().SetNull(i)
		}
	}

	startSlice := uint16(1)
	endSlice := uint16(0)
	for startSlice > endSlice {
		startSlice = uint16(rng.Intn(BatchSize))
		endSlice = uint16(1 + rng.Intn(BatchSize))
	}

	slice := c.Slice(types.Int64, uint64(startSlice), uint64(endSlice))
	sliceInts := slice.Int64()
	// Verify that every other value is null.
	for i, j := startSlice, uint16(0); i < endSlice; i, j = i+1, j+1 {
		if i%2 == 0 {
			if !slice.Nulls().NullAt(j) {
				t.Fatalf("expected null at %d (original index: %d)", j, i)
			}
			continue
		}
		if ints[i] != sliceInts[j] {
			t.Fatalf("unexected value at index %d (original index: %d): expected %d got %d", j, i, ints[i], sliceInts[j])
		}
	}
}

func TestNullRanges(t *testing.T) {
	tcs := []struct {
		start uint64
		end   uint64
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

	c := NewMemColumn(types.Int64, BatchSize)
	for _, tc := range tcs {
		c.Nulls().UnsetNulls()
		c.Nulls().SetNullRange(tc.start, tc.end)

		for i := uint64(0); i < BatchSize; i++ {
			if i >= tc.start && i < tc.end {
				if !c.Nulls().NullAt64(i) {
					t.Fatalf("expected null at %d, start: %d end: %d", i, tc.start, tc.end)
				}
			} else {
				if c.Nulls().NullAt64(i) {
					t.Fatalf("expected non-null at %d, start: %d end: %d", i, tc.start, tc.end)
				}
			}
		}
	}
}

func TestAppend(t *testing.T) {
	const typ = types.Int64

	src := NewMemColumn(typ, BatchSize)
	sel := make([]uint16, len(src.Int64()))
	for i := range sel {
		sel[i] = uint16(i)
	}

	testCases := []struct {
		name           string
		args           AppendArgs
		expectedLength int
	}{
		{
			name:           "AppendSimple",
			args:           AppendArgs{},
			expectedLength: BatchSize * 2,
		},
		{
			name: "AppendOverwriteSource",
			args: AppendArgs{
				// Start appending at index 10.
				DestIdx: 10,
			},
			expectedLength: BatchSize + 10,
		},
		{
			name: "AppendSlice",
			args: AppendArgs{
				DestIdx:     20,
				SrcStartIdx: 10,
				SrcEndIdx:   20,
			},
			expectedLength: 30,
		},
		{
			name: "AppendWithSel",
			args: AppendArgs{
				DestIdx:     5,
				SrcStartIdx: 10,
				SrcEndIdx:   20,
				Sel:         sel,
			},
			expectedLength: 15,
		},
		{
			name: "AppendWithHalfSel",
			args: AppendArgs{
				DestIdx: 5,
				Sel:     sel[:len(sel)/2],
			},
			expectedLength: 5 + (BatchSize / 2),
		},
	}

	for _, tc := range testCases {
		tc.args.Src = src
		tc.args.ColType = typ
		t.Run(tc.name, func(t *testing.T) {
			dest := NewMemColumn(typ, BatchSize)
			dest.Append(tc.args)
			require.Equal(t, tc.expectedLength, len(dest.Int64()))
		})
	}
}

func BenchmarkAppend(b *testing.B) {
	const typ = types.Int64

	src := NewMemColumn(typ, BatchSize)
	sel := make([]uint16, len(src.Int64()))

	benchCases := []struct {
		name string
		args AppendArgs
	}{
		{
			name: "SimpleAppend",
			args: AppendArgs{},
		},
		{
			name: "AppendWithSel",
			args: AppendArgs{
				Sel: sel,
			},
		},
	}

	for _, bc := range benchCases {
		bc.args.Src = src
		bc.args.ColType = typ
		dest := NewMemColumn(typ, BatchSize)
		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(8 * BatchSize)
			for i := 0; i < b.N; i++ {
				dest.Append(bc.args)
				// "Reset" dest for another round.
				dest.SetCol(dest.Int64()[:BatchSize])
			}
		})
	}
}
