// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowcontainer

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

func TestRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, numCols := range []int{0, 1, 2, 3, 5, 10, 15} {
		for _, numRows := range []int{5, 10, 100} {
			for _, numPops := range []int{0, 1, 2, numRows / 3, numRows / 2} {
				resCol := make(colinfo.ResultColumns, numCols)
				for i := range resCol {
					resCol[i] = colinfo.ResultColumn{Typ: types.Int}
				}
				st := cluster.MakeTestingClusterSettings()
				m := mon.NewUnlimitedMonitor(
					ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st,
				)
				rc := NewRowContainer(m.MakeBoundAccount(), colinfo.ColTypeInfoFromResCols(resCol))
				row := make(tree.Datums, numCols)
				for i := 0; i < numRows; i++ {
					for j := range row {
						row[j] = tree.NewDInt(tree.DInt(i*numCols + j))
					}
					if _, err := rc.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}
				}

				for i := 0; i < numPops; i++ {
					rc.PopFirst(ctx)
				}

				// Given that we just deleted numPops rows, we have numRows -
				// numPops rows remaining.
				if rc.Len() != numRows-numPops {
					t.Fatalf("invalid length, expected %d got %d", numRows-numPops, rc.Len())
				}

				// what was previously rc.At(i + numPops) is now rc.At(i).
				for i := 0; i < rc.Len(); i++ {
					row := rc.At(i)
					for j := range row {
						dint, ok := tree.AsDInt(row[j])
						if !ok || int(dint) != (i+numPops)*numCols+j {
							t.Fatalf("invalid value %+v on row %d, col %d", row[j], i+numPops, j)
						}
					}
				}
				rc.Close(ctx)
				m.Stop(ctx)
			}
		}
	}
}

func TestRowContainerAtOutOfRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := mon.NewUnlimitedMonitor(ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st)
	defer m.Stop(ctx)

	resCols := colinfo.ResultColumns{colinfo.ResultColumn{Typ: types.Int}}
	rc := NewRowContainer(m.MakeBoundAccount(), colinfo.ColTypeInfoFromResCols(resCols))
	defer rc.Close(ctx)

	// Verify that a panic is thrown for out-of-range conditions.
	for _, i := range []int{-1, 0} {
		var p interface{}
		func() {
			defer func() {
				p = recover()
			}()
			rc.At(i)
		}()
		if p == nil {
			t.Fatalf("%d: expected panic, but found success", i)
		}
	}
}

func TestRowContainerZeroCols(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := mon.NewUnlimitedMonitor(ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st)
	defer m.Stop(ctx)

	rc := NewRowContainer(m.MakeBoundAccount(), colinfo.ColTypeInfoFromResCols(nil))
	defer rc.Close(ctx)

	const numRows = 10
	for i := 0; i < numRows; i++ {
		if _, err := rc.AddRow(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	if rc.Len() != numRows {
		t.Fatalf("expected %d rows, but found %d", numRows, rc.Len())
	}
	row := rc.At(0)
	if row == nil {
		t.Fatalf("expected non-nil row")
	}
	if len(row) != 0 {
		t.Fatalf("expected empty row")
	}

	// Clear and try again.
	rc.Clear(ctx)

	const numRowsAfterClear = 5
	for i := 0; i < numRowsAfterClear; i++ {
		if _, err := rc.AddRow(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	if rc.Len() != numRowsAfterClear {
		t.Fatalf("expected %d rows, but found %d", numRowsAfterClear, rc.Len())
	}
	row = rc.At(0)
	if row == nil {
		t.Fatalf("expected non-nil row")
	}
	if len(row) != 0 {
		t.Fatalf("expected empty row")
	}
}

func BenchmarkRowContainerAt(b *testing.B) {
	const numCols = 3
	const numRows = 1024

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := mon.NewUnlimitedMonitor(
		ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st,
	)
	defer m.Stop(ctx)

	resCol := make(colinfo.ResultColumns, numCols)
	for i := range resCol {
		resCol[i] = colinfo.ResultColumn{Typ: types.Int}
	}

	rc := NewRowContainer(m.MakeBoundAccount(), colinfo.ColTypeInfoFromResCols(resCol))
	defer rc.Close(ctx)

	row := make(tree.Datums, numCols)
	for i := 0; i < numRows; i++ {
		for j := range row {
			row[j] = tree.NewDInt(tree.DInt(i*numCols + j))
		}
		if _, err := rc.AddRow(ctx, row); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rc.At(i & (numRows - 1))
	}
	b.StopTimer()
}
