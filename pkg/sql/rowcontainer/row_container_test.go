// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rowcontainer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// verifyRows verifies that the rows read with the given RowIterator match up
// with  the given rows. evalCtx and ordering are used to compare rows.
func verifyRows(
	ctx context.Context,
	i RowIterator,
	expectedRows sqlbase.EncDatumRows,
	evalCtx *tree.EvalContext,
	ordering sqlbase.ColumnOrdering,
) error {
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			return err
		}
		if cmp, err := compareRows(
			sqlbase.OneIntCol, row, expectedRows[0], evalCtx, &sqlbase.DatumAlloc{}, ordering,
		); err != nil {
			return err
		} else if cmp != 0 {
			return fmt.Errorf("unexpected row %v, expected %v", row, expectedRows[0])
		}
		expectedRows = expectedRows[1:]
	}
	if len(expectedRows) != 0 {
		return fmt.Errorf("iterator missed %d row(s)", len(expectedRows))
	}
	return nil
}

// TestRowContainerReplaceMax verifies that MaybeReplaceMax correctly adjusts
// the memory accounting.
func TestRowContainerReplaceMax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	typeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	typeStr := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}

	makeRow := func(intVal int, strLen int) sqlbase.EncDatumRow {
		var b []byte
		for i := 0; i < strLen; i++ {
			b = append(b, 'a')
		}
		return sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(typeInt, tree.NewDInt(tree.DInt(intVal))),
			sqlbase.DatumToEncDatum(typeStr, tree.NewDString(string(b))),
		}
	}

	m := mon.MakeUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource, nil, nil, math.MaxInt64, st,
	)
	defer m.Stop(ctx)

	var mc MemRowContainer
	mc.InitWithMon(
		sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		[]sqlbase.ColumnType{typeInt, typeStr}, evalCtx, &m, 0, /* rowCapacity */
	)
	defer mc.Close(ctx)

	// Initialize the heap with small rows.
	for i := 0; i < 1000; i++ {
		err := mc.AddRow(ctx, makeRow(rng.Intn(10000), rng.Intn(10)))
		if err != nil {
			t.Fatal(err)
		}
	}
	mc.InitTopK()
	// Replace some of the rows with large rows.
	for i := 0; i < 1000; i++ {
		err := mc.MaybeReplaceMax(ctx, makeRow(rng.Intn(10000), rng.Intn(100)))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Now pop the rows, which shrinks the memory account according to the current
	// row sizes. If we did not account for the larger rows, this will panic.
	for mc.Len() > 0 {
		mc.PopFirst()
	}
}

func TestRowContainerIterators(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	const numRows = 10
	const numCols = 1
	rows := sqlbase.MakeIntRows(numRows, numCols)
	ordering := sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	var mc MemRowContainer
	mc.Init(
		ordering,
		sqlbase.OneIntCol,
		evalCtx,
	)
	defer mc.Close(ctx)

	for _, row := range rows {
		if err := mc.AddRow(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	// NewIterator verifies that we read the expected rows from the
	// MemRowContainer and can recreate an iterator.
	t.Run("NewIterator", func(t *testing.T) {
		for k := 0; k < 2; k++ {
			func() {
				i := mc.NewIterator(ctx)
				defer i.Close()
				if err := verifyRows(ctx, i, rows, evalCtx, ordering); err != nil {
					t.Fatalf("rows mismatch on the run number %d: %s", k+1, err)
				}
			}()
		}
	})

	// NewFinalIterator verifies that we read the expected rows from the
	// MemRowContainer and as we do so, these rows are deleted from the
	// MemRowContainer.
	t.Run("NewFinalIterator", func(t *testing.T) {
		i := mc.NewFinalIterator(ctx)
		defer i.Close()
		if err := verifyRows(ctx, i, rows, evalCtx, ordering); err != nil {
			t.Fatal(err)
		}
		if mc.Len() != 0 {
			t.Fatal("MemRowContainer is not empty")
		}
	})
}

func TestDiskBackedRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, err := engine.NewTempEngine(base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These monitors are started and stopped by subtests.
	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	const numRows = 10
	const numCols = 1
	rows := sqlbase.MakeIntRows(numRows, numCols)
	ordering := sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	rc := DiskBackedRowContainer{}
	rc.Init(
		ordering,
		sqlbase.OneIntCol,
		&evalCtx,
		tempEngine,
		&memoryMonitor,
		&diskMonitor,
		0, /* rowCapacity */
	)
	defer rc.Close(ctx)

	// NormalRun adds rows to a DiskBackedRowContainer, makes it spill to disk
	// halfway through, keeps on adding rows, and then verifies that all rows
	// were properly added to the DiskBackedRowContainer.
	t.Run("NormalRun", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)

		defer func() {
			if err := rc.UnsafeReset(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		mid := len(rows) / 2
		for i := 0; i < mid; i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.Spilled() {
			t.Fatal("unexpectedly using disk")
		}
		func() {
			i := rc.NewIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows[:mid], &evalCtx, ordering); err != nil {
				t.Fatalf("verifying memory rows failed with: %s", err)
			}
		}()
		if err := rc.spillToDisk(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.Spilled() {
			t.Fatal("unexpectedly using memory")
		}
		for i := mid; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		func() {
			i := rc.NewIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows, &evalCtx, ordering); err != nil {
				t.Fatalf("verifying disk rows failed with: %s", err)
			}
		}()
	})

	t.Run("AddRowOutOfMem", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(1))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)

		defer func() {
			if err := rc.UnsafeReset(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		if err := rc.AddRow(ctx, rows[0]); err != nil {
			t.Fatal(err)
		}
		if !rc.Spilled() {
			t.Fatal("expected to have spilled to disk")
		}
		if diskMonitor.AllocBytes() == 0 {
			t.Fatal("disk monitor reports no disk usage")
		}
		if memoryMonitor.AllocBytes() > 0 {
			t.Fatal("memory monitor reports unexpected usage")
		}
	})

	t.Run("AddRowOutOfDisk", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(1))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(1))

		defer func() {
			if err := rc.UnsafeReset(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		err := rc.AddRow(ctx, rows[0])
		if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeDiskFullError) {
			t.Fatalf(
				"unexpected error %v, expected disk full error %s", err, pgerror.CodeDiskFullError,
			)
		}
		if !rc.Spilled() {
			t.Fatal("expected to have tried to spill to disk")
		}
		if diskMonitor.AllocBytes() != 0 {
			t.Fatal("disk monitor reports unexpected usage")
		}
		if memoryMonitor.AllocBytes() != 0 {
			t.Fatal("memory monitor reports unexpected usage")
		}
	})
}

// verifyOrdering checks whether the rows in src are ordered according to
// ordering.
func verifyOrdering(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	src SortableRowContainer,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
) error {
	var datumAlloc sqlbase.DatumAlloc
	var rowAlloc sqlbase.EncDatumRowAlloc
	var prevRow sqlbase.EncDatumRow
	i := src.NewIterator(ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			return err
		}
		if prevRow != nil {
			if cmp, err := prevRow.Compare(types, &datumAlloc, ordering, evalCtx, row); err != nil {
				return err
			} else if cmp > 0 {
				return errors.Errorf("rows are not ordered as expected: %s was before %s", prevRow.String(types), row.String(types))
			}
		}
		prevRow = rowAlloc.CopyRow(row)
	}
	return nil
}

func TestDiskBackedIndexedRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, err := engine.NewTempEngine(base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	const numTestRuns = 10
	const numRows = 10
	const numCols = 2
	rows := make([]sqlbase.EncDatumRow, numRows)
	ordering := sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}
	newOrdering := sqlbase.ColumnOrdering{{ColIdx: 1, Direction: encoding.Ascending}}

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memoryMonitor.Stop(ctx)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	// SpillingHalfway adds half of all rows into DiskBackedIndexedRowContainer,
	// forces it to spill to disk, adds the second half into the container, and
	// verifies that the rows are read correctly (along with the corresponding
	// index).
	t.Run("SpillingHalfway", func(t *testing.T) {
		for i := 0; i < numTestRuns; i++ {
			types := sqlbase.RandSortingColumnTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = sqlbase.RandEncDatumRowOfTypes(rng, types)
			}

			func() {
				rc := MakeDiskBackedIndexedRowContainer(ordering, types, &evalCtx, tempEngine, &memoryMonitor, &diskMonitor, 0 /* rowCapacity */)
				defer rc.Close(ctx)
				mid := numRows / 2
				for i := 0; i < mid; i++ {
					if err := rc.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}
				if rc.Spilled() {
					t.Fatal("unexpectedly using disk")
				}
				if err := rc.spillToDisk(ctx); err != nil {
					t.Fatal(err)
				}
				if !rc.Spilled() {
					t.Fatal("unexpectedly using memory")
				}
				for i := mid; i < numRows; i++ {
					if err := rc.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}

				// Check equality of the row we wrote and the row we read.
				for i := 0; i < numRows; i++ {
					readRow := rc.GetRow(i)
					writtenRow := rows[readRow.GetIdx()]
					for col := range writtenRow {
						if cmp := readRow.GetDatum(col).Compare(&evalCtx, writtenRow[col].Datum); cmp != 0 {
							t.Fatalf("read row is not equal to written one")
						}
					}
				}
			}()
		}
	})

	// ReorderingInMemory initializes a DiskBackedIndexedRowContainer with one
	// ordering, adds all rows to it, sorts it and makes sure that the rows are
	// sorted as expected. Then, it reorders the container to a different
	// ordering, sorts it and verifies that the rows are in the order we expect.
	// Only in-memory containers should be used.
	t.Run("ReorderingInMemory", func(t *testing.T) {
		for i := 0; i < numTestRuns; i++ {
			types := sqlbase.RandSortingColumnTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = sqlbase.RandEncDatumRowOfTypes(rng, types)
			}
			storedTypes := make([]sqlbase.ColumnType, len(types)+1)
			copy(storedTypes, types)
			// The container will add an extra int column for indices.
			storedTypes[len(types)] = sqlbase.OneIntCol[0]

			func() {
				rc := MakeDiskBackedIndexedRowContainer(ordering, types, &evalCtx, tempEngine, &memoryMonitor, &diskMonitor, 0 /* rowCapacity */)
				defer rc.Close(ctx)
				for i := 0; i < numRows; i++ {
					if err := rc.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}
				rc.Sort(ctx)
				if err := verifyOrdering(ctx, &evalCtx, rc, storedTypes, ordering); err != nil {
					t.Fatal(err)
				}

				if err := rc.Reorder(ctx, newOrdering); err != nil {
					t.Fatal(err)
				}
				rc.Sort(ctx)
				if err := verifyOrdering(ctx, &evalCtx, rc, storedTypes, newOrdering); err != nil {
					t.Fatal(err)
				}
				if rc.Spilled() {
					t.Fatal("unexpectedly using disk")
				}
			}()
		}
	})

	// ReorderingOnDisk is the same as ReorderingInMemory except here the
	// container is forced to spill to disk right after initialization.
	t.Run("ReorderingOnDisk", func(t *testing.T) {
		for i := 0; i < numTestRuns; i++ {
			types := sqlbase.RandSortingColumnTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = sqlbase.RandEncDatumRowOfTypes(rng, types)
			}
			storedTypes := make([]sqlbase.ColumnType, len(types)+1)
			copy(storedTypes, types)
			// The container will add an extra int column for indices.
			storedTypes[len(types)] = sqlbase.OneIntCol[0]

			func() {
				d := MakeDiskBackedIndexedRowContainer(ordering, types, &evalCtx, tempEngine, &memoryMonitor, &diskMonitor, 0 /* rowCapacity */)
				defer d.Close(ctx)
				if err := d.spillToDisk(ctx); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < numRows; i++ {
					if err := d.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}
				d.Sort(ctx)
				if err := verifyOrdering(ctx, &evalCtx, d, storedTypes, ordering); err != nil {
					t.Fatal(err)
				}

				if err := d.Reorder(ctx, newOrdering); err != nil {
					t.Fatal(err)
				}
				d.Sort(ctx)
				if err := verifyOrdering(ctx, &evalCtx, d, storedTypes, newOrdering); err != nil {
					t.Fatal(err)
				}
				if !d.UsingDisk() {
					t.Fatal("unexpectedly using memory")
				}
			}()
		}
	})
}
