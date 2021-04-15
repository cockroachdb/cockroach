// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// verifyRows verifies that the rows read with the given RowIterator match up
// with  the given rows. evalCtx and ordering are used to compare rows.
func verifyRows(
	ctx context.Context,
	i RowIterator,
	expectedRows rowenc.EncDatumRows,
	evalCtx *tree.EvalContext,
	ordering colinfo.ColumnOrdering,
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
			types.OneIntCol, row, expectedRows[0], evalCtx, &rowenc.DatumAlloc{}, ordering,
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

	makeRow := func(intVal int, strLen int) rowenc.EncDatumRow {
		var b []byte
		for i := 0; i < strLen; i++ {
			b = append(b, 'a')
		}
		return rowenc.EncDatumRow{
			rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(intVal))),
			rowenc.DatumToEncDatum(types.String, tree.NewDString(string(b))),
		}
	}

	m := mon.NewUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource, nil, nil, math.MaxInt64, st,
	)
	defer m.Stop(ctx)

	var mc MemRowContainer
	mc.InitWithMon(
		colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		[]*types.T{types.Int, types.String}, evalCtx, m,
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
		mc.PopFirst(ctx)
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
	rows := randgen.MakeIntRows(numRows, numCols)
	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	var mc MemRowContainer
	mc.Init(
		ordering,
		types.OneIntCol,
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
	tempEngine, _, err := storage.NewTempEngine(ctx, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These monitors are started and stopped by subtests.
	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.NewMonitor(
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
	rows := randgen.MakeIntRows(numRows, numCols)
	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	rc := DiskBackedRowContainer{}
	rc.Init(
		ordering,
		types.OneIntCol,
		&evalCtx,
		tempEngine,
		memoryMonitor,
		diskMonitor,
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
		if err := rc.SpillToDisk(ctx); err != nil {
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
		defer diskMonitor.Stop(ctx)

		defer func() {
			if err := rc.UnsafeReset(ctx); err != nil {
				t.Fatal(err)
			}
		}()

		err := rc.AddRow(ctx, rows[0])
		if code := pgerror.GetPGCode(err); code != pgcode.DiskFull {
			t.Fatalf(
				"unexpected error %v, expected disk full error %s", err, pgcode.DiskFull,
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

func TestDiskBackedRowContainerDeDuping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)

	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memoryMonitor.Stop(ctx)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	numRows := 10
	// Use 2 columns with both ascending and descending ordering to exercise
	// all possibilities with the randomly chosen types.
	const numCols = 2
	ordering := colinfo.ColumnOrdering{
		colinfo.ColumnOrderInfo{
			ColIdx:    0,
			Direction: encoding.Ascending,
		},
		colinfo.ColumnOrderInfo{
			ColIdx:    1,
			Direction: encoding.Descending,
		},
	}
	rng, _ := randutil.NewPseudoRand()
	// Use random types and random rows.
	types := randgen.RandSortingTypes(rng, numCols)
	numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := DiskBackedRowContainer{}
	rc.Init(
		ordering,
		types,
		&evalCtx,
		tempEngine,
		memoryMonitor,
		diskMonitor,
	)
	defer rc.Close(ctx)
	rc.DoDeDuplicate()

	for run := 0; run < 2; run++ {
		// Add rows to a DiskBackedRowContainer, and make it spill to disk halfway
		// through, and keep adding rows.
		mid := numRows / 2
		for i := 0; i < mid; i++ {
			idx, err := rc.AddRowWithDeDup(ctx, rows[i])
			require.NoError(t, err)
			require.Equal(t, i, idx)
		}
		require.Equal(t, false, rc.UsingDisk())
		require.NoError(t, rc.SpillToDisk(ctx))
		require.Equal(t, true, rc.UsingDisk())

		for i := mid; i < numRows; i++ {
			idx, err := rc.AddRowWithDeDup(ctx, rows[i])
			require.NoError(t, err)
			require.Equal(t, i, idx)
		}
		// Reset and reorder the rows for the next run.
		rand.Shuffle(numRows, func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		require.NoError(t, rc.UnsafeReset(ctx))
	}
}

// verifyOrdering checks whether the rows in src are ordered according to
// ordering.
func verifyOrdering(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	src SortableRowContainer,
	types []*types.T,
	ordering colinfo.ColumnOrdering,
) error {
	var datumAlloc rowenc.DatumAlloc
	var rowAlloc rowenc.EncDatumRowAlloc
	var prevRow rowenc.EncDatumRow
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
	tempEngine, _, err := storage.NewTempEngine(ctx, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.NewMonitor(
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
	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}
	newOrdering := colinfo.ColumnOrdering{{ColIdx: 1, Direction: encoding.Ascending}}

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
			rows := make([]rowenc.EncDatumRow, numRows)
			types := randgen.RandSortingTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = randgen.RandEncDatumRowOfTypes(rng, types)
			}

			func() {
				rc := NewDiskBackedIndexedRowContainer(ordering, types, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
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
				if err := rc.SpillToDisk(ctx); err != nil {
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
					readRow, err := rc.GetRow(ctx, i)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					writtenRow := rows[readRow.GetIdx()]
					for col := range writtenRow {
						datum, err := readRow.GetDatum(col)
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						if cmp := datum.Compare(&evalCtx, writtenRow[col].Datum); cmp != 0 {
							t.Fatalf("read row is not equal to written one")
						}
					}
				}
			}()
		}
	})

	// TestGetRow adds all rows into DiskBackedIndexedRowContainer, sorts them,
	// and checks that both the index and the row are what we expect by GetRow()
	// to be returned. Then, it spills to disk and does the same check again.
	t.Run("TestGetRow", func(t *testing.T) {
		for i := 0; i < numTestRuns; i++ {
			rows := make([]rowenc.EncDatumRow, numRows)
			sortedRows := indexedRows{rows: make([]IndexedRow, numRows)}
			types := randgen.RandSortingTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = randgen.RandEncDatumRowOfTypes(rng, types)
				sortedRows.rows[i] = IndexedRow{Idx: i, Row: rows[i]}
			}

			sorter := rowsSorter{evalCtx: &evalCtx, rows: sortedRows, ordering: ordering}
			sort.Sort(&sorter)
			if sorter.err != nil {
				t.Fatal(sorter.err)
			}

			func() {
				rc := NewDiskBackedIndexedRowContainer(ordering, types, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
				defer rc.Close(ctx)
				for _, row := range rows {
					if err := rc.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}
				}
				if rc.Spilled() {
					t.Fatal("unexpectedly using disk")
				}
				rc.Sort(ctx)

				// Check that GetRow returns the row we expect at each position.
				for i := 0; i < numRows; i++ {
					readRow, err := rc.GetRow(ctx, i)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					expectedRow := sortedRows.rows[i]
					if readRow.GetIdx() != expectedRow.GetIdx() {
						t.Fatalf("read row has different idx that what we expect")
					}
					for col, expectedDatum := range expectedRow.Row {
						readDatum, err := readRow.GetDatum(col)
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						if cmp := readDatum.Compare(&evalCtx, expectedDatum.Datum); cmp != 0 {
							t.Fatalf("read row is not equal to expected one")
						}
					}
				}
				if err := rc.SpillToDisk(ctx); err != nil {
					t.Fatal(err)
				}
				if !rc.Spilled() {
					t.Fatal("unexpectedly using memory")
				}

				// Check that GetRow returns the row we expect at each position.
				for i := 0; i < numRows; i++ {
					readRow, err := rc.GetRow(ctx, i)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					expectedRow := sortedRows.rows[i]
					if readRow.GetIdx() != expectedRow.GetIdx() {
						t.Fatalf("read row has different idx that what we expect")
					}
					for col, expectedDatum := range expectedRow.Row {
						readDatum, err := readRow.GetDatum(col)
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						if cmp := readDatum.Compare(&evalCtx, expectedDatum.Datum); cmp != 0 {
							t.Fatalf("read row is not equal to expected one")
						}
					}
				}
			}()
		}
	})

	// TestGetRowFromDiskWithLimitedMemory forces the container to spill to disk,
	// adds all rows to it, sorts them, and checks that both the index and the
	// row are what we expect by GetRow() to be returned. The goal is to test the
	// behavior of capping the cache size and reusing the memory of the first
	// rows in the cache, so we use the memory budget that accommodates only
	// about half of all rows in the cache.
	t.Run("TestGetRowWithLimitedMemory", func(t *testing.T) {
		for i := 0; i < numTestRuns; i++ {
			budget := int64(10240)
			memoryUsage := int64(0)
			rows := make([]rowenc.EncDatumRow, 0, numRows)
			sortedRows := indexedRows{rows: make([]IndexedRow, 0, numRows)}
			types := randgen.RandSortingTypes(rng, numCols)
			for memoryUsage < 2*budget {
				row := randgen.RandEncDatumRowOfTypes(rng, types)
				memoryUsage += int64(row.Size())
				rows = append(rows, row)
				sortedRows.rows = append(sortedRows.rows, IndexedRow{Idx: len(sortedRows.rows), Row: row})
			}

			memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(budget))
			defer memoryMonitor.Stop(ctx)
			diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
			defer diskMonitor.Stop(ctx)

			sorter := rowsSorter{evalCtx: &evalCtx, rows: sortedRows, ordering: ordering}
			sort.Sort(&sorter)
			if sorter.err != nil {
				t.Fatal(sorter.err)
			}

			func() {
				rc := NewDiskBackedIndexedRowContainer(ordering, types, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
				defer rc.Close(ctx)
				if err := rc.SpillToDisk(ctx); err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					if err := rc.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}
				}
				if !rc.Spilled() {
					t.Fatal("unexpectedly using memory")
				}
				rc.Sort(ctx)

				// Check that GetRow returns the row we expect at each position.
				for i := 0; i < len(rows); i++ {
					readRow, err := rc.GetRow(ctx, i)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					expectedRow := sortedRows.rows[i]
					readOrderingDatum, err := readRow.GetDatum(ordering[0].ColIdx)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if readOrderingDatum.Compare(&evalCtx, expectedRow.Row[ordering[0].ColIdx].Datum) != 0 {
						// We're skipping comparison if both rows are equal on the ordering
						// column since in this case the order of indexed rows after
						// sorting is nondeterministic.
						if readRow.GetIdx() != expectedRow.GetIdx() {
							t.Fatalf("read row has different idx that what we expect")
						}
						for col, expectedDatum := range expectedRow.Row {
							readDatum, err := readRow.GetDatum(col)
							if err != nil {
								t.Fatalf("unexpected error: %v", err)
							}
							if cmp := readDatum.Compare(&evalCtx, expectedDatum.Datum); cmp != 0 {
								t.Fatalf("read row is not equal to expected one")
							}
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
			rows := make([]rowenc.EncDatumRow, numRows)
			typs := randgen.RandSortingTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = randgen.RandEncDatumRowOfTypes(rng, typs)
			}
			storedTypes := make([]*types.T, len(typs)+1)
			copy(storedTypes, typs)
			// The container will add an extra int column for indices.
			storedTypes[len(typs)] = types.OneIntCol[0]

			func() {
				rc := NewDiskBackedIndexedRowContainer(ordering, typs, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
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
			rows := make([]rowenc.EncDatumRow, numRows)
			typs := randgen.RandSortingTypes(rng, numCols)
			for i := 0; i < numRows; i++ {
				rows[i] = randgen.RandEncDatumRowOfTypes(rng, typs)
			}
			storedTypes := make([]*types.T, len(typs)+1)
			copy(storedTypes, typs)
			// The container will add an extra int column for indices.
			storedTypes[len(typs)] = types.OneIntCol[0]

			func() {
				d := NewDiskBackedIndexedRowContainer(ordering, typs, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
				defer d.Close(ctx)
				if err := d.SpillToDisk(ctx); err != nil {
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

// indexedRows are rows with the corresponding indices.
type indexedRows struct {
	rows []IndexedRow
}

// Len implements tree.IndexedRows interface.
func (ir indexedRows) Len() int {
	return len(ir.rows)
}

// TODO(yuzefovich): this is a duplicate of partitionSorter from windower.go.
// There are possibly couple of other duplicates as well in other files, so we
// should refactor it and probably extract the code into a new package.
type rowsSorter struct {
	evalCtx  *tree.EvalContext
	rows     indexedRows
	ordering colinfo.ColumnOrdering
	err      error
}

func (n *rowsSorter) Len() int { return n.rows.Len() }

func (n *rowsSorter) Swap(i, j int) {
	n.rows.rows[i], n.rows.rows[j] = n.rows.rows[j], n.rows.rows[i]
}
func (n *rowsSorter) Less(i, j int) bool {
	if n.err != nil {
		// An error occurred in previous calls to Less(). We want to be done with
		// sorting and to propagate that error to the caller of Sort().
		return false
	}
	cmp, err := n.Compare(i, j)
	if err != nil {
		n.err = err
		return false
	}
	return cmp < 0
}

func (n *rowsSorter) Compare(i, j int) (int, error) {
	ra, rb := n.rows.rows[i], n.rows.rows[j]
	for _, o := range n.ordering {
		da, err := ra.GetDatum(o.ColIdx)
		if err != nil {
			return 0, err
		}
		db, err := rb.GetDatum(o.ColIdx)
		if err != nil {
			return 0, err
		}
		if c := da.Compare(n.evalCtx, db); c != 0 {
			if o.Direction != encoding.Ascending {
				return -c, nil
			}
			return c, nil
		}
	}
	return 0, nil
}

// generateAccessPattern populates int slices with position of rows to be
// accessed by GetRow(). The goal is to simulate an access pattern that would
// resemble a real one that might occur while window functions are computed.
func generateAccessPattern(numRows int) []int {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	const avgPeerGroupSize = 100
	accessPattern := make([]int, 0, 2*numRows)
	nextPeerGroupStart := 0
	for {
		peerGroupSize := int(rng.NormFloat64()*avgPeerGroupSize) + avgPeerGroupSize
		if peerGroupSize < 1 {
			peerGroupSize = 1
		}
		if nextPeerGroupStart+peerGroupSize > numRows {
			peerGroupSize = numRows - nextPeerGroupStart
		}
		accessPattern = append(accessPattern, nextPeerGroupStart)
		for j := 1; j < peerGroupSize; j++ {
			accessPattern = append(accessPattern, accessPattern[len(accessPattern)-1]+1)
		}
		for j := 0; j < peerGroupSize; j++ {
			accessPattern = append(accessPattern, accessPattern[len(accessPattern)-peerGroupSize])
		}
		nextPeerGroupStart += peerGroupSize
		if nextPeerGroupStart == numRows {
			return accessPattern
		}
	}
}

func BenchmarkDiskBackedIndexedRowContainer(b *testing.B) {
	const numCols = 1
	const numRows = 100000

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	rows := randgen.MakeIntRows(numRows, numCols)
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memoryMonitor.Stop(ctx)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	accessPattern := generateAccessPattern(numRows)

	b.Run("InMemory", func(b *testing.B) {
		rc := NewDiskBackedIndexedRowContainer(nil, types.OneIntCol, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
		defer rc.Close(ctx)
		for i := 0; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				b.Fatal(err)
			}
		}
		if rc.Spilled() {
			b.Fatal("unexpectedly using disk")
		}
		b.SetBytes(int64(8 * numCols))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pos := accessPattern[i%len(accessPattern)]
			if _, err := rc.GetRow(ctx, pos); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("OnDiskWithCache", func(b *testing.B) {
		rc := NewDiskBackedIndexedRowContainer(nil, types.OneIntCol, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
		defer rc.Close(ctx)
		if err := rc.SpillToDisk(ctx); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				b.Fatal(err)
			}
		}
		if !rc.Spilled() {
			b.Fatal("unexpectedly using memory")
		}
		b.SetBytes(int64(8 * numCols))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pos := accessPattern[i%len(accessPattern)]
			if _, err := rc.GetRow(ctx, pos); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("OnDiskWithoutCache", func(b *testing.B) {
		rc := NewDiskBackedIndexedRowContainer(nil, types.OneIntCol, &evalCtx, tempEngine, memoryMonitor, diskMonitor)
		defer rc.Close(ctx)
		if err := rc.SpillToDisk(ctx); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				b.Fatal(err)
			}
		}
		if !rc.Spilled() {
			b.Fatal("unexpectedly using memory")
		}
		b.SetBytes(int64(8 * numCols))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pos := accessPattern[i%len(accessPattern)]
			_ = rc.getRowWithoutCache(ctx, pos)
		}
		b.StopTimer()
	})
}
