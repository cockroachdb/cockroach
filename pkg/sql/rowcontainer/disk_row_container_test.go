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
	math "math"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// compareRows compares l and r according to a column ordering. Returns -1 if
// l < r, 0 if l == r, and 1 if l > r. If an error is returned the int returned
// is invalid. Note that the comparison is only performed on the ordering
// columns.
func compareRows(
	lTypes []*types.T,
	l, r rowenc.EncDatumRow,
	e *tree.EvalContext,
	d *rowenc.DatumAlloc,
	ordering colinfo.ColumnOrdering,
) (int, error) {
	for _, orderInfo := range ordering {
		col := orderInfo.ColIdx
		cmp, err := l[col].Compare(lTypes[col], d, e, &r[orderInfo.ColIdx])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if orderInfo.Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

func TestDiskRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These orderings assume at least 4 columns.
	numCols := 4
	orderings := []colinfo.ColumnOrdering{
		{
			colinfo.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		{
			colinfo.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Descending,
			},
		},
		{
			colinfo.ColumnOrderInfo{
				ColIdx:    3,
				Direction: encoding.Ascending,
			},
			colinfo.ColumnOrderInfo{
				ColIdx:    1,
				Direction: encoding.Descending,
			},
			colinfo.ColumnOrderInfo{
				ColIdx:    2,
				Direction: encoding.Ascending,
			},
		},
	}

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	evalCtx := tree.MakeTestingEvalContext(st)
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)
	t.Run("EncodeDecode", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			// Test with different orderings so that we have a mix of key and
			// value encodings.
			for _, ordering := range orderings {
				typs := make([]*types.T, numCols)
				for i := range typs {
					typs[i] = randgen.RandSortingType(rng)
				}
				row := randgen.RandEncDatumRowOfTypes(rng, typs)
				func() {
					d := MakeDiskRowContainer(diskMonitor, typs, ordering, tempEngine)
					defer d.Close(ctx)
					if err := d.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}

					i := d.NewIterator(ctx)
					defer i.Close()
					i.Rewind()
					if ok, err := i.Valid(); err != nil {
						t.Fatal(err)
					} else if !ok {
						t.Fatal("unexpectedly invalid")
					}
					readRow := make(rowenc.EncDatumRow, len(row))

					temp, err := i.Row()
					if err != nil {
						t.Fatal(err)
					}
					copy(readRow, temp)

					// Ensure the datum fields are set and no errors occur when
					// decoding.
					for i, encDatum := range readRow {
						if err := encDatum.EnsureDecoded(typs[i], d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check equality of the row we wrote and the row we read.
					for i := range row {
						if cmp, err := readRow[i].Compare(typs[i], d.datumAlloc, &evalCtx, &row[i]); err != nil {
							t.Fatal(err)
						} else if cmp != 0 {
							t.Fatalf("encoded %s but decoded %s", row.String(typs), readRow.String(typs))
						}
					}
				}()
			}
		}
	})

	t.Run("SortedOrder", func(t *testing.T) {
		numRows := 1024
		for _, ordering := range orderings {
			// numRows rows with numCols columns of random types.
			types := randgen.RandSortingTypes(rng, numCols)
			rows := randgen.RandEncDatumRowsOfTypes(rng, numRows, types)
			func() {
				d := MakeDiskRowContainer(diskMonitor, types, ordering, tempEngine)
				defer d.Close(ctx)
				for i := 0; i < len(rows); i++ {
					if err := d.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}

				// Make another row container that stores all the rows then sort
				// it to compare equality.
				var sortedRows MemRowContainer
				sortedRows.Init(ordering, types, &evalCtx)
				defer sortedRows.Close(ctx)
				for _, row := range rows {
					if err := sortedRows.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}
				}
				sortedRows.Sort(ctx)

				i := d.NewIterator(ctx)
				defer i.Close()

				numKeysRead := 0
				for i.Rewind(); ; i.Next() {
					if ok, err := i.Valid(); err != nil {
						t.Fatal(err)
					} else if !ok {
						break
					}
					row, err := i.Row()
					if err != nil {
						t.Fatal(err)
					}

					// Ensure datum fields are set and no errors occur when
					// decoding.
					for i, encDatum := range row {
						if err := encDatum.EnsureDecoded(types[i], d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check sorted order.
					if cmp, err := compareRows(
						types, sortedRows.EncRow(numKeysRead), row, &evalCtx, d.datumAlloc, ordering,
					); err != nil {
						t.Fatal(err)
					} else if cmp != 0 {
						t.Fatalf(
							"expected %s to be equal to %s",
							row.String(types),
							sortedRows.EncRow(numKeysRead).String(types),
						)
					}
					numKeysRead++
				}
				if numKeysRead != numRows {
					t.Fatalf("expected to read %d keys but only read %d", numRows, numKeysRead)
				}
			}()
		}
	})

	t.Run("DeDupingRowContainer", func(t *testing.T) {
		numCols := 2
		numRows := 10
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
		// Use random types and random rows.
		types := randgen.RandSortingTypes(rng, numCols)
		numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
		d := MakeDiskRowContainer(diskMonitor, types, ordering, tempEngine)
		defer d.Close(ctx)
		d.DoDeDuplicate()
		addRowsRepeatedly := func() {
			// Add some number of de-duplicated rows using AddRow() to exercise the
			// code path in DiskRowContainer that gets exercised by
			// DiskBackedRowContainer when it spills from memory to disk.
			addRowCalls := rng.Intn(numRows)
			for i := 0; i < addRowCalls; i++ {
				require.NoError(t, d.AddRow(ctx, rows[i]))
				require.Equal(t, d.bufferedRows.NumPutsSinceFlush(), len(d.deDupCache))
			}
			// Repeatedly add the same set of rows.
			for i := 0; i < 3; i++ {
				if i == 2 && rng.Intn(2) == 0 {
					// Clear the de-dup cache so that a SortedDiskMapIterator is needed
					// to de-dup.
					d.testingFlushBuffer(ctx)
				}
				for j := 0; j < numRows; j++ {
					idx, err := d.AddRowWithDeDup(ctx, rows[j])
					require.NoError(t, err)
					require.Equal(t, j, idx)
				}
			}
		}
		addRowsRepeatedly()
		// Reset and add the rows in a different order.
		require.NoError(t, d.UnsafeReset(ctx))
		rand.Shuffle(len(rows), func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		addRowsRepeatedly()
	})

	t.Run("NumberedRowIterator", func(t *testing.T) {
		numCols := 2
		numRows := 10
		// Use random types and random rows.
		types := randgen.RandSortingTypes(rng, numCols)
		rows := randgen.RandEncDatumRowsOfTypes(rng, numRows, types)
		// There are no ordering columns when using the numberedRowIterator.
		d := MakeDiskRowContainer(diskMonitor, types, nil, tempEngine)
		defer d.Close(ctx)
		for i := 0; i < numRows; i++ {
			require.NoError(t, d.AddRow(ctx, rows[i]))
		}
		require.NotEqual(t, 0, d.MeanEncodedRowBytes())
		iter := d.newNumberedIterator(ctx)
		defer iter.Close()
		// Checks equality of rows[index] and the current position of iter.
		checkEq := func(index int) {
			valid, err := iter.Valid()
			require.True(t, valid)
			require.NoError(t, err)
			row, err := iter.Row()
			require.NoError(t, err)
			require.Equal(t, rows[index].String(types), row.String(types))
		}
		for i := 0; i < numRows; i++ {
			// Seek to a random position and iterate until the end.
			index := rng.Intn(numRows)
			iter.seekToIndex(index)
			checkEq(index)
			for index++; index < numRows; index++ {
				iter.Next()
				checkEq(index)
			}
		}
	})
}

// makeUniqueRows can return a row count < numRows (always > 0 when numRows >
// 0), hence it also returns the actual returned count (to remind the caller).
func makeUniqueRows(
	t *testing.T,
	evalCtx *tree.EvalContext,
	rng *rand.Rand,
	numRows int,
	types []*types.T,
	ordering colinfo.ColumnOrdering,
) (int, rowenc.EncDatumRows) {
	rows := randgen.RandEncDatumRowsOfTypes(rng, numRows, types)
	// It is possible there was some duplication, so remove duplicates.
	var alloc rowenc.DatumAlloc
	sort.Slice(rows, func(i, j int) bool {
		cmp, err := rows[i].Compare(types, &alloc, ordering, evalCtx, rows[j])
		require.NoError(t, err)
		return cmp < 0
	})
	deDupedRows := rows[:1]
	for i := 1; i < len(rows); i++ {
		cmp, err := rows[i].Compare(types, &alloc, ordering, evalCtx, deDupedRows[len(deDupedRows)-1])
		require.NoError(t, err)
		if cmp != 0 {
			deDupedRows = append(deDupedRows, rows[i])
		}
	}
	rows = deDupedRows
	// Shuffle so that not adding in sorted order.
	rand.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})
	return len(rows), rows
}

func TestDiskRowContainerDiskFull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// Make a monitor with no capacity.
	monitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(0 /* capacity */))

	d := MakeDiskRowContainer(
		monitor,
		[]*types.T{types.Int},
		colinfo.ColumnOrdering{colinfo.ColumnOrderInfo{ColIdx: 0, Direction: encoding.Ascending}},
		tempEngine,
	)
	defer d.Close(ctx)

	row := rowenc.EncDatumRow{rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(1)))}
	err = d.AddRow(ctx, row)
	if code := pgerror.GetPGCode(err); code != pgcode.DiskFull {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiskRowContainerFinalIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	alloc := &rowenc.DatumAlloc{}
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	d := MakeDiskRowContainer(diskMonitor, types.OneIntCol, nil /* ordering */, tempEngine)
	defer d.Close(ctx)

	const numCols = 1
	const numRows = 100
	rows := randgen.MakeIntRows(numRows, numCols)
	for _, row := range rows {
		if err := d.AddRow(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	// checkEqual checks that the given row is equal to otherRow.
	checkEqual := func(row rowenc.EncDatumRow, otherRow rowenc.EncDatumRow) error {
		for j, c := range row {
			if cmp, err := c.Compare(types.Int, alloc, &evalCtx, &otherRow[j]); err != nil {
				return err
			} else if cmp != 0 {
				return fmt.Errorf(
					"unexpected row %v, expected %v",
					row.String(types.OneIntCol),
					otherRow.String(types.OneIntCol),
				)
			}
		}
		return nil
	}

	rowsRead := 0
	func() {
		i := d.NewFinalIterator(ctx)
		defer i.Close()
		for i.Rewind(); rowsRead < numRows/2; i.Next() {
			if ok, err := i.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				t.Fatalf("unexpectedly reached the end after %d rows read", rowsRead)
			}
			row, err := i.Row()
			if err != nil {
				t.Fatal(err)
			}
			if err := checkEqual(row, rows[rowsRead]); err != nil {
				t.Fatal(err)
			}
			rowsRead++
		}
	}()

	// Verify resumability.
	func() {
		i := d.NewFinalIterator(ctx)
		defer i.Close()
		for i.Rewind(); ; i.Next() {
			if ok, err := i.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				break
			}
			row, err := i.Row()
			if err != nil {
				t.Fatal(err)
			}
			if err := checkEqual(row, rows[rowsRead]); err != nil {
				t.Fatal(err)
			}
			rowsRead++
		}
	}()

	if rowsRead != len(rows) {
		t.Fatalf("only read %d rows, expected %d", rowsRead, len(rows))
	}

	// Add a couple extra rows to check that they're picked up by the iterator.
	extraRows := randgen.MakeIntRows(4, 1)
	for _, row := range extraRows {
		if err := d.AddRow(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	i := d.NewFinalIterator(ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			t.Fatal(err)
		}
		if err := checkEqual(row, extraRows[rowsRead-len(rows)]); err != nil {
			t.Fatal(err)
		}
		rowsRead++
	}

	if rowsRead != len(rows)+len(extraRows) {
		t.Fatalf("only read %d rows, expected %d", rowsRead, len(rows)+len(extraRows))
	}
}

func TestDiskRowContainerUnsafeReset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	monitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))

	d := MakeDiskRowContainer(monitor, types.OneIntCol, nil /* ordering */, tempEngine)
	defer d.Close(ctx)

	const (
		numCols = 1
		numRows = 100
	)
	rows := randgen.MakeIntRows(numRows, numCols)

	const (
		numResets            = 4
		expectedRowsPerReset = numRows / numResets
	)
	for i := 0; i < numResets; i++ {
		if err := d.UnsafeReset(ctx); err != nil {
			t.Fatal(err)
		}
		if d.Len() != 0 {
			t.Fatalf("disk row container still contains %d row(s) after a reset", d.Len())
		}
		firstRow := rows[0]
		for _, row := range rows[:len(rows)/numResets] {
			if err := d.AddRow(ctx, row); err != nil {
				t.Fatal(err)
			}
		}
		// Verify that the first row matches up.
		func() {
			i := d.NewFinalIterator(ctx)
			defer i.Close()
			i.Rewind()
			if ok, err := i.Valid(); err != nil || !ok {
				t.Fatalf("unexpected i.Valid() return values: ok=%t, err=%s", ok, err)
			}
			row, err := i.Row()
			if err != nil {
				t.Fatal(err)
			}
			if row.String(types.OneIntCol) != firstRow.String(types.OneIntCol) {
				t.Fatalf("unexpected row read %s, expected %s", row.String(types.OneIntCol), firstRow.String(types.OneIntCol))
			}
		}()

		// diskRowFinalIterator does not actually discard rows, so Len() should
		// still account for the row we just read.
		if d.Len() != expectedRowsPerReset {
			t.Fatalf("expected %d rows but got %d", expectedRowsPerReset, d.Len())
		}
	}

	// Verify we read the expected number of rows (note that we already read one
	// in the last iteration of the numResets loop).
	i := d.NewFinalIterator(ctx)
	defer i.Close()
	rowsRead := 0
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		_, err := i.Row()
		if err != nil {
			t.Fatal(err)
		}
		rowsRead++
	}
	if rowsRead != expectedRowsPerReset-1 {
		t.Fatalf("read %d rows using a final iterator but expected %d", rowsRead, expectedRowsPerReset)
	}
}
