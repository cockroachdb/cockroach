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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// compareRows compares l and r according to a column ordering. Returns -1 if
// l < r, 0 if l == r, and 1 if l > r. If an error is returned the int returned
// is invalid. Note that the comparison is only performed on the ordering
// columns.
func compareRows(
	lTypes []types.T,
	l, r sqlbase.EncDatumRow,
	e *tree.EvalContext,
	d *sqlbase.DatumAlloc,
	ordering sqlbase.ColumnOrdering,
) (int, error) {
	for _, orderInfo := range ordering {
		col := orderInfo.ColIdx
		cmp, err := l[col].Compare(&lTypes[col], d, e, &r[orderInfo.ColIdx])
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
	tempEngine, err := engine.NewTempEngine(engine.TestStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These orderings assume at least 4 columns.
	numCols := 4
	orderings := []sqlbase.ColumnOrdering{
		{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Descending,
			},
		},
		{
			sqlbase.ColumnOrderInfo{
				ColIdx:    3,
				Direction: encoding.Ascending,
			},
			sqlbase.ColumnOrderInfo{
				ColIdx:    1,
				Direction: encoding.Descending,
			},
			sqlbase.ColumnOrderInfo{
				ColIdx:    2,
				Direction: encoding.Ascending,
			},
		},
	}

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	evalCtx := tree.MakeTestingEvalContext(st)
	diskMonitor := mon.MakeMonitor(
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
				typs := make([]types.T, numCols)
				for i := range typs {
					typs[i] = *sqlbase.RandSortingType(rng)
				}
				row := sqlbase.RandEncDatumRowOfTypes(rng, typs)
				func() {
					d := MakeDiskRowContainer(&diskMonitor, typs, ordering, tempEngine)
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
					readRow := make(sqlbase.EncDatumRow, len(row))

					temp, err := i.Row()
					if err != nil {
						t.Fatal(err)
					}
					copy(readRow, temp)

					// Ensure the datum fields are set and no errors occur when
					// decoding.
					for i, encDatum := range readRow {
						if err := encDatum.EnsureDecoded(&typs[i], &d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check equality of the row we wrote and the row we read.
					for i := range row {
						if cmp, err := readRow[i].Compare(&typs[i], &d.datumAlloc, &evalCtx, &row[i]); err != nil {
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
			types := sqlbase.RandSortingTypes(rng, numCols)
			rows := sqlbase.RandEncDatumRowsOfTypes(rng, numRows, types)
			func() {
				d := MakeDiskRowContainer(&diskMonitor, types, ordering, tempEngine)
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
						if err := encDatum.EnsureDecoded(&types[i], &d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check sorted order.
					if cmp, err := compareRows(
						types, sortedRows.EncRow(numKeysRead), row, &evalCtx, &d.datumAlloc, ordering,
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
}

func TestDiskRowContainerDiskFull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(engine.TestStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// Make a monitor with no capacity.
	monitor := mon.MakeMonitor(
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
		&monitor,
		[]types.T{*types.Int},
		sqlbase.ColumnOrdering{sqlbase.ColumnOrderInfo{ColIdx: 0, Direction: encoding.Ascending}},
		tempEngine,
	)
	defer d.Close(ctx)

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(1)))}
	err = d.AddRow(ctx, row)
	if code := pgerror.GetPGCode(err); code != pgcode.DiskFull {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiskRowContainerFinalIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	alloc := &sqlbase.DatumAlloc{}
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, err := engine.NewTempEngine(engine.TestStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	diskMonitor := mon.MakeMonitor(
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

	d := MakeDiskRowContainer(&diskMonitor, sqlbase.OneIntCol, nil /* ordering */, tempEngine)
	defer d.Close(ctx)

	const numCols = 1
	const numRows = 100
	rows := sqlbase.MakeIntRows(numRows, numCols)
	for _, row := range rows {
		if err := d.AddRow(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	// checkEqual checks that the given row is equal to otherRow.
	checkEqual := func(row sqlbase.EncDatumRow, otherRow sqlbase.EncDatumRow) error {
		for j, c := range row {
			if cmp, err := c.Compare(types.Int, alloc, &evalCtx, &otherRow[j]); err != nil {
				return err
			} else if cmp != 0 {
				return fmt.Errorf(
					"unexpected row %v, expected %v",
					row.String(sqlbase.OneIntCol),
					otherRow.String(sqlbase.OneIntCol),
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
	extraRows := sqlbase.MakeIntRows(4, 1)
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
	tempEngine, err := engine.NewTempEngine(engine.TestStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	monitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))

	d := MakeDiskRowContainer(&monitor, sqlbase.OneIntCol, nil /* ordering */, tempEngine)
	defer d.Close(ctx)

	const (
		numCols = 1
		numRows = 100
	)
	rows := sqlbase.MakeIntRows(numRows, numCols)

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
			if row.String(sqlbase.OneIntCol) != firstRow.String(sqlbase.OneIntCol) {
				t.Fatalf("unexpected row read %s, expected %s", row.String(sqlbase.OneIntCol), firstRow.String(sqlbase.OneIntCol))
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
