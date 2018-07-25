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

package distsqlrun

import (
	"context"
	math "math"
	"math/rand"
	"testing"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
func distinctRows(
	lTypes []sqlbase.ColumnType,
	l, r sqlbase.EncDatumRow,
	e *tree.EvalContext,
	d *sqlbase.DatumAlloc,
	ordering sqlbase.ColumnOrdering,
) (bool, error) {
	for _, orderInfo := range ordering {
		col := orderInfo.ColIdx
		cmp, err := l[col].Distinct(&lTypes[col], d, e, &r[orderInfo.ColIdx])
		if err != nil {
			return false, err
		}
		if cmp {
			return true, nil
		}
	}
	return false, nil
}

func TestDiskRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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
				types := make([]sqlbase.ColumnType, numCols)
				for i := range types {
					types[i] = sqlbase.RandSortingColumnType(rng)
				}
				row := sqlbase.RandEncDatumRowOfTypes(rng, types)
				func() {
					d := makeDiskRowContainer(&diskMonitor, types, ordering, tempEngine)
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
						if err := encDatum.EnsureDecoded(&types[i], &d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check equality of the row we wrote and the row we read.
					for i := range row {
						if cmp, err := readRow[i].Distinct(&types[i], &d.datumAlloc, &evalCtx, &row[i]); err != nil {
							t.Fatal(err)
						} else if cmp {
							t.Fatalf("encoded %s but decoded %s", row.String(types), readRow.String(types))
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
			types := sqlbase.RandSortingColumnTypes(rng, numCols)
			rows := sqlbase.RandEncDatumRowsOfTypes(rng, numRows, types)
			func() {
				d := makeDiskRowContainer(&diskMonitor, types, ordering, tempEngine)
				defer d.Close(ctx)
				for i := 0; i < len(rows); i++ {
					if err := d.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}

				// Make another row container that stores all the rows then sort
				// it to compare equality.
				var sortedRows memRowContainer
				sortedRows.init(ordering, types, &evalCtx)
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
					if cmp, err := distinctRows(
						types, sortedRows.EncRow(numKeysRead), row, &evalCtx, &d.datumAlloc, ordering,
					); err != nil {
						t.Fatal(err)
					} else if cmp {
						t.Fatalf(
							"expected %s to be not distinct from %s",
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
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	d := makeDiskRowContainer(
		&monitor,
		[]sqlbase.ColumnType{columnTypeInt},
		sqlbase.ColumnOrdering{sqlbase.ColumnOrderInfo{ColIdx: 0, Direction: encoding.Ascending}},
		tempEngine,
	)
	defer d.Close(ctx)

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(1)))}
	err = d.AddRow(ctx, row)
	if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeDiskFullError) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiskRowContainerFinalIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	alloc := &sqlbase.DatumAlloc{}
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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

	d := makeDiskRowContainer(&diskMonitor, oneIntCol, nil /* ordering */, tempEngine)
	defer d.Close(ctx)

	const numCols = 1
	const numRows = 100
	rows := makeIntRows(numRows, numCols)
	for _, row := range rows {
		if err := d.AddRow(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	// checkEqual checks that the given row is equal to otherRow.
	checkEqual := func(row sqlbase.EncDatumRow, otherRow sqlbase.EncDatumRow) error {
		for j, c := range row {
			if cmp, err := c.Distinct(&intType, alloc, &evalCtx, &otherRow[j]); err != nil {
				return err
			} else if cmp {
				return fmt.Errorf(
					"unexpected row %v, expected %v",
					row.String(oneIntCol),
					otherRow.String(oneIntCol),
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
	extraRows := makeIntRows(4, 1)
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
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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

	d := makeDiskRowContainer(&monitor, oneIntCol, nil /* ordering */, tempEngine)
	defer d.Close(ctx)

	const (
		numCols = 1
		numRows = 100
	)
	rows := makeIntRows(numRows, numCols)

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
			if row.String(oneIntCol) != firstRow.String(oneIntCol) {
				t.Fatalf("unexpected row read %s, expected %s", row.String(oneIntCol), firstRow.String(oneIntCol))
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
