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
//
// Author: Alfonso Subiotto Marques (alfonso@cockroachlabs.com)

package distsqlrun

import (
	"math/rand"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// compareRows compares l and r according to a column ordering. Returns -1 if
// l < r, 0 if l == r, and 1 if l > r. If an error is returned the int returned
// is invalid. Note that the comparison is only performed on the ordering
// columns.
func compareRows(
	l, r sqlbase.EncDatumRow,
	e *parser.EvalContext,
	d *sqlbase.DatumAlloc,
	ordering sqlbase.ColumnOrdering,
) (int, error) {
	for _, orderInfo := range ordering {
		cmp, err := l[orderInfo.ColIdx].Compare(d, e, &r[orderInfo.ColIdx])
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
	tempEngine, err := engine.NewTempEngine(ctx, base.DefaultTestStoreSpec)
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

	rng := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))

	evalCtx := parser.MakeTestingEvalContext()
	t.Run("EncodeDecode", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			// Test with different orderings so that we have a mix of key and
			// value encodings.
			for _, ordering := range orderings {
				types := make([]sqlbase.ColumnType, numCols)
				for i := range types {
					types[i] = sqlbase.RandColumnType(rng)
				}
				row := sqlbase.EncDatumRow(sqlbase.RandEncDatumSliceOfTypes(rng, types))
				func() {
					d, err := makeDiskRowContainer(
						ctx, types, ordering, memRowContainer{}, tempEngine,
					)
					if err != nil {
						t.Fatal(err)
					}
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
					for _, encDatum := range readRow {
						if err := encDatum.EnsureDecoded(&d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check equality of the row we wrote and the row we read.
					for i := range row {
						if cmp, err := readRow[i].Compare(&d.datumAlloc, &evalCtx, &row[i]); err != nil {
							t.Fatal(err)
						} else if cmp != 0 {
							t.Fatalf("encoded %s but decoded %s", row, readRow)
						}
					}
				}()
			}
		}
	})

	t.Run("SortedOrder", func(t *testing.T) {
		numRows := 1024
		for _, ordering := range orderings {
			// numRows rows with numCols columns of the same random type.
			rows := sqlbase.RandEncDatumRows(rng, numRows, numCols)
			types := make([]sqlbase.ColumnType, len(rows[0]))
			for i := range types {
				types[i] = rows[0][i].Type
			}
			func() {
				// Make the diskRowContainer with half of these rows and insert
				// the other half normally.
				memoryContainer := makeRowContainer(ordering, types, &evalCtx)
				defer memoryContainer.Close(ctx)
				midIdx := len(rows) / 2
				for i := 0; i < midIdx; i++ {
					if err := memoryContainer.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}

				d, err := makeDiskRowContainer(
					ctx,
					types,
					ordering,
					memoryContainer,
					tempEngine,
				)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close(ctx)
				for i := midIdx; i < len(rows); i++ {
					if err := d.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}

				// Make another row container that stores all the rows then sort
				// it to compare equality.
				sortedRows := makeRowContainer(ordering, types, &evalCtx)
				defer sortedRows.Close(ctx)
				for _, row := range rows {
					if err := sortedRows.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}
				}
				sortedRows.Sort()

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
					for _, encDatum := range row {
						if err := encDatum.EnsureDecoded(&d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check sorted order.
					if cmp, err := compareRows(
						sortedRows.EncRow(numKeysRead), row, &evalCtx, &d.datumAlloc, ordering,
					); err != nil {
						t.Fatal(err)
					} else if cmp != 0 {
						t.Fatalf(
							"expected %s to be equal to %s",
							row,
							sortedRows.EncRow(numKeysRead),
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
