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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// checkEqualRows compares l and r, returning true if l == r and 0 if not. If
// the returned error is not nil, the returned boolean is invalid.
func checkEqualRows(
	l, r sqlbase.EncDatumRow, e *parser.EvalContext, d *sqlbase.DatumAlloc,
) (bool, error) {
	if len(l) != len(r) {
		return false, nil
	}
	for i := range l {
		if cmp, err := l[i].Compare(d, e, &r[i]); err != nil {
			return false, err
		} else if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func TestDiskRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, err := newTestingRocksDB()
	if err != nil {
		t.Fatal(err)
	}
	defer closeRocks(r)

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

	ctx := context.Background()
	evalCtx := parser.MakeTestingEvalContext()
	t.Run("EncodeDecode", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			// Test with different orderings so that we have a mix of key and
			// value encodings.
			for _, ordering := range orderings {
				row := sqlbase.EncDatumRow(sqlbase.RandEncDatumSlice(rng, numCols))
				types := make([]sqlbase.ColumnType, len(row))
				for i := range types {
					types[i] = row[i].Type
				}
				func() {
					d, err := makeDiskRowContainer(
						ctx, 0 /* processorID */, types, ordering, memRowContainer{}, r,
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
					if !i.Valid() {
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
					for _, encDatum := range row {
						if err := encDatum.EnsureDecoded(&d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					if equal, err := checkEqualRows(
						readRow, row, &evalCtx, &d.datumAlloc,
					); err != nil {
						t.Fatal(err)
					} else if !equal {
						t.Fatalf("encoded %s but decoded %s", row, readRow)
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
					0, /* processorID */
					types,
					ordering,
					memoryContainer,
					r,
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
				for i.Rewind(); i.Valid(); i.Next() {
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
					if equal, err := checkEqualRows(
						sortedRows.EncRow(numKeysRead), row, &evalCtx, &d.datumAlloc,
					); err != nil {
						t.Fatal(err)
					} else if !equal {
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
