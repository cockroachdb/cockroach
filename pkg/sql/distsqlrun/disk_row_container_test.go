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
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// compareRows compares l and r, returning -1 if l < r, 0 if l == r, and 1
// if l > r. If an error is returned the int returned is invalid.
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

	r, err := newTestingRocksDB()
	if err != nil {
		t.Fatal(err)
	}
	defer closeRocks(r)

	// These orderings assume at least 4 columns.
	numCols := 4
	orderings := []sqlbase.ColumnOrdering{
		sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Descending,
			},
		},
		sqlbase.ColumnOrdering{
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

	rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	ctx := context.Background()
	evalCtx := parser.MakeTestingEvalContext()
	t.Run("EncodeDecode", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			// Test with different orderings so that we have a mix of key and
			// value encodings.
			for _, ordering := range orderings {
				row := sqlbase.RandEncDatumSlice(rng, numCols)
				types := make([]sqlbase.ColumnType, len(row))
				for i := range types {
					types[i] = row[i].Type
				}
				func() {
					d, err := makeDiskRowContainer(
						ctx, 0 /* processorId */, types, ordering, memRowContainer{}, r,
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
					copy(readRow, i.Row(ctx))
					if cmp, err := compareRows(readRow, row, &evalCtx, &d.datumAlloc, ordering); err != nil {
						t.Fatal(err)
					} else if cmp != 0 {
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
					0, /* processorId */
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

				i := d.NewIterator(ctx)
				defer i.Close()
				i.Rewind()
				if !i.Valid() {
					t.Fatal("unexpectedly invalid")
				}
				// Copy because row is only valid until next call to Row().
				lastRow := make(sqlbase.EncDatumRow, numCols)
				copy(lastRow, i.Row(ctx))
				i.Next()

				numKeysRead := 1
				for ; i.Valid(); i.Next() {
					row := i.Row(ctx)

					// Force the decoding to check correct encoding/decoding.
					if err := sqlbase.EncDatumRowToDatums(
						make(parser.Datums, len(types)), row, &d.datumAlloc,
					); err != nil {
						t.Fatal(err)
					}

					// Check sorted order.
					cmp, err := compareRows(lastRow, row, &evalCtx, &d.datumAlloc, ordering)
					if err != nil {
						t.Fatal(err)
					} else if cmp > 0 {
						t.Fatalf("expected %s to be greater than or equal to %s", row, lastRow)
					}
					copy(lastRow, row)
					numKeysRead++
				}
				if numKeysRead != numRows {
					t.Fatalf("expected to read %d keys but only read %d", numRows, numKeysRead)
				}
			}()
		}
	})
}
