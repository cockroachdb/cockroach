// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestHashRouter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numCols = 6
	const numRows = 200

	rng, _ := randutil.NewPseudoRand()
	alloc := &sqlbase.DatumAlloc{}

	// Generate tables of possible values for each column; we have fewer possible
	// values than rows to guarantee many occurrences of each value.
	// TODO(radu): we don't support collated strings yet, as equal strings can
	// have different key encodings. When we support them, we should revert to
	// using RandEncDatumSlices.
	vals := make([][]sqlbase.EncDatum, numCols)
	for i := range vals {
		for {
			vals[i] = sqlbase.RandEncDatumSlice(rng, numRows/10)
			// If we generated collated string, try again.
			if vals[i][0].Type.Kind != sqlbase.ColumnType_COLLATEDSTRING {
				break
			}
		}
	}

	testCases := []struct {
		hashColumns []uint32
		numBuckets  int
	}{
		{[]uint32{0}, 2},
		{[]uint32{3}, 4},
		{[]uint32{1, 3}, 4},
		{[]uint32{5, 2}, 3},
		{[]uint32{0, 1, 2, 3, 4}, 5},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			bufs := make([]*RowBuffer, tc.numBuckets)
			recvs := make([]RowReceiver, tc.numBuckets)
			for i := 0; i < tc.numBuckets; i++ {
				bufs[i] = &RowBuffer{}
				recvs[i] = bufs[i]
			}
			hr, err := makeHashRouter(tc.hashColumns, recvs)
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < numRows; i++ {
				row := make(sqlbase.EncDatumRow, numCols)
				for j := 0; j < numCols; j++ {
					row[j] = vals[j][rng.Intn(len(vals[j]))]
				}
				if status := hr.PushRow(RowOrMetadata{Row: row}); status != NeedMoreRows {
					t.Fatalf("unexpected status: %d", status)
				}
			}
			hr.ProducerDone(nil)

			rows := make([]sqlbase.EncDatumRows, len(bufs))
			for i, b := range bufs {
				if !b.ProducerClosed {
					t.Fatalf("bucket not closed: %d", i)
				}
				if b.Err != nil {
					t.Fatal(b.Err)
				}
				rows[i] = getRowsFromBuffer(t, b)
			}

			for bIdx := range rows {
				for _, row := range rows[bIdx] {
					// Verify there are no rows that
					//  - have the same values with this row on all the hashColumns, and
					//  - ended up in a different bucket
					for b2Idx, r2 := range rows {
						if b2Idx == bIdx {
							continue
						}
						for _, row2 := range r2 {
							equal := true
							for _, c := range tc.hashColumns {
								cmp, err := row[c].Compare(alloc, &row2[c])
								if err != nil {
									t.Fatal(err)
								}
								if cmp != 0 {
									equal = false
									break
								}
							}
							if equal {
								t.Errorf("rows %s and %s in different buckets", row, row2)
							}
						}
					}
				}
			}
		})
	}
}

func getRowsFromBuffer(t *testing.T, buf *RowBuffer) sqlbase.EncDatumRows {
	var res sqlbase.EncDatumRows
	for {
		row, err := buf.NextRow()
		if err != nil {
			t.Fatal(err)
		}
		if row.Metadata != nil {
			t.Fatalf("unexpected metadata: %v", row)
		}
		if row.Empty() {
			break
		}
		res = append(res, row.Row)
	}
	return res
}

func TestMirrorRouter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numCols = 6
	const numRows = 20

	rng, _ := randutil.NewPseudoRand()
	alloc := &sqlbase.DatumAlloc{}

	vals := sqlbase.RandEncDatumSlices(rng, numCols, numRows)

	for numBuckets := 2; numBuckets <= 4; numBuckets++ {
		bufs := make([]*RowBuffer, numBuckets)
		recvs := make([]RowReceiver, numBuckets)
		for i := 0; i < numBuckets; i++ {
			bufs[i] = &RowBuffer{}
			recvs[i] = bufs[i]
		}
		mr, err := makeMirrorRouter(recvs)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < numRows; i++ {
			row := make(sqlbase.EncDatumRow, numCols)
			for j := 0; j < numCols; j++ {
				row[j] = vals[j][rng.Intn(len(vals[j]))]
			}
			if status := mr.PushRow(RowOrMetadata{Row: row}); status != NeedMoreRows {
				t.Fatalf("unexpected status: %d", status)
			}
		}
		mr.ProducerDone(nil)

		rows := make([]sqlbase.EncDatumRows, len(bufs))
		for i, b := range bufs {
			if !b.ProducerClosed {
				t.Fatalf("bucket not closed: %d", i)
			}
			if b.Err != nil {
				t.Fatal(b.Err)
			}
			rows[i] = getRowsFromBuffer(t, b)
		}

		// Verify each row is sent to each of the output streams.
		for bIdx, r := range rows {
			if bIdx == 0 {
				continue
			}
			if len(rows[bIdx]) != len(rows[0]) {
				t.Errorf("buckets %d and %d have different number of rows", 0, bIdx)
			}

			// Verify that the i-th row is the same across all buffers.
			for i, row := range r {
				row2 := rows[0][i]

				equal := true
				for j, c := range row {
					cmp, err := c.Compare(alloc, &row2[j])
					if err != nil {
						t.Fatal(err)
					}
					if cmp != 0 {
						equal = false
						break
					}
				}
				if !equal {
					t.Errorf("rows %s and %s found in one bucket and not the other", row, row2)
				}
			}
		}
	}
}
