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

package distsql

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func TestHashRouter(t *testing.T) {
	const numCols = 6
	const numRows = 200

	rng, _ := randutil.NewPseudoRand()
	alloc := &sqlbase.DatumAlloc{}

	// Generate tables of possible values for each column; we have fewer possible
	// values than rows to guarantee many occurrences of each value.
	vals := make([][]sqlbase.EncDatum, numCols)
	for i := 0; i < numCols; i++ {
		typ := sqlbase.RandColumnType(rng)
		vals[i] = make([]sqlbase.EncDatum, numRows/10)
		for j := range vals[i] {
			vals[i][j].SetDatum(typ, sqlbase.RandDatum(rng, typ, true))
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
			ok := hr.PushRow(row)
			if !ok {
				t.Fatalf("PushRow returned false")
			}
		}
		hr.Close(nil)
		for bIdx, b := range bufs {
			if !b.closed {
				t.Errorf("bucket not closed")
			}
			if b.err != nil {
				t.Error(b.err)
			}

			for _, row := range b.rows {
				// Verify there are no rows that
				//  - have the same values with this row on all the hashColumns, and
				//  - ended up in a different bucket
				for b2Idx, b2 := range bufs {
					if b2Idx == bIdx {
						continue
					}
					for _, row2 := range b2.rows {
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
	}
}
