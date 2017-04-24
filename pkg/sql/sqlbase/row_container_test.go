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

package sql

import (
	"math"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, numCols := range []int{1, 2, 3, 5, 10, 15} {
		for _, numRows := range []int{5, 10, 100} {
			for _, numPops := range []int{0, 1, 2, numRows / 3, numRows / 2} {
				resCol := make(ResultColumns, numCols)
				for i := range resCol {
					resCol[i] = ResultColumn{Typ: parser.TypeInt}
				}
				m := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, math.MaxInt64)
				rc := NewRowContainer(m.MakeBoundAccount(), resCol, 0)
				row := make(parser.Datums, numCols)
				for i := 0; i < numRows; i++ {
					for j := range row {
						row[j] = parser.NewDInt(parser.DInt(i*numCols + j))
					}
					if _, err := rc.AddRow(context.Background(), row); err != nil {
						t.Fatal(err)
					}
				}

				for i := 0; i < numPops; i++ {
					rc.PopFirst()
				}

				// Given that we just deleted numPops rows, we have numRows -
				// numPops rows remaining.
				if rc.Len() != numRows-numPops {
					t.Fatalf("invalid length, expected %d got %d", numRows-numPops, rc.Len())
				}

				// what was previously rc.At(i + numPops) is now rc.At(i).
				for i := 0; i < rc.Len(); i++ {
					row := rc.At(i)
					for j := range row {
						dint, ok := parser.AsDInt(row[j])
						if !ok || int(dint) != (i+numPops)*numCols+j {
							t.Fatalf("invalid value %+v on row %d, col %d", row[j], i+numPops, j)
						}
					}
				}
			}
		}
	}
}
