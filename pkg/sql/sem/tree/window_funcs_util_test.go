// Copyright 2018 The Cockroach Authors.
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

package tree

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

const maxCount = 1000
const maxInt = 1000000

type indexedRows struct {
	rows []indexedRow
}

func (ir indexedRows) Len() int {
	return len(ir.rows)
}

func (ir indexedRows) GetRow(idx int) IndexedRow {
	return ir.rows[idx]
}

type indexedRow struct {
	idx int
	row Datums
}

func (ir indexedRow) GetIdx() int {
	return ir.idx
}

func (ir indexedRow) GetDatum(colIdx int) Datum {
	return ir.row[colIdx]
}

func (ir indexedRow) GetDatums(firstColIdx, lastColIdx int) Datums {
	return ir.row[firstColIdx:lastColIdx]
}

func makeTestPartition(count int) IndexedRows {
	partition := indexedRows{rows: make([]indexedRow, count)}
	for idx := 0; idx < count; idx++ {
		partition.rows[idx] = indexedRow{idx: idx, row: Datums{NewDInt(DInt(rand.Int31n(maxInt)))}}
	}
	return partition
}

func testRingBuffer(t *testing.T, count int) {
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	partition := makeTestPartition(count)
	ring := RingBuffer{}
	naiveBuffer := make([]*IndexedValue, 0, count)
	for rowIdx := 0; rowIdx < partition.Len(); rowIdx++ {
		if ring.Len() != len(naiveBuffer) {
			t.Errorf("Ring buffer returned incorrect Len: expected %v, found %v", len(naiveBuffer), ring.Len())
			panic("")
		}

		op := rand.Float64()
		if op < 0.5 {
			iv := &IndexedValue{Idx: rowIdx, Value: partition.GetRow(rowIdx).GetDatum(0)}
			ring.Add(iv)
			naiveBuffer = append(naiveBuffer, iv)
		} else if op < 0.75 {
			if len(naiveBuffer) > 0 {
				ring.RemoveHead()
				naiveBuffer = naiveBuffer[1:]
			}
		} else {
			if len(naiveBuffer) > 0 {
				ring.RemoveTail()
				naiveBuffer = naiveBuffer[:len(naiveBuffer)-1]
			}
		}

		for pos, iv := range naiveBuffer {
			res := ring.Get(pos)
			if res.Idx != iv.Idx || res.Value.Compare(evalCtx, iv.Value) != 0 {
				t.Errorf("Ring buffer returned incorrect value: expected %+v, found %+v", iv, res)
				panic("")
			}
		}
	}
}

func TestRingBuffer(t *testing.T) {
	for count := 1; count <= maxCount; count++ {
		testRingBuffer(t, count)
	}
}
