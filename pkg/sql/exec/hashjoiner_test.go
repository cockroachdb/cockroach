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

package exec

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testSource struct {
	t         []types.T
	columns   []column
	index     uint64
	batchSize uint16
	batchId   int
	nRows     uint64
	nColumns  int
}

var _ Operator = &testSource{}

func (source *testSource) Init() {
	source.nColumns = len(source.columns)
	if source.nColumns == 0 || len(source.t) != source.nColumns {
		panic(fmt.Sprintf("testSource columns length must be equal to t length and cannot be 0"))
	}
}

func (source *testSource) Next() ColBatch {
	batch := NewMemBatch(source.t...)

	if source.index >= source.nRows {
		return batch
	}

	if source.index+uint64(source.batchSize) < source.nRows {
		batch.SetLength(source.batchSize)
	} else {
		batch.SetLength(uint16(source.nRows - source.index))
	}

	n := batch.Length()

	for colId := 0; colId < source.nColumns; colId++ {
		switch source.t[colId] {
		case types.Int8:
			vec := batch.ColVec(colId).Int8()
			col := source.columns[colId].([]int8)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec[rowId] = col[uint64(rowId)+source.index]
			}
			break
		case types.Int16:
			vec := batch.ColVec(colId).Int16()
			col := source.columns[colId].([]int16)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec[rowId] = col[uint64(rowId)+source.index]
			}
			break
		case types.Int32:
			vec := batch.ColVec(colId).Int32()
			col := source.columns[colId].([]int32)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec[rowId] = col[uint64(rowId)+source.index]
			}
			break
		case types.Int64:
			vec := batch.ColVec(colId).Int64()
			col := source.columns[colId].([]int64)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec[rowId] = col[uint64(rowId)+source.index]
			}
			break
		case types.Float32:
			vec := batch.ColVec(colId).Float32()
			col := source.columns[colId].([]float32)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec[rowId] = col[uint64(rowId)+source.index]
			}
			break
		case types.Float64:
			vec := batch.ColVec(colId).Float64()
			col := source.columns[colId].([]float64)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec[rowId] = col[uint64(rowId)+source.index]
			}
			break
		case types.Bool:
			vec := batch.ColVec(colId).Bool()
			col := source.columns[colId].([]bool)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec.Set(rowId, col[uint64(rowId)+source.index])
			}
			break
		case types.Bytes:
			vec := batch.ColVec(colId).Bytes()
			col := source.columns[colId].([][]byte)
			for rowId := uint16(0); rowId < n; rowId++ {
				vec.Set(rowId, col[uint64(rowId)+source.index])
			}
			break
		default:
			panic(fmt.Sprintf("unhandled type %d", source.t[colId]))
		}
	}

	source.index += uint64(n)

	return batch
}

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	leftSource := &testSource{
		t: []types.T{types.Bool, types.Int64, types.Int32, types.Int16},
		columns: []column{
			[]bool{false, true, false, false},
			[]int64{5, 3, 6, 2},
			[]int32{40, 80, 90, 10},
			[]int16{10, 30, 20, 50},
		},
		batchSize: 3,
		nRows:     4,
	}

	rightSource := &testSource{
		t: []types.T{types.Int64, types.Float32, types.Int8},
		columns: []column{
			[]int64{1, 2, 3, 4, 5},
			[]float32{1.1, 2.2, 3.3, 4.4, 5.5},
			[]int8{1, 2, 4, 8, 16},
		},
		batchSize: 2,
		nRows:     5,
	}

	spec := &hashJoinerSpec{
		leftEqCol:  1,
		rightEqCol: 0,

		leftOutCols:  []int{1, 2},
		rightOutCols: []int{0, 2},

		leftSourceTypes:  leftSource.t,
		rightSourceTypes: rightSource.t,
	}

	nOutCols := len(spec.leftOutCols) + len(spec.rightOutCols)
	expectedTypes := make([]types.T, nOutCols)
	for i, colId := range spec.leftOutCols {
		expectedTypes[i] = spec.leftSourceTypes[colId]
	}
	for i, colId := range spec.rightOutCols {
		expectedTypes[i+len(spec.leftOutCols)] = spec.rightSourceTypes[colId]
	}

	expectedSource := &testSource{
		t: expectedTypes,
		columns: []column{
			[]int64{2, 3, 5},
			[]int32{10, 80, 40},
			[]int64{2, 3, 5},
			[]int8{2, 4, 16},
		},
		nRows: 3,
	}

	hj := &hashJoinerInt64{
		leftSource:  leftSource,
		rightSource: rightSource,
		spec:        spec,
	}

	hj.Init()
	expectedSource.Init()

	for {
		actualBatch := hj.Next()

		if actualBatch.Length() == 0 {
			break
		}
		expectedSource.batchSize = actualBatch.Length()
		expectedBatch := expectedSource.Next()

		if !reflect.DeepEqual(expectedBatch, actualBatch) {
			t.Errorf("expected %v\ngot %v", expectedBatch, actualBatch)
		}
	}
}
