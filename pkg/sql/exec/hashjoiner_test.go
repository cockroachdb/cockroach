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

type testOp struct {
	t         []types.T
	columns   []column
	index     uint64
	batchSize uint16
	nRows     uint64
	nColumns  int
}

var _ Operator = &testOp{}

func (source *testOp) Init() {
	source.nColumns = len(source.columns)
	if source.nColumns == 0 || len(source.t) != source.nColumns {
		panic(fmt.Sprintf("testOp columns length must be equal to t length and cannot be 0"))
	}
}

func (source *testOp) Next() ColBatch {
	batch := NewMemBatch(source.t)

	if source.index >= source.nRows {
		return batch
	}

	if source.index+uint64(source.batchSize) < source.nRows {
		batch.SetLength(source.batchSize)
	} else {
		batch.SetLength(uint16(source.nRows - source.index))
	}

	n := batch.Length()

	for colID := 0; colID < source.nColumns; colID++ {
		switch source.t[colID] {
		case types.Int8:
			vec := batch.ColVec(colID).Int8()
			col := source.columns[colID].([]int8)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Int16:
			vec := batch.ColVec(colID).Int16()
			col := source.columns[colID].([]int16)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Int32:
			vec := batch.ColVec(colID).Int32()
			col := source.columns[colID].([]int32)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Int64:
			vec := batch.ColVec(colID).Int64()
			col := source.columns[colID].([]int64)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Float32:
			vec := batch.ColVec(colID).Float32()
			col := source.columns[colID].([]float32)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Float64:
			vec := batch.ColVec(colID).Float64()
			col := source.columns[colID].([]float64)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Bool:
			vec := batch.ColVec(colID).Bool()
			col := source.columns[colID].([]bool)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		case types.Bytes:
			vec := batch.ColVec(colID).Bytes()
			col := source.columns[colID].([][]byte)
			for rowID := uint16(0); rowID < n; rowID++ {
				vec[rowID] = col[uint64(rowID)+source.index]
			}
			break
		default:
			panic(fmt.Sprintf("unhandled type %d", source.t[colID]))
		}
	}

	source.index += uint64(n)

	return batch
}

func TestHashJoinerInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		leftTypes  []types.T
		rightTypes []types.T

		leftColumns  []column
		rightColumns []column

		leftBatchSize  uint16
		rightBatchSize uint16

		nLeftRows  uint64
		nRightRows uint64

		leftEqCol    int
		rightEqCol   int
		leftOutCols  []int
		rightOutCols []int

		expectedColumns []column
		nExpectedRows   uint64
	}{
		{
			leftTypes:  []types.T{types.Bool, types.Int64, types.Int32, types.Int16},
			rightTypes: []types.T{types.Int64, types.Float32, types.Int8},

			leftColumns: []column{
				[]bool{false, true, false, false},
				[]int64{5, 3, 6, 2},
				[]int32{40, 80, 90, 10},
				[]int16{10, 30, 20, 50},
			},
			rightColumns: []column{
				[]int64{1, 2, 3, 4, 5},
				[]float32{1.1, 2.2, 3.3, 4.4, 5.5},
				[]int8{1, 2, 4, 8, 16},
			},

			leftBatchSize:  3,
			rightBatchSize: 4,

			nLeftRows:  4,
			nRightRows: 5,

			leftEqCol:    1,
			rightEqCol:   0,
			leftOutCols:  []int{1, 2},
			rightOutCols: []int{0, 2},

			expectedColumns: []column{
				[]int64{2, 3, 5},
				[]int32{10, 80, 40},
				[]int64{2, 3, 5},
				[]int8{2, 4, 16},
			},
			nExpectedRows: 3,
		},
	}

	for _, tc := range tcs {
		leftSource := &testOp{
			t:         tc.leftTypes,
			columns:   tc.leftColumns,
			nRows:     tc.nLeftRows,
			batchSize: tc.leftBatchSize,
		}

		rightSource := &testOp{
			t:         tc.rightTypes,
			columns:   tc.rightColumns,
			nRows:     tc.nRightRows,
			batchSize: tc.rightBatchSize,
		}

		spec := &hashJoinerSpec{
			leftSourceTypes:  tc.leftTypes,
			rightSourceTypes: tc.rightTypes,

			leftEqCol:  tc.leftEqCol,
			rightEqCol: tc.rightEqCol,

			leftOutCols:  tc.leftOutCols,
			rightOutCols: tc.rightOutCols,
		}

		nOutCols := len(spec.leftOutCols) + len(spec.rightOutCols)
		expectedTypes := make([]types.T, nOutCols)
		for i, colID := range spec.leftOutCols {

			expectedTypes[i] = spec.leftSourceTypes[colID]
		}
		for i, colID := range spec.rightOutCols {
			expectedTypes[i+len(spec.leftOutCols)] = spec.rightSourceTypes[colID]
		}

		expectedSource := &testOp{
			t:       expectedTypes,
			columns: tc.expectedColumns,
			nRows:   tc.nExpectedRows,
		}

		hj := &hashJoinerInt64Op{
			leftSource:  leftSource,
			rightSource: rightSource,
			spec:        spec,
		}

		hj.Init()
		expectedSource.Init()

		for {
			actualBatch := hj.Next()
			batchSize := actualBatch.Length()

			if batchSize == 0 {
				break
			}

			expectedSource.batchSize = batchSize
			expectedBatch := expectedSource.Next()

			for i := 0; i < expectedSource.nColumns; i++ {
				expectedCol := expectedBatch.ColVec(i).Slice(batchSize, expectedSource.t[i])
				actualCol := actualBatch.ColVec(i).Slice(batchSize, expectedSource.t[i])
				if !reflect.DeepEqual(expectedCol, actualCol) {
					t.Errorf("expected %v\ngot %v", expectedCol, actualCol)
				}
			}
		}
	}
}
