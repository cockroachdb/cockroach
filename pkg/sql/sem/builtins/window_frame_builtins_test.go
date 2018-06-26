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

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const maxCount = 1000
const maxInt = 1000000
const maxOffset = 100

func testSlidingWindow(t *testing.T, count int) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	wfr := makeTestWindowFrameRun(count)
	wfr.Frame = &tree.WindowFrame{
		Mode: tree.ROWS,
		Bounds: tree.WindowFrameBounds{
			StartBound: &tree.WindowFrameBound{BoundType: tree.ValuePreceding},
			EndBound:   &tree.WindowFrameBound{BoundType: tree.ValueFollowing},
		},
	}
	testMin(t, evalCtx, wfr)
	testMax(t, evalCtx, wfr)
	testSumAndAvg(t, evalCtx, wfr)
}

func testMin(t *testing.T, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = offset
		wfr.EndBoundOffset = offset
		min := &slidingWindowFunc{}
		min.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
			return -a.Compare(evalCtx, b)
		})
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			res, err := min.Compute(evalCtx.Ctx(), evalCtx, wfr)
			if err != nil {
				t.Errorf("Unexpected error received when getting min from sliding window: %+v", err)
			}
			minResult, _ := tree.AsDInt(res)
			naiveMin := tree.DInt(maxInt)
			for idx := wfr.RowIdx - offset; idx <= wfr.RowIdx+offset; idx++ {
				if idx < 0 || idx >= wfr.PartitionSize() {
					continue
				}
				el, _ := tree.AsDInt(wfr.Rows[idx].Row[0])
				if el < naiveMin {
					naiveMin = el
				}
			}
			if minResult != naiveMin {
				t.Errorf("Min sliding window returned wrong result: expected %+v, found %+v", naiveMin, minResult)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(min.sw.string())
				t.Errorf(partitionToString(wfr.Rows))
				panic("")
			}
		}
	}
}

func testMax(t *testing.T, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = offset
		wfr.EndBoundOffset = offset
		max := &slidingWindowFunc{}
		max.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
			return a.Compare(evalCtx, b)
		})
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			res, err := max.Compute(evalCtx.Ctx(), evalCtx, wfr)
			if err != nil {
				t.Errorf("Unexpected error received when getting max from sliding window: %+v", err)
			}
			maxResult, _ := tree.AsDInt(res)
			naiveMax := tree.DInt(-maxInt)
			for idx := wfr.RowIdx - offset; idx <= wfr.RowIdx+offset; idx++ {
				if idx < 0 || idx >= wfr.PartitionSize() {
					continue
				}
				el, _ := tree.AsDInt(wfr.Rows[idx].Row[0])
				if el > naiveMax {
					naiveMax = el
				}
			}
			if maxResult != naiveMax {
				t.Errorf("Max sliding window returned wrong result: expected %+v, found %+v", naiveMax, maxResult)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(max.sw.string())
				t.Errorf(partitionToString(wfr.Rows))
				panic("")
			}
		}
	}
}

func testSumAndAvg(t *testing.T, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = offset
		wfr.EndBoundOffset = offset
		sum := &slidingWindowSumFunc{agg: &intSumAggregate{}}
		avg := &avgWindowFunc{sum: slidingWindowSumFunc{agg: &intSumAggregate{}}}
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			res, err := sum.Compute(evalCtx.Ctx(), evalCtx, wfr)
			if err != nil {
				t.Errorf("Unexpected error received when getting sum from sliding window: %+v", err)
			}
			sumResult := tree.DDecimal{Decimal: res.(*tree.DDecimal).Decimal}
			res, err = avg.Compute(evalCtx.Ctx(), evalCtx, wfr)
			if err != nil {
				t.Errorf("Unexpected error received when getting avg from sliding window: %+v", err)
			}
			avgResult := tree.DDecimal{Decimal: res.(*tree.DDecimal).Decimal}
			naiveSum := int64(0)
			for idx := wfr.RowIdx - offset; idx <= wfr.RowIdx+offset; idx++ {
				if idx < 0 || idx >= wfr.PartitionSize() {
					continue
				}
				el, _ := tree.AsDInt(wfr.Rows[idx].Row[0])
				naiveSum += int64(el)
			}
			s, err := sumResult.Int64()
			if err != nil {
				t.Errorf("Unexpected error received when converting sum from DDecimal to int64: %+v", err)
			}
			if s != naiveSum {
				t.Errorf("Sum sliding window returned wrong result: expected %+v, found %+v", naiveSum, s)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(partitionToString(wfr.Rows))
				panic("")
			}
			a, err := avgResult.Float64()
			if err != nil {
				t.Errorf("Unexpected error received when converting avg from DDecimal to float64: %+v", err)
			}
			if a != float64(naiveSum)/float64(wfr.FrameSize()) {
				t.Errorf("Sum sliding window returned wrong result: expected %+v, found %+v", float64(naiveSum)/float64(wfr.FrameSize()), a)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(partitionToString(wfr.Rows))
				panic("")
			}
		}
	}
}

func makeTestWindowFrameRun(count int) *tree.WindowFrameRun {
	return &tree.WindowFrameRun{
		Rows:        makeTestPartition(count),
		ArgIdxStart: 0,
		ArgCount:    1,
	}
}

func makeTestPartition(count int) []tree.IndexedRow {
	partition := make([]tree.IndexedRow, count)
	for idx := 0; idx < count; idx++ {
		partition[idx] = tree.IndexedRow{Idx: idx, Row: tree.Datums{tree.NewDInt(tree.DInt(rand.Int31n(maxInt)))}}
	}
	return partition
}

func partitionToString(partition []tree.IndexedRow) string {
	var buf bytes.Buffer
	buf.WriteString("\n=====Partition=====\n")
	for idx := 0; idx < len(partition); idx++ {
		buf.WriteString(fmt.Sprintf("%v\n", partition[idx]))
	}
	buf.WriteString("====================\n")
	return buf.String()
}

func testRingBuffer(t *testing.T, count int) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	partition := makeTestPartition(count)
	ring := ringBuffer{}
	naiveBuffer := make([]*indexedValue, 0, count)
	for idx, row := range partition {
		if ring.len() != len(naiveBuffer) {
			t.Errorf("Ring ring returned incorrect len: expected %v, found %v", len(naiveBuffer), ring.len())
			panic("")
		}

		op := rand.Float64()
		if op < 0.5 {
			iv := &indexedValue{idx: idx, value: row.Row[0]}
			ring.add(iv)
			naiveBuffer = append(naiveBuffer, iv)
		} else if op < 0.75 {
			if len(naiveBuffer) > 0 {
				ring.removeHead()
				naiveBuffer = naiveBuffer[1:]
			}
		} else {
			if len(naiveBuffer) > 0 {
				ring.removeTail()
				naiveBuffer = naiveBuffer[:len(naiveBuffer)-1]
			}
		}

		for pos, iv := range naiveBuffer {
			res := ring.get(pos)
			if res.idx != iv.idx || res.value.Compare(evalCtx, iv.value) != 0 {
				t.Errorf("Ring buffer returned incorrect value: expected %+v, found %+v", iv, res)
				panic("")
			}
		}
	}
}

func TestSlidingWindow(t *testing.T) {
	for count := 1; count <= maxCount; count += int(rand.Int31n(maxCount / 10)) {
		testSlidingWindow(t, count)
	}
}

func TestRingBuffer(t *testing.T) {
	for count := 1; count <= maxCount; count++ {
		testRingBuffer(t, count)
	}
}
