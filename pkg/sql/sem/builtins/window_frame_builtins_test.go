// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const maxInt = 1000000
const maxOffset = 100

type indexedRows struct {
	rows []indexedRow
}

func (ir indexedRows) Len() int {
	return len(ir.rows)
}

func (ir indexedRows) GetRow(_ context.Context, idx int) (tree.IndexedRow, error) {
	return ir.rows[idx], nil
}

type indexedRow struct {
	idx int
	row tree.Datums
}

func (ir indexedRow) GetIdx() int {
	return ir.idx
}

func (ir indexedRow) GetDatum(colIdx int) (tree.Datum, error) {
	return ir.row[colIdx], nil
}

func (ir indexedRow) GetDatums(firstColIdx, lastColIdx int) (tree.Datums, error) {
	return ir.row[firstColIdx:lastColIdx], nil
}

func testSlidingWindow(t *testing.T, count int) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	wfr := makeTestWindowFrameRun(count)
	wfr.Frame = &tree.WindowFrame{
		Mode: tree.ROWS,
		Bounds: tree.WindowFrameBounds{
			StartBound: &tree.WindowFrameBound{BoundType: tree.OffsetPreceding},
			EndBound:   &tree.WindowFrameBound{BoundType: tree.OffsetFollowing},
		},
	}
	testMin(t, evalCtx, wfr)
	testMax(t, evalCtx, wfr)
	testSumAndAvg(t, evalCtx, wfr)
}

func testMin(t *testing.T, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = tree.NewDInt(tree.DInt(offset))
		wfr.EndBoundOffset = tree.NewDInt(tree.DInt(offset))
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
				row, err := wfr.Rows.GetRow(evalCtx.Ctx(), idx)
				if err != nil {
					panic(err)
				}
				datum, err := row.GetDatum(0)
				if err != nil {
					panic(err)
				}
				el, _ := tree.AsDInt(datum)
				if el < naiveMin {
					naiveMin = el
				}
			}
			if minResult != naiveMin {
				t.Errorf("Min sliding window returned wrong result: expected %+v, found %+v", naiveMin, minResult)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(min.sw.string())
				t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
				panic("")
			}
		}
	}
}

func testMax(t *testing.T, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = tree.NewDInt(tree.DInt(offset))
		wfr.EndBoundOffset = tree.NewDInt(tree.DInt(offset))
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
				row, err := wfr.Rows.GetRow(evalCtx.Ctx(), idx)
				if err != nil {
					panic(err)
				}
				datum, err := row.GetDatum(0)
				if err != nil {
					panic(err)
				}
				el, _ := tree.AsDInt(datum)
				if el > naiveMax {
					naiveMax = el
				}
			}
			if maxResult != naiveMax {
				t.Errorf("Max sliding window returned wrong result: expected %+v, found %+v", naiveMax, maxResult)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(max.sw.string())
				t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
				panic("")
			}
		}
	}
}

func testSumAndAvg(t *testing.T, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = tree.NewDInt(tree.DInt(offset))
		wfr.EndBoundOffset = tree.NewDInt(tree.DInt(offset))
		sum := &slidingWindowSumFunc{agg: &intSumAggregate{}}
		avg := &avgWindowFunc{sum: newSlidingWindowSumFunc(&intSumAggregate{})}
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
				row, err := wfr.Rows.GetRow(evalCtx.Ctx(), idx)
				if err != nil {
					panic(err)
				}
				datum, err := row.GetDatum(0)
				if err != nil {
					panic(err)
				}
				el, _ := tree.AsDInt(datum)
				naiveSum += int64(el)
			}
			s, err := sumResult.Int64()
			if err != nil {
				t.Errorf("Unexpected error received when converting sum from DDecimal to int64: %+v", err)
			}
			if s != naiveSum {
				t.Errorf("Sum sliding window returned wrong result: expected %+v, found %+v", naiveSum, s)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
				panic("")
			}
			a, err := avgResult.Float64()
			if err != nil {
				t.Errorf("Unexpected error received when converting avg from DDecimal to float64: %+v", err)
			}
			frameSize, err := wfr.FrameSize(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("Unexpected error when getting FrameSize: %+v", err)
			}
			if a != float64(naiveSum)/float64(frameSize) {
				t.Errorf("Sum sliding window returned wrong result: expected %+v, found %+v", float64(naiveSum)/float64(frameSize), a)
				t.Errorf("partitionSize: %+v idx: %+v offset: %+v", wfr.PartitionSize(), wfr.RowIdx, offset)
				t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
				panic("")
			}
		}
	}
}

func makeTestWindowFrameRun(count int) *tree.WindowFrameRun {
	return &tree.WindowFrameRun{
		Rows:         makeTestPartition(count),
		ArgsIdxs:     []uint32{0},
		FilterColIdx: tree.NoColumnIdx,
	}
}

func makeTestPartition(count int) tree.IndexedRows {
	partition := indexedRows{rows: make([]indexedRow, count)}
	for idx := 0; idx < count; idx++ {
		partition.rows[idx] = indexedRow{idx: idx, row: tree.Datums{tree.NewDInt(tree.DInt(rand.Int31n(maxInt)))}}
	}
	return partition
}

func partitionToString(ctx context.Context, partition tree.IndexedRows) string {
	var buf bytes.Buffer
	var err error
	var row tree.IndexedRow
	buf.WriteString("\n=====Partition=====\n")
	for idx := 0; idx < partition.Len(); idx++ {
		if row, err = partition.GetRow(ctx, idx); err != nil {
			return err.Error()
		}
		buf.WriteString(fmt.Sprintf("%v\n", row))
	}
	buf.WriteString("====================\n")
	return buf.String()
}

func TestSlidingWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, count := range []int{1, 17, 253} {
		testSlidingWindow(t, count)
	}
}
