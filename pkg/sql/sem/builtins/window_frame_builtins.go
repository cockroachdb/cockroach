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
	"context"

	"bytes"

	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// MaybeReplaceWithFasterImplementation replaces an aggregate with more efficient one, if present.
func MaybeReplaceWithFasterImplementation(
	windowFunc tree.WindowFunc, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) tree.WindowFunc {
	if framableAgg, ok := windowFunc.(*framableAggregateWindowFunc); ok {
		aggWindowFunc := framableAgg.agg.agg
		switch w := aggWindowFunc.(type) {
		case *MinAggregate:
			min := &slidingWindowFunc{}
			min.sw = &slidingWindow{
				values: make([]indexedValue, 0, wfr.PartitionSize()),
				cmp: func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return -a.Compare(evalCtx, b)
				},
			}
			return min
		case *MaxAggregate:
			max := &slidingWindowFunc{}
			max.sw = &slidingWindow{
				values: make([]indexedValue, 0, wfr.PartitionSize()),
				cmp: func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				},
			}
			return max
		case *intSumAggregate:
			return &slidingWindowSumFunc{agg: aggWindowFunc}
		case *decimalSumAggregate:
			return &slidingWindowSumFunc{agg: aggWindowFunc}
		case *floatSumAggregate:
			return &slidingWindowSumFunc{agg: aggWindowFunc}
		case *avgAggregate:
			// w.agg is a sum aggregate.
			return &avgWindowFunc{sum: slidingWindowSumFunc{agg: w.agg}}
		}
	}
	return windowFunc
}

// indexedValue combines a value from the row with the index of that row.
type indexedValue struct {
	value tree.Datum
	idx   int
}

// slidingWindow maintains a deque of values along with corresponding indices
// based on cmp function:
// for Min behavior, cmp = -a.Compare(b)
// for Max behavior, cmp = a.Compare(b)
//
// It assumes that the frame bounds will never go back, i.e. non-decreasing sequences
// of frame start and frame end indices.
type slidingWindow struct {
	values []indexedValue
	cmp    func(*tree.EvalContext, tree.Datum, tree.Datum) int
}

// add first removes all values that are "smaller or equal" (depending on cmp)
// from the end of the deque and then appends 'iv' to the end. This way, the deque
// always contains unique values sorted in descending order of their "priority"
// (when we encounter duplicates, we always keep the one with the largest idx).
func (sw *slidingWindow) add(evalCtx *tree.EvalContext, iv indexedValue) {
	var newEndIdx int
	for newEndIdx = len(sw.values) - 1; newEndIdx >= 0; newEndIdx-- {
		if sw.cmp(evalCtx, sw.values[newEndIdx].value, iv.value) > 0 {
			break
		}
	}
	sw.values = sw.values[:newEndIdx+1]
	sw.values = append(sw.values, iv)
}

// removeAllBefore removes all values from the beginning of the deque that have indices
// smaller than given 'idx'.
// This operation corresponds to shifting the start of the frame up to 'idx'.
func (sw *slidingWindow) removeAllBefore(idx int) {
	var newStartIdx int
	for newStartIdx = 0; newStartIdx < len(sw.values) && newStartIdx < idx; newStartIdx++ {
		if sw.values[newStartIdx].idx >= idx {
			break
		}
	}
	sw.values = sw.values[newStartIdx:]
}

func (sw *slidingWindow) string() string {
	var buf bytes.Buffer
	for i := 0; i < len(sw.values); i++ {
		buf.WriteString(fmt.Sprintf("(%v, %v)\t", sw.values[i].value, sw.values[i].idx))
	}
	return buf.String()
}

type slidingWindowFunc struct {
	sw      *slidingWindow
	prevEnd int
}

// Compute implements WindowFunc interface.
func (w *slidingWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	start, end := wfr.FrameStartIdx(), wfr.FrameEndIdx()

	// We need to discard all values that are no longer in the frame.
	w.sw.removeAllBefore(start)

	// We need to add all values that just entered the frame and have not been added yet.
	for idx := max(w.prevEnd, start); idx < end; idx++ {
		w.sw.add(evalCtx, indexedValue{wfr.ArgsByRowIdx(idx)[0], idx})
	}
	w.prevEnd = end

	if len(w.sw.values) == 0 {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	// The datum with "highest priority" within the frame is at the very front of the deque.
	return w.sw.values[0].value, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Close implements WindowFunc interface.
func (w *slidingWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.sw = nil
}

// slidingWindowSumFunc applies sliding window approach to summation over a frame.
// It assumes that the frame bounds will never go back, i.e. non-decreasing sequences
// of frame start and frame end indices.
type slidingWindowSumFunc struct {
	agg                tree.AggregateFunc // one of the three SumAggregates
	prevStart, prevEnd int
}

// removeAllBefore subtracts the values from all the rows that are no longer in the frame.
func (w *slidingWindowSumFunc) removeAllBefore(
	ctx context.Context, wfr *tree.WindowFrameRun,
) error {
	for idx := w.prevStart; idx < wfr.FrameStartIdx() && idx < w.prevEnd; idx++ {
		value := wfr.ArgsByRowIdx(idx)[0]
		switch v := value.(type) {
		case *tree.DInt:
			return w.agg.Add(ctx, tree.NewDInt(-*v))
		case *tree.DDecimal:
			d := tree.DDecimal{}
			d.Neg(&v.Decimal)
			return w.agg.Add(ctx, &d)
		case *tree.DFloat:
			return w.agg.Add(ctx, tree.NewDFloat(-*v))
		default:
			return pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected value %v", v)
		}
	}
	return nil
}

// Compute implements WindowFunc interface.
func (w *slidingWindowSumFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	start, end := wfr.FrameStartIdx(), wfr.FrameEndIdx()

	// We need to discard all values that are no longer in the frame.
	err := w.removeAllBefore(ctx, wfr)
	if err != nil {
		return tree.DNull, err
	}

	// We need to sum all values that just entered the frame and have not been added yet.
	for idx := max(w.prevEnd, start); idx < end; idx++ {
		err = w.agg.Add(ctx, wfr.ArgsByRowIdx(idx)[0])
		if err != nil {
			return tree.DNull, err
		}
	}

	w.prevStart = start
	w.prevEnd = end
	return w.agg.Result()
}

// Close implements WindowFunc interface.
func (w *slidingWindowSumFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.agg.Close(ctx)
}

// avgWindowFunc uses slidingWindowSumFunc to compute average over a frame.
type avgWindowFunc struct {
	sum slidingWindowSumFunc
}

// Compute implements WindowFunc interface.
func (w *avgWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if wfr.FrameSize() == 0 {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	var sum tree.Datum
	var err error
	sum, err = w.sum.Compute(ctx, evalCtx, wfr)
	if err != nil {
		return nil, err
	}

	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(wfr.FrameSize())), nil
	case *tree.DDecimal:
		var avg tree.DDecimal
		count := apd.New(int64(wfr.FrameSize()), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &t.Decimal, count)
		return &avg, err
	case *tree.DInt:
		dd := tree.DDecimal{}
		dd.SetCoefficient(int64(*t))
		var avg tree.DDecimal
		count := apd.New(int64(wfr.FrameSize()), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &dd.Decimal, count)
		return &avg, err
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected SUM result type: %s", t)
	}
}

// Close implements WindowFunc interface.
func (w *avgWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.sum.Close(ctx, evalCtx)
}
