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
	"fmt"
	"strings"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
)

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
// It assumes that the frame bounds will never go back, i.e. non-decreasing
// sequences of frame start and frame end indices.
type slidingWindow struct {
	values  ring.Buffer
	evalCtx *tree.EvalContext
	cmp     func(*tree.EvalContext, tree.Datum, tree.Datum) int
}

func makeSlidingWindow(
	evalCtx *tree.EvalContext, cmp func(*tree.EvalContext, tree.Datum, tree.Datum) int,
) *slidingWindow {
	return &slidingWindow{
		evalCtx: evalCtx,
		cmp:     cmp,
	}
}

// add first removes all values that are "smaller or equal" (depending on cmp)
// from the end of the deque and then appends 'iv' to the end. This way, the
// deque always contains unique values sorted in descending order of their
// "priority" (when we encounter duplicates, we always keep the one with the
// largest idx).
func (sw *slidingWindow) add(iv *indexedValue) {
	for i := sw.values.Len() - 1; i >= 0; i-- {
		if sw.cmp(sw.evalCtx, sw.values.Get(i).(*indexedValue).value, iv.value) > 0 {
			break
		}
		sw.values.RemoveLast()
	}
	sw.values.AddLast(iv)
}

// removeAllBefore removes all values from the beginning of the deque that have
// indices smaller than given 'idx'. This operation corresponds to shifting the
// start of the frame up to 'idx'.
func (sw *slidingWindow) removeAllBefore(idx int) {
	for i := 0; i < sw.values.Len() && i < idx; i++ {
		if sw.values.Get(i).(*indexedValue).idx >= idx {
			break
		}
		sw.values.RemoveFirst()
	}
}

func (sw *slidingWindow) string() string {
	var builder strings.Builder
	for i := 0; i < sw.values.Len(); i++ {
		builder.WriteString(fmt.Sprintf("(%v, %v)\t", sw.values.Get(i).(*indexedValue).value, sw.values.Get(i).(*indexedValue).idx))
	}
	return builder.String()
}

type slidingWindowFunc struct {
	sw      *slidingWindow
	prevEnd int
}

// Compute implements WindowFunc interface.
func (w *slidingWindowFunc) Compute(
	_ context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	start, end := wfr.FrameStartIdx(evalCtx), wfr.FrameEndIdx(evalCtx)

	// We need to discard all values that are no longer in the frame.
	w.sw.removeAllBefore(start)

	// We need to add all values that just entered the frame and have not been
	// added yet.
	for idx := max(w.prevEnd, start); idx < end; idx++ {
		if wfr.FilterColIdx != noFilterIdx && wfr.Rows.GetRow(idx).GetDatum(wfr.FilterColIdx) != tree.DBoolTrue {
			continue
		}
		args := wfr.ArgsByRowIdx(idx)
		value := args[0]
		if value == tree.DNull {
			// Null value can neither be minimum nor maximum over a window frame with
			// non-null values, so we're not adding them to the sliding window. The
			// case of a window frame with no non-null values is handled below.
			continue
		}
		w.sw.add(&indexedValue{value: value, idx: idx})
	}
	w.prevEnd = end

	if w.sw.values.Len() == 0 {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	// The datum with "highest priority" within the frame is at the very front
	// of the deque.
	return w.sw.values.GetFirst().(*indexedValue).value, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Close implements WindowFunc interface.
func (w *slidingWindowFunc) Close(context.Context, *tree.EvalContext) {
	w.sw = nil
}

// slidingWindowSumFunc applies sliding window approach to summation over
// a frame. It assumes that the frame bounds will never go back, i.e.
// non-decreasing sequences of frame start and frame end indices.
type slidingWindowSumFunc struct {
	agg                tree.AggregateFunc // one of the four SumAggregates
	prevStart, prevEnd int

	// lastNonNullIdx is the index of the latest non-null value seen in the
	// sliding window so far. noNonNullSeen indicates non-null values are yet to
	// be seen.
	lastNonNullIdx int
}

const noNonNullSeen = -1

func newSlidingWindowSumFunc(agg tree.AggregateFunc) *slidingWindowSumFunc {
	return &slidingWindowSumFunc{
		agg:            agg,
		lastNonNullIdx: noNonNullSeen,
	}
}

// removeAllBefore subtracts the values from all the rows that are no longer in
// the frame.
func (w *slidingWindowSumFunc) removeAllBefore(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) error {
	var err error
	for idx := w.prevStart; idx < wfr.FrameStartIdx(evalCtx) && idx < w.prevEnd; idx++ {
		if wfr.FilterColIdx != noFilterIdx && wfr.Rows.GetRow(idx).GetDatum(wfr.FilterColIdx) != tree.DBoolTrue {
			continue
		}
		value := wfr.ArgsByRowIdx(idx)[0]
		if value == tree.DNull {
			// Null values do not contribute to the running sum, so there is nothing
			// to subtract once they leave the window frame.
			continue
		}
		switch v := value.(type) {
		case *tree.DInt:
			err = w.agg.Add(ctx, tree.NewDInt(-*v))
		case *tree.DDecimal:
			d := tree.DDecimal{}
			d.Neg(&v.Decimal)
			err = w.agg.Add(ctx, &d)
		case *tree.DFloat:
			err = w.agg.Add(ctx, tree.NewDFloat(-*v))
		case *tree.DInterval:
			err = w.agg.Add(ctx, &tree.DInterval{Duration: duration.Duration{}.Sub(v.Duration)})
		default:
			err = pgerror.NewAssertionErrorf("unexpected value %v", v)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Compute implements WindowFunc interface.
func (w *slidingWindowSumFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	start, end := wfr.FrameStartIdx(evalCtx), wfr.FrameEndIdx(evalCtx)

	// We need to discard all values that are no longer in the frame.
	err := w.removeAllBefore(ctx, evalCtx, wfr)
	if err != nil {
		return tree.DNull, err
	}

	// We need to sum all values that just entered the frame and have not been
	// added yet.
	for idx := max(w.prevEnd, start); idx < end; idx++ {
		if wfr.FilterColIdx != noFilterIdx && wfr.Rows.GetRow(idx).GetDatum(wfr.FilterColIdx) != tree.DBoolTrue {
			continue
		}
		value := wfr.ArgsByRowIdx(idx)[0]
		if value != tree.DNull {
			w.lastNonNullIdx = idx
			err = w.agg.Add(ctx, value)
			if err != nil {
				return nil, err
			}
		}
	}

	w.prevStart = start
	w.prevEnd = end
	// If last non-null value has index smaller than the start of the window
	// frame, then only nulls can be in the frame. This holds true as well for
	// the special noNonNullsSeen index.
	onlyNulls := w.lastNonNullIdx < start
	if start == end || onlyNulls {
		// Either the window frame is empty or only null values are in the frame,
		// so we return NULL as per spec.
		return tree.DNull, nil
	}
	return w.agg.Result()
}

// Close implements WindowFunc interface.
func (w *slidingWindowSumFunc) Close(ctx context.Context, _ *tree.EvalContext) {
	w.agg.Close(ctx)
}

// avgWindowFunc uses slidingWindowSumFunc to compute average over a frame.
type avgWindowFunc struct {
	sum *slidingWindowSumFunc
}

// Compute implements WindowFunc interface.
func (w *avgWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	sum, err := w.sum.Compute(ctx, evalCtx, wfr)
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	frameSize := 0
	frameStartIdx := wfr.FrameStartIdx(evalCtx)
	frameEndIdx := wfr.FrameEndIdx(evalCtx)
	for idx := frameStartIdx; idx < frameEndIdx; idx++ {
		if wfr.FilterColIdx != noFilterIdx {
			row := wfr.Rows.GetRow(idx)
			datum := row.GetDatum(wfr.FilterColIdx)
			if datum != tree.DBoolTrue {
				continue
			}
		}

		value := wfr.ArgsByRowIdx(idx)[0]
		if value == tree.DNull {
			// Null values do not count towards the number of rows that contribute
			// to the sum, so we're omitting them from the frame.
			continue
		}
		frameSize++
	}

	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(frameSize)), nil
	case *tree.DDecimal:
		var avg tree.DDecimal
		count := apd.New(int64(frameSize), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &t.Decimal, count)
		return &avg, err
	case *tree.DInt:
		dd := tree.DDecimal{}
		dd.SetCoefficient(int64(*t))
		var avg tree.DDecimal
		count := apd.New(int64(frameSize), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &dd.Decimal, count)
		return &avg, err
	default:
		return nil, pgerror.NewAssertionErrorf("unexpected SUM result type: %s", t)
	}
}

// Close implements WindowFunc interface.
func (w *avgWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.sum.Close(ctx, evalCtx)
}
