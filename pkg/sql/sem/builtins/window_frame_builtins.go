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
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}

	if !wfr.DefaultFrameExclusion() {
		// We cannot use sliding window approach because we have frame exclusion
		// clause - some rows will be in and out of the frame which breaks
		// the necessary assumption, so we fallback to naive quadratic approach.
		var res tree.Datum
		for idx := frameStartIdx; idx < frameEndIdx; idx++ {
			if wfr.FilterColIdx != noFilterIdx {
				row, err := wfr.Rows.GetRow(ctx, idx)
				if err != nil {
					return nil, err
				}
				d, err := row.GetDatum(wfr.FilterColIdx)
				if err != nil {
					return nil, err
				}
				if d != tree.DBoolTrue {
					// Row idx is filtered out from the window frame, so we skip it.
					continue
				}
			}
			if wfr.IsRowExcluded(idx) {
				// Row idx is excluded from the window frame, so we skip it.
				continue
			}
			args, err := wfr.ArgsByRowIdx(ctx, idx)
			if err != nil {
				return nil, err
			}
			if res == nil {
				res = args[0]
			} else {
				if w.sw.cmp(evalCtx, args[0], res) > 0 {
					res = args[0]
				}
			}
		}
		if res == nil {
			// Spec: the frame is empty, so we return NULL.
			return tree.DNull, nil
		}
		return res, nil
	}

	// We need to discard all values that are no longer in the frame.
	w.sw.removeAllBefore(frameStartIdx)

	// We need to add all values that just entered the frame and have not been
	// added yet.
	for idx := max(w.prevEnd, frameStartIdx); idx < frameEndIdx; idx++ {
		if wfr.FilterColIdx != noFilterIdx {
			row, err := wfr.Rows.GetRow(ctx, idx)
			if err != nil {
				return nil, err
			}
			datum, err := row.GetDatum(wfr.FilterColIdx)
			if err != nil {
				return nil, err
			}
			if datum != tree.DBoolTrue {
				// Row idx is filtered out from the window frame, so we skip it.
				continue
			}
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return nil, err
		}
		w.sw.add(&indexedValue{value: args[0], idx: idx})
	}
	w.prevEnd = frameEndIdx

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
}

// removeAllBefore subtracts the values from all the rows that are no longer in
// the frame.
func (w *slidingWindowSumFunc) removeAllBefore(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) error {
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return err
	}
	for idx := w.prevStart; idx < frameStartIdx && idx < w.prevEnd; idx++ {
		if wfr.FilterColIdx != noFilterIdx {
			row, err := wfr.Rows.GetRow(ctx, idx)
			if err != nil {
				return err
			}
			datum, err := row.GetDatum(wfr.FilterColIdx)
			if err != nil {
				return err
			}
			if datum != tree.DBoolTrue {
				// Row idx is filtered out from the window frame, so we skip it.
				continue
			}
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return err
		}
		value := args[0]
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
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	if !wfr.DefaultFrameExclusion() {
		// We cannot use sliding window approach because we have frame exclusion
		// clause - some row or rows will be in and out of the frame which breaks
		// the necessary assumption, so we fallback to naive quadratic approach.
		var sum tree.Datum
		for idx := frameStartIdx; idx < frameEndIdx; idx++ {
			if wfr.IsRowExcluded(idx) {
				// Row idx is excluded from the window frame, so we skip it.
				continue
			}
			args, err := wfr.ArgsByRowIdx(ctx, idx)
			if err != nil {
				return nil, err
			}
			value := args[0]
			switch v := value.(type) {
			case *tree.DInt:
				if sum == nil {
					sum = tree.NewDInt(0)
				}
				sum = tree.NewDInt(*sum.(*tree.DInt) + *v)
			case *tree.DDecimal:
				if sum == nil {
					sum = &tree.DDecimal{}
				}
				_, err := tree.ExactCtx.Add(&sum.(*tree.DDecimal).Decimal, &v.Decimal, &sum.(*tree.DDecimal).Decimal)
				if err != nil {
					return tree.DNull, err
				}
			case *tree.DFloat:
				if sum == nil {
					sum = tree.NewDFloat(0)
				}
				sum = tree.NewDFloat(*sum.(*tree.DFloat) + *v)
			case *tree.DInterval:
				if sum == nil {
					sum = &tree.DInterval{}
				}
				sum.(*tree.DInterval).Duration = sum.(*tree.DInterval).Duration.Add(v.Duration)
			default:
				return tree.DNull, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected value %v", v)
			}
		}
		if sum == nil {
			// Spec: the frame is empty, so we return NULL.
			return tree.DNull, nil
		}
		return sum, nil
	}

	// We need to discard all values that are no longer in the frame.
	if err = w.removeAllBefore(ctx, evalCtx, wfr); err != nil {
		return nil, err
	}

	// We need to sum all values that just entered the frame and have not been
	// added yet.
	for idx := max(w.prevEnd, frameStartIdx); idx < frameEndIdx; idx++ {
		if wfr.FilterColIdx != noFilterIdx {
			row, err := wfr.Rows.GetRow(ctx, idx)
			if err != nil {
				return nil, err
			}
			d, err := row.GetDatum(wfr.FilterColIdx)
			if err != nil {
				return nil, err
			}
			if d != tree.DBoolTrue {
				// Row idx is filtered out from the window frame, so we skip it.
				continue
			}
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return nil, err
		}
		err = w.agg.Add(ctx, args[0])
		if err != nil {
			return nil, err
		}
	}

	w.prevStart = frameStartIdx
	w.prevEnd = frameEndIdx
	if frameStartIdx == frameEndIdx {
		// Spec: the frame is empty, so we return NULL.
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
	sum slidingWindowSumFunc
}

// Compute implements WindowFunc interface.
func (w *avgWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	var sum tree.Datum
	var err error
	sum, err = w.sum.Compute(ctx, evalCtx, wfr)
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	var frameSize int
	if wfr.FilterColIdx != noFilterIdx {
		frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
		if err != nil {
			return nil, err
		}
		frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
		if err != nil {
			return nil, err
		}
		for idx := frameStartIdx; idx < frameEndIdx; idx++ {
			row, err := wfr.Rows.GetRow(ctx, idx)
			if err != nil {
				return nil, err
			}
			d, err := row.GetDatum(wfr.FilterColIdx)
			if err != nil {
				return nil, err
			}
			if d != tree.DBoolTrue {
				// Row idx is filtered out from the window frame, so we skip it.
				continue
			}
			frameSize++
		}
	} else {
		if frameSize, err = wfr.FrameSizeIgnoringFilter(ctx, evalCtx); err != nil {
			return nil, err
		}
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
		dd.SetFinite(int64(*t), 0)
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
