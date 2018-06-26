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
)

// indexedValue combines a value from the row with the index of that row.
type indexedValue struct {
	value tree.Datum
	idx   int
}

// RingBufferInitialSize defines the initial size of the ring buffer.
const RingBufferInitialSize = 8

// ringBuffer is a deque of indexedValue's maintained over a ring buffer.
type ringBuffer struct {
	values []*indexedValue
	head   int // the index of the front of the deque.
	tail   int // the index of the first position right after the end of the deque.

	nonEmpty bool // indicates whether the deque is empty, necessary to distinguish
	// between an empty deque and a deque that uses all of its capacity.
}

// len returns number of indexedValue's in the deque.
func (r *ringBuffer) len() int {
	if !r.nonEmpty {
		return 0
	}
	if r.head < r.tail {
		return r.tail - r.head
	} else if r.head == r.tail {
		return cap(r.values)
	} else {
		return cap(r.values) + r.tail - r.head
	}
}

// add adds value to the end of the deque
// and doubles it's underlying slice if necessary.
func (r *ringBuffer) add(value *indexedValue) {
	if cap(r.values) == 0 {
		r.values = make([]*indexedValue, RingBufferInitialSize)
		r.values[0] = value
		r.tail = 1
	} else {
		if r.len() == cap(r.values) {
			newValues := make([]*indexedValue, 2*cap(r.values))
			if r.head < r.tail {
				copy(newValues[:r.len()], r.values[r.head:r.tail])
			} else {
				copy(newValues[:cap(r.values)-r.head], r.values[r.head:])
				copy(newValues[cap(r.values)-r.head:r.len()], r.values[:r.tail])
			}
			r.head = 0
			r.tail = cap(r.values)
			r.values = newValues
		}
		r.values[r.tail] = value
		r.tail = (r.tail + 1) % cap(r.values)
	}
	r.nonEmpty = true
}

// get returns indexedValue at position pos in the deque (zero-based).
func (r *ringBuffer) get(pos int) *indexedValue {
	if !r.nonEmpty || pos < 0 || pos >= r.len() {
		panic("unexpected behavior: index out of bounds")
	}
	return r.values[(pos+r.head)%cap(r.values)]
}

// removeHead removes a single element from the front of the deque.
func (r *ringBuffer) removeHead() {
	if r.len() == 0 {
		panic("removing head from empty ring buffer")
	}
	r.values[r.head] = nil
	r.head = (r.head + 1) % cap(r.values)
	if r.head == r.tail {
		r.nonEmpty = false
	}
}

// removeTail removes a single element from the end of the deque.
func (r *ringBuffer) removeTail() {
	if r.len() == 0 {
		panic("removing tail from empty ring buffer")
	}
	lastPos := (cap(r.values) + r.tail - 1) % cap(r.values)
	r.values[lastPos] = nil
	r.tail = lastPos
	if r.tail == r.head {
		r.nonEmpty = false
	}
}

// slidingWindow maintains a deque of values along with corresponding indices
// based on cmp function:
// for Min behavior, cmp = -a.Compare(b)
// for Max behavior, cmp = a.Compare(b)
//
// It assumes that the frame bounds will never go back, i.e. non-decreasing sequences
// of frame start and frame end indices.
type slidingWindow struct {
	values  ringBuffer
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
// from the end of the deque and then appends 'iv' to the end. This way, the deque
// always contains unique values sorted in descending order of their "priority"
// (when we encounter duplicates, we always keep the one with the largest idx).
func (sw *slidingWindow) add(iv *indexedValue) {
	for i := sw.values.len() - 1; i >= 0; i-- {
		if sw.cmp(sw.evalCtx, sw.values.get(i).value, iv.value) > 0 {
			break
		}
		sw.values.removeTail()
	}
	sw.values.add(iv)
}

// removeAllBefore removes all values from the beginning of the deque that have indices
// smaller than given 'idx'.
// This operation corresponds to shifting the start of the frame up to 'idx'.
func (sw *slidingWindow) removeAllBefore(idx int) {
	for i := 0; i < sw.values.len() && i < idx; i++ {
		if sw.values.get(i).idx >= idx {
			break
		}
		sw.values.removeHead()
	}
}

func (sw *slidingWindow) string() string {
	var builder strings.Builder
	for i := 0; i < sw.values.len(); i++ {
		builder.WriteString(fmt.Sprintf("(%v, %v)\t", sw.values.get(i).value, sw.values.get(i).idx))
	}
	return builder.String()
}

type slidingWindowFunc struct {
	sw      *slidingWindow
	prevEnd int
}

// Compute implements WindowFunc interface.
func (w *slidingWindowFunc) Compute(
	_ context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	start, end := wfr.FrameStartIdx(), wfr.FrameEndIdx()

	// We need to discard all values that are no longer in the frame.
	w.sw.removeAllBefore(start)

	// We need to add all values that just entered the frame and have not been added yet.
	for idx := max(w.prevEnd, start); idx < end; idx++ {
		w.sw.add(&indexedValue{wfr.ArgsByRowIdx(idx)[0], idx})
	}
	w.prevEnd = end

	if w.sw.values.len() == 0 {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	// The datum with "highest priority" within the frame is at the very front of the deque.
	return w.sw.values.get(0).value, nil
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

// slidingWindowSumFunc applies sliding window approach to summation over a frame.
// It assumes that the frame bounds will never go back, i.e. non-decreasing sequences
// of frame start and frame end indices.
type slidingWindowSumFunc struct {
	agg                tree.AggregateFunc // one of the four SumAggregates
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
		case *tree.DInterval:
			// TODO(yuzefovich): currently only integer offsets are supported,
			// but they could also be like '10 days' PRECEDING. When it is
			// supported, this logic should be checked and tested.
			return w.agg.Add(ctx, &tree.DInterval{Duration: duration.Duration{}.Sub(v.Duration)})
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
