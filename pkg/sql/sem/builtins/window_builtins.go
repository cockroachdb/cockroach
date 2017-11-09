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

package builtins

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func initWindowBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range windows {
		for _, w := range v {
			if !w.Impure {
				panic(fmt.Sprintf("window functions should all be impure, found %v", w))
			}
			if w.Class != parser.WindowClass {
				panic(fmt.Sprintf("window functions should be marked with the parser.WindowClass "+
					"function class, found %v", w))
			}
			if w.WindowFunc == nil {
				panic(fmt.Sprintf("window functions should have parser.WindowFunc constructors, "+
					"found %v", w))
			}
		}
		Builtins[k] = v
	}
}

// windows are a special class of builtin functions that can only be applied
// as window functions using an OVER clause.
// See `windowFuncHolder` in the sql package.
var windows = map[string][]parser.Builtin{
	"row_number": {
		makeWindowBuiltin(parser.ArgTypes{}, types.Int, newRowNumberWindow),
	},
	"rank": {
		makeWindowBuiltin(parser.ArgTypes{}, types.Int, newRankWindow),
	},
	"dense_rank": {
		makeWindowBuiltin(parser.ArgTypes{}, types.Int, newDenseRankWindow),
	},
	"percent_rank": {
		makeWindowBuiltin(parser.ArgTypes{}, types.Float, newPercentRankWindow),
	},
	"cume_dist": {
		makeWindowBuiltin(parser.ArgTypes{}, types.Float, newCumulativeDistWindow),
	},
	"ntile": {
		makeWindowBuiltin(parser.ArgTypes{{"n", types.Int}}, types.Int, newNtileWindow),
	},
	"lag": mergeBuiltinSlices(
		collectBuiltins(func(t types.T) parser.Builtin {
			return makeWindowBuiltin(parser.ArgTypes{{"val", t}}, t, makeLeadLagWindowConstructor(false, false, false))
		}, types.AnyNonArray...),
		collectBuiltins(func(t types.T) parser.Builtin {
			return makeWindowBuiltin(parser.ArgTypes{{"val", t}, {"n", types.Int}}, t, makeLeadLagWindowConstructor(false, true, false))
		}, types.AnyNonArray...),
		// TODO(nvanbenschoten): We still have no good way to represent two parameters that
		// can be any types but must be the same (eg. lag(T, Int, T)).
		collectBuiltins(func(t types.T) parser.Builtin {
			return makeWindowBuiltin(parser.ArgTypes{{"val", t}, {"n", types.Int}, {"default", t}},
				t, makeLeadLagWindowConstructor(false, true, true))
		}, types.AnyNonArray...),
	),
	"lead": mergeBuiltinSlices(
		collectBuiltins(func(t types.T) parser.Builtin {
			return makeWindowBuiltin(parser.ArgTypes{{"val", t}}, t, makeLeadLagWindowConstructor(true, false, false))
		}, types.AnyNonArray...),
		collectBuiltins(func(t types.T) parser.Builtin {
			return makeWindowBuiltin(parser.ArgTypes{{"val", t}, {"n", types.Int}}, t, makeLeadLagWindowConstructor(true, true, false))
		}, types.AnyNonArray...),
		collectBuiltins(func(t types.T) parser.Builtin {
			return makeWindowBuiltin(parser.ArgTypes{{"val", t}, {"n", types.Int}, {"default", t}},
				t, makeLeadLagWindowConstructor(true, true, true))
		}, types.AnyNonArray...),
	),
	"first_value": collectBuiltins(func(t types.T) parser.Builtin {
		return makeWindowBuiltin(parser.ArgTypes{{"val", t}}, t, newFirstValueWindow)
	}, types.AnyNonArray...),
	"last_value": collectBuiltins(func(t types.T) parser.Builtin {
		return makeWindowBuiltin(parser.ArgTypes{{"val", t}}, t, newLastValueWindow)
	}, types.AnyNonArray...),
	"nth_value": collectBuiltins(func(t types.T) parser.Builtin {
		return makeWindowBuiltin(parser.ArgTypes{{"val", t}, {"n", types.Int}}, t, newNthValueWindow)
	}, types.AnyNonArray...),
}

func makeWindowBuiltin(
	in parser.ArgTypes, ret types.T, f func([]types.T, *parser.EvalContext) parser.WindowFunc,
) parser.Builtin {
	return parser.Builtin{
		Impure:     true,
		Class:      parser.WindowClass,
		Types:      in,
		ReturnType: parser.FixedReturnType(ret),
		WindowFunc: f,
	}
}

func mergeBuiltinSlices(s ...[]parser.Builtin) []parser.Builtin {
	var r []parser.Builtin
	for _, bs := range s {
		r = append(r, bs...)
	}
	return r
}

var _ parser.WindowFunc = &aggregateWindowFunc{}
var _ parser.WindowFunc = &rowNumberWindow{}
var _ parser.WindowFunc = &rankWindow{}
var _ parser.WindowFunc = &denseRankWindow{}
var _ parser.WindowFunc = &percentRankWindow{}
var _ parser.WindowFunc = &cumulativeDistWindow{}
var _ parser.WindowFunc = &ntileWindow{}
var _ parser.WindowFunc = &leadLagWindow{}
var _ parser.WindowFunc = &firstValueWindow{}
var _ parser.WindowFunc = &lastValueWindow{}
var _ parser.WindowFunc = &nthValueWindow{}

// aggregateWindowFunc aggregates over the the current row's window frame, using
// the internal parser.AggregateFunc to perform the aggregation.
type aggregateWindowFunc struct {
	agg     parser.AggregateFunc
	peerRes parser.Datum
}

func newAggregateWindow(agg parser.AggregateFunc) parser.WindowFunc {
	return &aggregateWindowFunc{agg: agg}
}

func (w *aggregateWindowFunc) Compute(
	ctx context.Context, evalCtx *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	if !wf.FirstInPeerGroup() {
		return w.peerRes, nil
	}

	// Accumulate all values in the peer group at the same time, as these
	// must return the same value.
	for i := 0; i < wf.PeerRowCount; i++ {
		args := wf.ArgsWithRowOffset(i)
		var value parser.Datum
		// COUNT_ROWS takes no arguments.
		if len(args) > 0 {
			value = args[0]
		}
		if err := w.agg.Add(ctx, value); err != nil {
			return nil, err
		}
	}

	// Retrieve the value for the entire peer group, save it, and return it.

	peerRes, err := w.agg.Result()
	if err != nil {
		return nil, err
	}
	w.peerRes = peerRes
	return w.peerRes, nil
}

func (w *aggregateWindowFunc) Close(ctx context.Context, evalCtx *parser.EvalContext) {
	w.agg.Close(ctx)
}

// rowNumberWindow computes the number of the current row within its partition,
// counting from 1.
type rowNumberWindow struct{}

func newRowNumberWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &rowNumberWindow{}
}

func (rowNumberWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	return parser.NewDInt(parser.DInt(wf.RowIdx + 1 /* one-indexed */)), nil
}

func (rowNumberWindow) Close(context.Context, *parser.EvalContext) {}

// rankWindow computes the rank of the current row with gaps.
type rankWindow struct {
	peerRes *parser.DInt
}

func newRankWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &rankWindow{}
}

func (w *rankWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	if wf.FirstInPeerGroup() {
		w.peerRes = parser.NewDInt(parser.DInt(wf.Rank()))
	}
	return w.peerRes, nil
}

func (w *rankWindow) Close(context.Context, *parser.EvalContext) {}

// denseRankWindow computes the rank of the current row without gaps (it counts peer groups).
type denseRankWindow struct {
	denseRank int
	peerRes   *parser.DInt
}

func newDenseRankWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &denseRankWindow{}
}

func (w *denseRankWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	if wf.FirstInPeerGroup() {
		w.denseRank++
		w.peerRes = parser.NewDInt(parser.DInt(w.denseRank))
	}
	return w.peerRes, nil
}

func (w *denseRankWindow) Close(context.Context, *parser.EvalContext) {}

// percentRankWindow computes the relative rank of the current row using:
//   (rank - 1) / (total rows - 1)
type percentRankWindow struct {
	peerRes *parser.DFloat
}

func newPercentRankWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &percentRankWindow{}
}

var dfloatZero = parser.NewDFloat(0)

func (w *percentRankWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	// Return zero if there's only one row, per spec.
	if wf.RowCount() <= 1 {
		return dfloatZero, nil
	}

	if wf.FirstInPeerGroup() {
		// (rank - 1) / (total rows - 1)
		w.peerRes = parser.NewDFloat(parser.DFloat(wf.Rank()-1) / parser.DFloat(wf.RowCount()-1))
	}
	return w.peerRes, nil
}

func (w *percentRankWindow) Close(context.Context, *parser.EvalContext) {}

// cumulativeDistWindow computes the relative rank of the current row using:
//   (number of rows preceding or peer with current row) / (total rows)
type cumulativeDistWindow struct {
	peerRes *parser.DFloat
}

func newCumulativeDistWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &cumulativeDistWindow{}
}

func (w *cumulativeDistWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	if wf.FirstInPeerGroup() {
		// (number of rows preceding or peer with current row) / (total rows)
		w.peerRes = parser.NewDFloat(parser.DFloat(wf.FrameSize()) / parser.DFloat(wf.RowCount()))
	}
	return w.peerRes, nil
}

func (w *cumulativeDistWindow) Close(context.Context, *parser.EvalContext) {}

// ntileWindow computes an integer ranging from 1 to the argument value, dividing
// the partition as equally as possible.
type ntileWindow struct {
	ntile          *parser.DInt // current result
	curBucketCount int          // row number of current bucket
	boundary       int          // how many rows should be in the bucket
	remainder      int          // (total rows) % (bucket num)
}

func newNtileWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &ntileWindow{}
}

var errInvalidArgumentForNtile = pgerror.NewErrorf(
	pgerror.CodeInvalidParameterValueError, "argument of ntile() must be greater than zero")

func (w *ntileWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	if w.ntile == nil {
		// If this is the first call to ntileWindow.Compute, set up the buckets.
		total := wf.RowCount()

		arg := wf.Args()[0]
		if arg == parser.DNull {
			// per spec: If argument is the null value, then the result is the null value.
			return parser.DNull, nil
		}

		nbuckets := int(parser.MustBeDInt(arg))
		if nbuckets <= 0 {
			// per spec: If argument is less than or equal to 0, then an error is returned.
			return nil, errInvalidArgumentForNtile
		}

		w.ntile = parser.NewDInt(1)
		w.curBucketCount = 0
		w.boundary = total / nbuckets
		if w.boundary <= 0 {
			w.boundary = 1
		} else {
			// If the total number is not divisible, add 1 row to leading buckets.
			w.remainder = total % nbuckets
			if w.remainder != 0 {
				w.boundary++
			}
		}
	}

	w.curBucketCount++
	if w.boundary < w.curBucketCount {
		// Move to next ntile bucket.
		if w.remainder != 0 && int(*w.ntile) == w.remainder {
			w.remainder = 0
			w.boundary--
		}
		w.ntile = parser.NewDInt(*w.ntile + 1)
		w.curBucketCount = 1
	}
	return w.ntile, nil
}

func (w *ntileWindow) Close(context.Context, *parser.EvalContext) {}

type leadLagWindow struct {
	forward     bool
	withOffset  bool
	withDefault bool
}

func newLeadLagWindow(forward, withOffset, withDefault bool) parser.WindowFunc {
	return &leadLagWindow{
		forward:     forward,
		withOffset:  withOffset,
		withDefault: withDefault,
	}
}

func makeLeadLagWindowConstructor(
	forward, withOffset, withDefault bool,
) func([]types.T, *parser.EvalContext) parser.WindowFunc {
	return func([]types.T, *parser.EvalContext) parser.WindowFunc {
		return newLeadLagWindow(forward, withOffset, withDefault)
	}
}

func (w *leadLagWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	offset := 1
	if w.withOffset {
		offsetArg := wf.Args()[1]
		if offsetArg == parser.DNull {
			return parser.DNull, nil
		}
		offset = int(parser.MustBeDInt(offsetArg))
	}
	if !w.forward {
		offset *= -1
	}

	if targetRow := wf.RowIdx + offset; targetRow < 0 || targetRow >= wf.RowCount() {
		// Target row is out of the partition; supply default value if provided,
		// otherwise return NULL.
		if w.withDefault {
			return wf.Args()[2], nil
		}
		return parser.DNull, nil
	}

	return wf.ArgsWithRowOffset(offset)[0], nil
}

func (w *leadLagWindow) Close(context.Context, *parser.EvalContext) {}

// firstValueWindow returns value evaluated at the row that is the first row of the window frame.
type firstValueWindow struct{}

func newFirstValueWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &firstValueWindow{}
}

func (firstValueWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	return wf.Rows[0].Row[wf.ArgIdxStart], nil
}

func (firstValueWindow) Close(context.Context, *parser.EvalContext) {}

// lastValueWindow returns value evaluated at the row that is the last row of the window frame.
type lastValueWindow struct{}

func newLastValueWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &lastValueWindow{}
}

func (lastValueWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	return wf.Rows[wf.FrameSize()-1].Row[wf.ArgIdxStart], nil
}

func (lastValueWindow) Close(context.Context, *parser.EvalContext) {}

// nthValueWindow returns value evaluated at the row that is the nth row of the window frame
// (counting from 1). Returns null if no such row.
type nthValueWindow struct{}

func newNthValueWindow([]types.T, *parser.EvalContext) parser.WindowFunc {
	return &nthValueWindow{}
}

var errInvalidArgumentForNthValue = pgerror.NewErrorf(
	pgerror.CodeInvalidParameterValueError, "argument of nth_value() must be greater than zero")

func (nthValueWindow) Compute(
	_ context.Context, _ *parser.EvalContext, wf parser.WindowFrame,
) (parser.Datum, error) {
	arg := wf.Args()[1]
	if arg == parser.DNull {
		return parser.DNull, nil
	}

	nth := int(parser.MustBeDInt(arg))
	if nth <= 0 {
		return nil, errInvalidArgumentForNthValue
	}

	// per spec: Only consider the rows within the "window frame", which by default contains
	// the rows from the start of the partition through the last peer of the current row.
	if nth > wf.FrameSize() {
		return parser.DNull, nil
	}
	return wf.Rows[nth-1].Row[wf.ArgIdxStart], nil
}

func (nthValueWindow) Close(context.Context, *parser.EvalContext) {}
