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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

func init() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range windows {
		for _, w := range v {
			if !w.impure {
				panic(fmt.Sprintf("window functions should all be impure, found %v", w))
			}
			if w.class != WindowClass {
				panic(fmt.Sprintf("window functions should be marked with the WindowClass "+
					"function class, found %v", w))
			}
			if w.WindowFunc == nil {
				panic(fmt.Sprintf("window functions should have WindowFunc constructors, "+
					"found %v", w))
			}
		}
		Builtins[strings.ToUpper(k)] = v
		Builtins[strings.ToLower(k)] = v
	}
}

// IndexedRow is a row with a corresponding index.
type IndexedRow struct {
	Idx int
	Row DTuple
}

// WindowFrame is a view into a subset of data over which calculations are made.
type WindowFrame struct {
	// constant for all calls to WindowFunc.Add
	Rows        []IndexedRow
	ArgIdxStart int // the index which arguments to the window function begin
	ArgCount    int // the number of window function arguments

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index

	// changes for each peer group
	FirstPeerIdx int // the first index in the current peer group
	PeerRowCount int // the number of rows in the current peer group
}

func (wf WindowFrame) rank() int {
	return wf.RowIdx + 1
}

func (wf WindowFrame) rowCount() int {
	return len(wf.Rows)
}

// TODO(nvanbenschoten): This definition only holds while we don't support
// frame specification (RANGE or ROWS) in the OVER clause.
func (wf WindowFrame) frameSize() int {
	return wf.FirstPeerIdx + wf.PeerRowCount
}

// firstInPeerGroup returns if the current row is the first in its peer group.
func (wf WindowFrame) firstInPeerGroup() bool {
	return wf.RowIdx == wf.FirstPeerIdx
}

func (wf WindowFrame) args() []Datum {
	return wf.argsWithRowOffset(0)
}

func (wf WindowFrame) argsWithRowOffset(offset int) []Datum {
	return wf.Rows[wf.RowIdx+offset].Row[wf.ArgIdxStart : wf.ArgIdxStart+wf.ArgCount]
}

// WindowFunc performs a computation on each row using data from a provided WindowFrame.
type WindowFunc interface {
	// Compute computes the window function for the provided window frame, given the
	// current state of WindowFunc. The method should be called sequentially for every
	// row in a partition in turn with the desired ordering of the WindowFunc. This is
	// because there is an implicit carried dependency between each row and all those
	// that have come before it (like in an AggregateFunc). As such, this approach does
	// not present any exploitable associativity/commutativity for optimization.
	Compute(WindowFrame) (Datum, error)
}

// windows are a special class of builtin functions that can only be applied
// as window functions using an OVER clause.
// See `windowFuncHolder` in the sql package.
var windows = map[string][]Builtin{
	"row_number": {
		makeWindowBuiltin(ArgTypes{}, TypeInt, newRowNumberWindow),
	},
	"rank": {
		makeWindowBuiltin(ArgTypes{}, TypeInt, newRankWindow),
	},
	"dense_rank": {
		makeWindowBuiltin(ArgTypes{}, TypeInt, newDenseRankWindow),
	},
	"percent_rank": {
		makeWindowBuiltin(ArgTypes{}, TypeFloat, newPercentRankWindow),
	},
	"cume_dist": {
		makeWindowBuiltin(ArgTypes{}, TypeFloat, newCumulativeDistWindow),
	},
	"ntile": {
		makeWindowBuiltin(ArgTypes{TypeInt}, TypeInt, newNtileWindow),
	},
	"lag": mergeBuiltinSlices(
		collectWindowBuiltins(func(t Datum) Builtin {
			return makeWindowBuiltin(ArgTypes{t}, t, makeLeadLagWindowConstructor(false, false, false))
		}, anyElementTypes...),
		collectWindowBuiltins(func(t Datum) Builtin {
			return makeWindowBuiltin(ArgTypes{t, TypeInt}, t, makeLeadLagWindowConstructor(false, true, false))
		}, anyElementTypes...),
		collectWindowBuiltins(func(t Datum) Builtin {
			return makeWindowBuiltin(ArgTypes{t, TypeInt, t}, t, makeLeadLagWindowConstructor(false, true, true))
		}, anyElementTypes...),
	),
	"lead": mergeBuiltinSlices(
		collectWindowBuiltins(func(t Datum) Builtin {
			return makeWindowBuiltin(ArgTypes{t}, t, makeLeadLagWindowConstructor(true, false, false))
		}, anyElementTypes...),
		collectWindowBuiltins(func(t Datum) Builtin {
			return makeWindowBuiltin(ArgTypes{t, TypeInt}, t, makeLeadLagWindowConstructor(true, true, false))
		}, anyElementTypes...),
		collectWindowBuiltins(func(t Datum) Builtin {
			return makeWindowBuiltin(ArgTypes{t, TypeInt, t}, t, makeLeadLagWindowConstructor(true, true, true))
		}, anyElementTypes...),
	),
	"first_value": collectWindowBuiltins(func(t Datum) Builtin {
		return makeWindowBuiltin(ArgTypes{t}, t, newFirstValueWindow)
	}, anyElementTypes...),
	"last_value": collectWindowBuiltins(func(t Datum) Builtin {
		return makeWindowBuiltin(ArgTypes{t}, t, newLastValueWindow)
	}, anyElementTypes...),
	"nth_value": collectWindowBuiltins(func(t Datum) Builtin {
		return makeWindowBuiltin(ArgTypes{t, TypeInt}, t, newNthValueWindow)
	}, anyElementTypes...),
}

var anyElementTypes = []Datum{
	TypeBool,
	TypeInt,
	TypeFloat,
	TypeDecimal,
	TypeString,
	TypeBytes,
	TypeDate,
	TypeTimestamp,
	TypeInterval,
	TypeTuple,
}

func makeWindowBuiltin(in ArgTypes, ret Datum, f func() WindowFunc) Builtin {
	return Builtin{
		impure:     true,
		class:      WindowClass,
		Types:      in,
		ReturnType: ret,
		WindowFunc: f,
	}
}

func collectWindowBuiltins(f func(Datum) Builtin, types ...Datum) []Builtin {
	r := make([]Builtin, len(types))
	for i := range types {
		r[i] = f(types[i])
	}
	return r
}

func mergeBuiltinSlices(s ...[]Builtin) []Builtin {
	var r []Builtin
	for _, bs := range s {
		r = append(r, bs...)
	}
	return r
}

var _ WindowFunc = &aggregateWindowFunc{}
var _ WindowFunc = &rowNumberWindow{}
var _ WindowFunc = &rankWindow{}
var _ WindowFunc = &denseRankWindow{}
var _ WindowFunc = &percentRankWindow{}
var _ WindowFunc = &cumulativeDistWindow{}
var _ WindowFunc = &ntileWindow{}
var _ WindowFunc = &leadLagWindow{}
var _ WindowFunc = &firstValueWindow{}
var _ WindowFunc = &lastValueWindow{}
var _ WindowFunc = &nthValueWindow{}

// aggregateWindowFunc aggregates over the the current row's window frame, using
// the internal AggregateFunc to perform the aggregation.
type aggregateWindowFunc struct {
	agg     AggregateFunc
	peerRes Datum
}

func newAggregateWindow(agg AggregateFunc) WindowFunc {
	return &aggregateWindowFunc{agg: agg}
}

func (w *aggregateWindowFunc) Compute(wf WindowFrame) (Datum, error) {
	if !wf.firstInPeerGroup() {
		return w.peerRes, nil
	}

	// Accumulate all values in the peer group at the same time, as these
	// must return the same value.
	for i := 0; i < wf.PeerRowCount; i++ {
		w.agg.Add(wf.argsWithRowOffset(i)[0])
	}

	// Retrieve the value for the entire peer group, save it, and return it.
	w.peerRes = w.agg.Result()
	return w.peerRes, nil
}

// rowNumberWindow computes the number of the current row within its partition,
// counting from 1.
type rowNumberWindow struct{}

func newRowNumberWindow() WindowFunc {
	return &rowNumberWindow{}
}

func (rowNumberWindow) Compute(wf WindowFrame) (Datum, error) {
	return NewDInt(DInt(wf.RowIdx + 1 /* one-indexed */)), nil
}

// rankWindow computes the rank of the current row with gaps.
type rankWindow struct {
	peerRes *DInt
}

func newRankWindow() WindowFunc {
	return &rankWindow{}
}

func (w *rankWindow) Compute(wf WindowFrame) (Datum, error) {
	if wf.firstInPeerGroup() {
		w.peerRes = NewDInt(DInt(wf.rank()))
	}
	return w.peerRes, nil
}

// denseRankWindow computes the rank of the current row without gaps (it counts peer groups).
type denseRankWindow struct {
	denseRank int64
	peerRes   *DInt
}

func newDenseRankWindow() WindowFunc {
	return &denseRankWindow{}
}

func (w *denseRankWindow) Compute(wf WindowFrame) (Datum, error) {
	if wf.firstInPeerGroup() {
		w.denseRank++
		w.peerRes = NewDInt(DInt(w.denseRank))
	}
	return w.peerRes, nil
}

// percentRankWindow computes the relative rank of the current row using:
//   (rank - 1) / (total rows - 1)
type percentRankWindow struct {
	peerRes *DFloat
}

func newPercentRankWindow() WindowFunc {
	return &percentRankWindow{}
}

var dfloatZero = NewDFloat(0)

func (w *percentRankWindow) Compute(wf WindowFrame) (Datum, error) {
	// Return zero if there's only one row, per spec.
	if wf.rowCount() <= 1 {
		return dfloatZero, nil
	}

	if wf.firstInPeerGroup() {
		// (rank - 1) / (total rows - 1)
		w.peerRes = NewDFloat(DFloat(wf.rank()-1) / DFloat(wf.rowCount()-1))
	}
	return w.peerRes, nil
}

// cumulativeDistWindow computes the relative rank of the current row using:
//   (number of rows preceding or peer with current row) / (total rows)
type cumulativeDistWindow struct {
	peerRes *DFloat
}

func newCumulativeDistWindow() WindowFunc {
	return &cumulativeDistWindow{}
}

func (w *cumulativeDistWindow) Compute(wf WindowFrame) (Datum, error) {
	if wf.firstInPeerGroup() {
		// (number of rows preceding or peer with current row) / (total rows)
		w.peerRes = NewDFloat(DFloat(wf.frameSize()) / DFloat(wf.rowCount()))
	}
	return w.peerRes, nil
}

// ntileWindow computes an integer ranging from 1 to the argument value, dividing
// the partition as equally as possible.
type ntileWindow struct {
	ntile          *DInt // current result
	curBucketCount int64 // row number of current bucket
	boundary       int64 // how many rows should be in the bucket
	remainder      int64 // (total rows) % (bucket num)
}

func newNtileWindow() WindowFunc {
	return &ntileWindow{}
}

var errInvalidArgumentForNtile = errors.Errorf("argument of ntile must be greater than zero")

func (w *ntileWindow) Compute(wf WindowFrame) (Datum, error) {
	if w.ntile == nil {
		// If this is the first call to ntileWindow.Compute, set up the buckets.
		total := int64(wf.rowCount())

		arg := wf.args()[0]
		if arg == DNull {
			// per spec: If argument is the null value, then the result is the null value.
			return DNull, nil
		}
		nbuckets := int64(*arg.(*DInt))
		if nbuckets <= 0 {
			// per spec: If argument is less than or equal to 0, then an error is returned.
			return nil, errInvalidArgumentForNtile
		}

		w.ntile = NewDInt(1)
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
		if w.remainder != 0 && int64(*w.ntile) == w.remainder {
			w.remainder = 0
			w.boundary--
		}
		w.ntile = NewDInt(*w.ntile + 1)
		w.curBucketCount = 1
	}
	return w.ntile, nil
}

type leadLagWindow struct {
	forward     bool
	withOffset  bool
	withDefault bool
}

func newLeadLagWindow(forward, withOffset, withDefault bool) WindowFunc {
	return &leadLagWindow{
		forward:     forward,
		withOffset:  withOffset,
		withDefault: withDefault,
	}
}

func makeLeadLagWindowConstructor(forward, withOffset, withDefault bool) func() WindowFunc {
	return func() WindowFunc {
		return newLeadLagWindow(forward, withOffset, withDefault)
	}
}

func (w *leadLagWindow) Compute(wf WindowFrame) (Datum, error) {
	offset := 1
	if w.withOffset {
		offsetArg := wf.args()[1]
		if offsetArg == DNull {
			return DNull, nil
		}
		offset = int(*offsetArg.(*DInt))
	}
	if !w.forward {
		offset *= -1
	}

	if targetRow := wf.RowIdx + offset; targetRow < 0 || targetRow >= wf.rowCount() {
		// Target row is out of the partition; supply default value if provided,
		// otherwise return NULL.
		if w.withDefault {
			return wf.args()[2], nil
		}
		return DNull, nil
	}

	return wf.argsWithRowOffset(offset)[0], nil
}

// firstValueWindow returns value evaluated at the row that is the first row of the window frame.
type firstValueWindow struct{}

func newFirstValueWindow() WindowFunc {
	return &firstValueWindow{}
}

func (firstValueWindow) Compute(wf WindowFrame) (Datum, error) {
	return wf.Rows[0].Row[wf.ArgIdxStart], nil
}

// lastValueWindow returns value evaluated at the row that is the last row of the window frame.
type lastValueWindow struct{}

func newLastValueWindow() WindowFunc {
	return &lastValueWindow{}
}

func (lastValueWindow) Compute(wf WindowFrame) (Datum, error) {
	return wf.Rows[wf.frameSize()-1].Row[wf.ArgIdxStart], nil
}

// nthValueWindow returns value evaluated at the row that is the nth row of the window frame
// (counting from 1). Returns null if no such row.
type nthValueWindow struct{}

func newNthValueWindow() WindowFunc {
	return &nthValueWindow{}
}

var errInvalidArgumentForNthValue = errors.Errorf("argument of nth_value must be greater than zero")

func (nthValueWindow) Compute(wf WindowFrame) (Datum, error) {
	arg := wf.args()[1]
	if arg == DNull {
		return DNull, nil
	}

	nth := int(*arg.(*DInt))
	if nth <= 0 {
		return nil, errInvalidArgumentForNthValue
	}

	// per spec: Only consider the rows within the "window frame", which by default contains
	// the rows from the start of the partition through the last peer of the current row.
	if nth > wf.frameSize() {
		return DNull, nil
	}
	return wf.Rows[nth-1].Row[wf.ArgIdxStart], nil
}

var _ Visitor = &ContainsWindowVisitor{}

// ContainsWindowVisitor checks if walked expressions contain window functions.
type ContainsWindowVisitor struct {
	sawWindow bool
}

// VisitPre satisfies the Visitor interface.
func (v *ContainsWindowVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *FuncExpr:
		if t.IsWindowFunctionApplication() {
			v.sawWindow = true
			return false, expr
		}
	case *Subquery:
		return false, expr
	}
	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*ContainsWindowVisitor) VisitPost(expr Expr) Expr { return expr }

// ContainsWindowFunc determines if an Expr contains a window function.
func (v *ContainsWindowVisitor) ContainsWindowFunc(expr Expr) bool {
	if expr != nil {
		WalkExprConst(v, expr)
		ret := v.sawWindow
		v.sawWindow = false
		return ret
	}
	return false
}

// WindowFuncInExpr determines if an Expr contains a window function, using
// the Parser's embedded visitor.
func (p *Parser) WindowFuncInExpr(expr Expr) bool {
	return p.containsWindowVisitor.ContainsWindowFunc(expr)
}

// WindowFuncInExprs determines if any of the provided TypedExpr contains a
// window function, using the Parser's embedded visitor.
func (p *Parser) WindowFuncInExprs(exprs []TypedExpr) bool {
	for _, expr := range exprs {
		if p.WindowFuncInExpr(expr) {
			return true
		}
	}
	return false
}
