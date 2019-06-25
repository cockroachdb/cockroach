// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

var (
	scalars, bools             []scalarWeight
	scalarWeights, boolWeights []int
)

func init() {
	scalars = []scalarWeight{
		{10, scalarNoContext(makeAnd)},
		{1, scalarNoContext(makeCaseExpr)},
		{1, scalarNoContext(makeCoalesceExpr)},
		{50, scalarNoContext(makeColRef)},
		{10, scalarNoContext(makeBinOp)},
		{2, scalarNoContext(makeScalarSubquery)},
		{2, scalarNoContext(makeExists)},
		{2, scalarNoContext(makeIn)},
		{2, scalarNoContext(makeStringComparison)},
		{5, scalarNoContext(makeAnd)},
		{5, scalarNoContext(makeOr)},
		{5, scalarNoContext(makeNot)},
		{10, makeFunc},
		{10, func(s *scope, ctx Context, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeConstExpr(s, typ, refs), true
		}},
	}
	scalarWeights = extractWeights(scalars)

	bools = []scalarWeight{
		{1, scalarNoContext(makeColRef)},
		{1, scalarNoContext(makeAnd)},
		{1, scalarNoContext(makeOr)},
		{1, scalarNoContext(makeNot)},
		{1, scalarNoContext(makeCompareOp)},
		{1, scalarNoContext(makeIn)},
		{1, scalarNoContext(makeStringComparison)},
		{1, func(s *scope, ctx Context, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeScalar(s, typ, refs), true
		}},
		{1, scalarNoContext(makeExists)},
		{1, makeFunc},
	}
	boolWeights = extractWeights(bools)
}

// TODO(mjibson): remove this and correctly pass around the Context.
func scalarNoContext(fn func(*scope, *types.T, colRefs) (tree.TypedExpr, bool)) scalarFn {
	return func(s *scope, ctx Context, t *types.T, refs colRefs) (tree.TypedExpr, bool) {
		return fn(s, t, refs)
	}
}

type scalarFn func(*scope, Context, *types.T, colRefs) (expr tree.TypedExpr, ok bool)

type scalarWeight struct {
	weight int
	fn     scalarFn
}

func extractWeights(weights []scalarWeight) []int {
	w := make([]int, len(weights))
	for i, s := range weights {
		w[i] = s.weight
	}
	return w
}

// makeScalar attempts to construct a scalar expression of the requested type.
// If it was unsuccessful, it will return false.
func makeScalar(s *scope, typ *types.T, refs colRefs) tree.TypedExpr {
	return makeScalarContext(s, emptyCtx, typ, refs)
}

func makeScalarContext(s *scope, ctx Context, typ *types.T, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.schema.scalars, scalars, s, ctx, typ, refs)
}

func makeBoolExpr(s *scope, refs colRefs) tree.TypedExpr {
	return makeBoolExprContext(s, emptyCtx, refs)
}

func makeBoolExprContext(s *scope, ctx Context, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.schema.bools, bools, s, ctx, types.Bool, refs)
}

func makeScalarSample(
	sampler *WeightedSampler,
	weights []scalarWeight,
	s *scope,
	ctx Context,
	typ *types.T,
	refs colRefs,
) tree.TypedExpr {
	// If we are in a GROUP BY, attempt to find an aggregate function.
	if ctx.fnClass == tree.AggregateClass {
		if expr, ok := makeFunc(s, ctx, typ, refs); ok {
			return expr
		}
	}
	if s.canRecurse() {
		for {
			// No need for a retry counter here because makeConstExpr well eventually
			// be called and it always succeeds.
			idx := sampler.Next()
			result, ok := weights[idx].fn(s, ctx, typ, refs)
			if ok {
				return result
			}
		}
	}
	// Sometimes try to find a col ref or a const if there's no columns
	// with a matching type.
	if s.coin() {
		if expr, ok := makeColRef(s, typ, refs); ok {
			return expr
		}
	}
	return makeConstExpr(s, typ, refs)
}

func makeCaseExpr(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(s, typ)
	condition := makeScalar(s, types.Bool, refs)
	trueExpr := makeScalar(s, typ, refs)
	falseExpr := makeScalar(s, typ, refs)
	expr, err := tree.NewTypedCaseExpr(
		nil,
		[]*tree.When{{
			Cond: condition,
			Val:  trueExpr,
		}},
		falseExpr,
		typ,
	)
	return expr, err == nil
}

func makeCoalesceExpr(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(s, typ)
	firstExpr := makeScalar(s, typ, refs)
	secondExpr := makeScalar(s, typ, refs)
	return tree.NewTypedCoalesceExpr(
		tree.TypedExprs{
			firstExpr,
			secondExpr,
		},
		typ,
	), true
}

func makeConstExpr(s *scope, typ *types.T, refs colRefs) tree.TypedExpr {
	typ = pickAnyType(s, typ)

	if s.schema.avoidConsts {
		if expr, ok := makeColRef(s, typ, refs); ok {
			return expr
		}
	}

	var datum tree.Datum
	s.schema.lock.Lock()
	datum = sqlbase.RandDatumWithNullChance(s.schema.rnd, typ, 6)
	if f := datum.ResolvedType().Family(); f != types.UnknownFamily && s.schema.simpleDatums {
		switch f {
		case types.TupleFamily:
			// TODO(mjibson): improve
			datum = tree.DNull
		case types.StringFamily:
			p := make([]byte, s.schema.rnd.Intn(5))
			for i := range p {
				p[i] = byte('A' + s.schema.rnd.Intn(26))
			}
			datum = tree.NewDString(string(p))
		case types.BytesFamily:
			p := make([]byte, s.schema.rnd.Intn(5))
			for i := range p {
				p[i] = byte('A' + s.schema.rnd.Intn(26))
			}
			datum = tree.NewDBytes(tree.DBytes(p))
		case types.IntervalFamily:
			datum = &tree.DInterval{Duration: duration.MakeDuration(
				s.schema.rnd.Int63n(3),
				s.schema.rnd.Int63n(3),
				s.schema.rnd.Int63n(3),
			)}
		case types.JsonFamily:
			p := make([]byte, s.schema.rnd.Intn(5))
			for i := range p {
				p[i] = byte('A' + s.schema.rnd.Intn(26))
			}
			datum = tree.NewDJSON(json.FromString(string(p)))
		case types.TimestampFamily:
			datum = tree.MakeDTimestamp(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
		case types.TimestampTZFamily:
			datum = tree.MakeDTimestampTZ(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
		}
	}
	s.schema.lock.Unlock()

	return datum
}

func makeColRef(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	expr, _, ok := getColRef(s, typ, refs)
	return expr, ok
}

func getColRef(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, *colRef, bool) {
	// Filter by needed type.
	cols := make(colRefs, 0, len(refs))
	for _, c := range refs {
		if typ.Family() == types.AnyFamily || c.typ.Equivalent(typ) {
			cols = append(cols, c)
		}
	}
	if len(cols) == 0 {
		return nil, nil, false
	}
	col := cols[s.schema.rnd.Intn(len(cols))]
	return makeTypedExpr(
		col.item,
		col.typ,
	), col, true
}

// castType tries to wrap expr in a CastExpr. This can be useful for times
// when operators or functions have ambiguous implementations (i.e., string
// or bytes, timestamp or timestamptz) and a cast will inform which one to use.
func castType(expr tree.TypedExpr, typ *types.T) tree.TypedExpr {
	// If target type involves ANY, then no cast needed.
	if typ.IsAmbiguous() {
		return expr
	}
	return makeTypedExpr(&tree.CastExpr{
		Expr:       expr,
		Type:       typ,
		SyntaxMode: tree.CastShort,
	}, typ)
}

func typedParen(expr tree.TypedExpr, typ *types.T) tree.TypedExpr {
	return makeTypedExpr(&tree.ParenExpr{Expr: expr}, typ)
}

func makeOr(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	left := makeBoolExpr(s, refs)
	right := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedAndExpr(left, right), types.Bool), true
}

func makeAnd(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	left := makeBoolExpr(s, refs)
	right := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedOrExpr(left, right), types.Bool), true
}

func makeNot(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	expr := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedNotExpr(expr), types.Bool), true
}

// TODO(mjibson): add the other operators somewhere.
var compareOps = [...]tree.ComparisonOperator{
	tree.EQ,
	tree.LT,
	tree.GT,
	tree.LE,
	tree.GE,
	tree.NE,
	tree.IsDistinctFrom,
	tree.IsNotDistinctFrom,
}

func makeCompareOp(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(s, typ)
	op := compareOps[s.schema.rnd.Intn(len(compareOps))]
	left := makeScalar(s, typ, refs)
	right := makeScalar(s, typ, refs)
	return typedParen(tree.NewTypedComparisonExpr(op, left, right), typ), true
}

func makeBinOp(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(s, typ)
	ops := operators[typ.Oid()]
	if len(ops) == 0 {
		return nil, false
	}
	n := s.schema.rnd.Intn(len(ops))
	op := ops[n]
	left := makeScalar(s, op.LeftType, refs)
	right := makeScalar(s, op.RightType, refs)
	return castType(
		typedParen(
			tree.NewTypedBinaryExpr(op.Operator, castType(left, op.LeftType), castType(right, op.RightType), typ),
			typ,
		),
		typ,
	), true
}

func makeFunc(s *scope, ctx Context, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(s, typ)

	class := ctx.fnClass
	// Turn off window functions most of the time because they are
	// enabled for the entire select exprs instead of on a per-expr
	// basis.
	if class == tree.WindowClass && s.d6() != 1 {
		class = tree.NormalClass
	}
	fns := functions[class][typ.Oid()]
	if len(fns) == 0 {
		return nil, false
	}
	fn := fns[s.schema.rnd.Intn(len(fns))]
	if s.schema.disableImpureFns && fn.def.Impure {
		return nil, false
	}
	for _, ignore := range s.schema.ignoreFNs {
		if ignore.MatchString(fn.def.Name) {
			return nil, false
		}
	}

	args := make(tree.TypedExprs, 0)
	for _, argTyp := range fn.overload.Types.Types() {
		var arg tree.TypedExpr
		// If we're a GROUP BY or window function, try to choose a col ref for the arguments.
		if class == tree.AggregateClass || class == tree.WindowClass {
			var ok bool
			arg, ok = makeColRef(s, argTyp, refs)
			if !ok {
				// If we can't find a col ref for our
				// aggregate function, try again with
				// a non-aggregate.
				return makeFunc(s, emptyCtx, typ, refs)
			}
		}
		if arg == nil {
			arg = makeScalar(s, argTyp, refs)
		}
		args = append(args, castType(arg, argTyp))
	}

	var window *tree.WindowDef
	// Use a window function if:
	// - we chose an aggregate function, then 1/6 chance, but not if we're in a HAVING (noWindow == true)
	// - we explicitly chose a window function
	if fn.def.Class == tree.WindowClass || (!ctx.noWindow && s.d6() == 1 && fn.def.Class == tree.AggregateClass) {
		var parts tree.Exprs
		s.schema.sample(len(refs), 2, func(i int) {
			parts = append(parts, refs[i].item)
		})
		var order tree.OrderBy
		s.schema.sample(len(refs)-len(parts), 2, func(i int) {
			order = append(order, &tree.Order{
				Expr:      refs[i+len(parts)].item,
				Direction: s.schema.randDirection(),
			})
		})
		var frame *tree.WindowFrame
		if s.coin() {
			frame = makeWindowFrame(s, refs)
		}
		window = &tree.WindowDef{
			Partitions: parts,
			OrderBy:    order,
			Frame:      frame,
		}
	}

	// Cast the return and arguments to prevent ambiguity during function
	// implementation choosing.
	return castType(tree.NewTypedFuncExpr(
		tree.ResolvableFunctionReference{FunctionReference: fn.def},
		0, /* aggQualifier */
		args,
		nil, /* filter */
		window,
		typ,
		&fn.def.FunctionProperties,
		fn.overload,
	), typ), true
}

var windowFrameModes = []tree.WindowFrameMode{
	tree.RANGE,
	tree.ROWS,
	tree.GROUPS,
}

func randWindowFrameMode(s *scope) tree.WindowFrameMode {
	return windowFrameModes[s.schema.rnd.Intn(len(windowFrameModes))]
}

func makeWindowFrame(s *scope, refs colRefs) *tree.WindowFrame {
	// Window frame mode and start bound must always be present whereas end
	// bound can be omitted.
	frameMode := randWindowFrameMode(s)
	var startBound tree.WindowFrameBound
	var endBound *tree.WindowFrameBound
	if frameMode == tree.RANGE {
		// RANGE mode is special in that if a bound is of type OffsetPreceding or
		// OffsetFollowing, it requires that ORDER BY clause of the window function
		// have exactly one column that can be only of the following types:
		// DInt, DFloat, DDecimal, DInterval; so for now let's avoid this
		// complication and not choose offset bound types.
		// TODO(yuzefovich): fix this.
		if s.coin() {
			startBound.BoundType = tree.UnboundedPreceding
		} else {
			startBound.BoundType = tree.CurrentRow
		}
		if s.coin() {
			endBound = new(tree.WindowFrameBound)
			if s.coin() {
				endBound.BoundType = tree.CurrentRow
			} else {
				endBound.BoundType = tree.UnboundedFollowing
			}
		}
	} else {
		// There are 5 bound types, but only 4 can be used for the start bound.
		startBound.BoundType = tree.WindowFrameBoundType(s.schema.rnd.Intn(4))
		if startBound.BoundType == tree.OffsetFollowing {
			// With OffsetFollowing as the start bound, the end bound must be
			// present and can either be OffsetFollowing or UnboundedFollowing.
			endBound = new(tree.WindowFrameBound)
			if s.coin() {
				endBound.BoundType = tree.OffsetFollowing
			} else {
				endBound.BoundType = tree.UnboundedFollowing
			}
		}
		if endBound == nil && s.coin() {
			endBound = new(tree.WindowFrameBound)
			// endBound cannot be "smaller" than startBound, so we will "prohibit" all
			// such choices.
			endBoundProhibitedChoices := int(startBound.BoundType)
			if startBound.BoundType == tree.UnboundedPreceding {
				// endBound cannot be UnboundedPreceding, so we always need to skip that
				// choice.
				endBoundProhibitedChoices = 1
			}
			endBound.BoundType = tree.WindowFrameBoundType(endBoundProhibitedChoices + s.schema.rnd.Intn(5-endBoundProhibitedChoices))
		}
		// We will set offsets regardless of the bound type, but they will only be
		// used when a bound is either OffsetPreceding or OffsetFollowing. Both
		// ROWS and GROUPS mode need non-negative integers as bounds.
		startBound.OffsetExpr = makeScalar(s, types.Int, refs)
		if endBound != nil {
			endBound.OffsetExpr = makeScalar(s, types.Int, refs)
		}
	}
	return &tree.WindowFrame{
		Mode: frameMode,
		Bounds: tree.WindowFrameBounds{
			StartBound: &startBound,
			EndBound:   endBound,
		},
	}
}

func makeExists(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}

	selectStmt, _, ok := s.makeSelect(makeDesiredTypes(s), refs)
	if !ok {
		return nil, false
	}

	subq := &tree.Subquery{
		Select: &tree.ParenSelect{Select: selectStmt},
		Exists: true,
	}
	subq.SetType(types.Bool)
	return subq, true
}

func makeIn(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}

	t := sqlbase.RandScalarType(s.schema.rnd)
	var rhs tree.TypedExpr
	if s.coin() {
		rhs = makeTuple(s, t, refs)
	} else {
		selectStmt, _, ok := s.makeSelect([]*types.T{t}, refs)
		if !ok {
			return nil, false
		}
		// This sometimes produces `SELECT NULL ...`. Cast the
		// first expression so IN succeeds.
		clause := selectStmt.Select.(*tree.SelectClause)
		clause.Exprs[0].Expr = &tree.CastExpr{
			Expr:       clause.Exprs[0].Expr,
			Type:       t,
			SyntaxMode: tree.CastShort,
		}
		subq := &tree.Subquery{
			Select: &tree.ParenSelect{Select: selectStmt},
		}
		subq.SetType(types.MakeTuple([]types.T{*t}))
		rhs = subq
	}
	op := tree.In
	if s.coin() {
		op = tree.NotIn
	}
	return tree.NewTypedComparisonExpr(
		op,
		// Cast any NULLs to a concrete type.
		castType(makeScalar(s, t, refs), t),
		rhs,
	), true
}

func makeStringComparison(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	return tree.NewTypedComparisonExpr(
		s.schema.randStringComparison(),
		makeScalar(s, types.String, refs),
		makeScalar(s, types.String, refs),
	), true
}

func makeTuple(s *scope, typ *types.T, refs colRefs) *tree.Tuple {
	n := s.schema.rnd.Intn(5)
	// Don't allow empty tuples in simple/postgres mode.
	if s.schema.simpleDatums && n == 0 {
		n++
	}
	exprs := make(tree.Exprs, n)
	for i := range exprs {
		exprs[i] = makeScalar(s, typ, refs)
	}
	return tree.NewTypedTuple(types.MakeTuple([]types.T{*typ}), exprs)
}

func makeScalarSubquery(s *scope, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	if s.schema.disableLimits {
		// This query must use a LIMIT, so bail if they are disabled.
		return nil, false
	}
	selectStmt, _, ok := s.makeSelect([]*types.T{typ}, refs)
	if !ok {
		return nil, false
	}
	selectStmt.Limit = &tree.Limit{Count: tree.NewDInt(1)}

	subq := &tree.Subquery{
		Select: &tree.ParenSelect{Select: selectStmt},
	}
	subq.SetType(typ)

	return subq, true
}
