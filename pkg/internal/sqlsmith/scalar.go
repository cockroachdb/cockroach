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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var (
	scalars = []scalarExprWeight{
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
		{10, func(s *Smither, ctx Context, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeConstExpr(s, typ, refs), true
		}},
	}

	bools = []scalarExprWeight{
		{1, scalarNoContext(makeColRef)},
		{1, scalarNoContext(makeAnd)},
		{1, scalarNoContext(makeOr)},
		{1, scalarNoContext(makeNot)},
		{1, scalarNoContext(makeCompareOp)},
		{1, scalarNoContext(makeIn)},
		{1, scalarNoContext(makeStringComparison)},
		{1, func(s *Smither, ctx Context, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeScalar(s, typ, refs), true
		}},
		{1, scalarNoContext(makeExists)},
		{1, makeFunc},
	}
)

// TODO(mjibson): remove this and correctly pass around the Context.
func scalarNoContext(fn func(*Smither, *types.T, colRefs) (tree.TypedExpr, bool)) scalarExpr {
	return func(s *Smither, ctx Context, t *types.T, refs colRefs) (tree.TypedExpr, bool) {
		return fn(s, t, refs)
	}
}

// makeScalar attempts to construct a scalar expression of the requested type.
// If it was unsuccessful, it will return false.
func makeScalar(s *Smither, typ *types.T, refs colRefs) tree.TypedExpr {
	return makeScalarContext(s, emptyCtx, typ, refs)
}

func makeScalarContext(s *Smither, ctx Context, typ *types.T, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.scalarExprSampler, s, ctx, typ, refs)
}

func makeBoolExpr(s *Smither, refs colRefs) tree.TypedExpr {
	return makeBoolExprContext(s, emptyCtx, refs)
}

func makeBoolExprWithPlaceholders(s *Smither, refs colRefs) (tree.Expr, []interface{}) {
	expr := makeBoolExprContext(s, emptyCtx, refs)

	// Replace constants with placeholders if the type is numeric or bool.
	visitor := replaceDatumPlaceholderVisitor{}
	exprFmt := expr.Walk(&visitor)
	return exprFmt, visitor.Args
}

func makeBoolExprContext(s *Smither, ctx Context, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.boolExprSampler, s, ctx, types.Bool, refs)
}

func makeScalarSample(
	sampler *scalarExprSampler, s *Smither, ctx Context, typ *types.T, refs colRefs,
) tree.TypedExpr {
	// If we are in a GROUP BY, attempt to find an aggregate function.
	if ctx.fnClass == tree.AggregateClass {
		if expr, ok := makeFunc(s, ctx, typ, refs); ok {
			return expr
		}
	}
	if s.canRecurse() {
		for {
			// No need for a retry counter here because makeConstExpr will eventually
			// be called and it always succeeds.
			result, ok := sampler.Next()(s, ctx, typ, refs)
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

func makeCaseExpr(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = s.pickAnyType(typ)
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

func makeCoalesceExpr(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = s.pickAnyType(typ)
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

func makeConstExpr(s *Smither, typ *types.T, refs colRefs) tree.TypedExpr {
	typ = s.pickAnyType(typ)

	if s.avoidConsts {
		if expr, ok := makeColRef(s, typ, refs); ok {
			return expr
		}
	}

	expr := tree.TypedExpr(makeConstDatum(s, typ))
	// In Postgres mode, make sure the datum is resolved as the type we want.
	// CockroachDB and Postgres differ in how constants are typed otherwise.
	if s.postgres {
		// Casts to REGTYPE, REGCLASS, etc are not deterministic since they
		// involve OID->name resolution, and the OIDs will not match across
		// two different databases.
		if typ.Family() == types.OidFamily {
			typ = types.Oid
		}
		expr = tree.NewTypedCastExpr(expr, typ)
	}
	return expr
}

func makeConstDatum(s *Smither, typ *types.T) tree.Datum {
	var datum tree.Datum
	s.lock.Lock()
	datum = randgen.RandDatumWithNullChance(s.rnd, typ, 6)
	if f := datum.ResolvedType().Family(); f != types.UnknownFamily && s.simpleDatums {
		datum = randgen.RandDatumSimple(s.rnd, typ)
	}
	s.lock.Unlock()

	return datum
}

func makeColRef(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	expr, _, ok := getColRef(s, typ, refs)
	return expr, ok
}

func getColRef(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, *colRef, bool) {
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
	col := cols[s.rnd.Intn(len(cols))]
	return col.typedExpr(), col, true
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

func makeOr(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	left := makeBoolExpr(s, refs)
	right := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedOrExpr(left, right), types.Bool), true
}

func makeAnd(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	left := makeBoolExpr(s, refs)
	right := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedAndExpr(left, right), types.Bool), true
}

func makeNot(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	expr := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedNotExpr(expr), types.Bool), true
}

// TODO(mjibson): add the other operators somewhere.
var compareOps = [...]treecmp.ComparisonOperatorSymbol{
	treecmp.EQ,
	treecmp.LT,
	treecmp.GT,
	treecmp.LE,
	treecmp.GE,
	treecmp.NE,
	treecmp.IsDistinctFrom,
	treecmp.IsNotDistinctFrom,
}

func makeCompareOp(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	if f := typ.Family(); f != types.BoolFamily && f != types.AnyFamily && f != types.VoidFamily {
		return nil, false
	}
	typ = s.randScalarType()
	op := compareOps[s.rnd.Intn(len(compareOps))]
	if _, ok := tree.CmpOps[op].LookupImpl(typ, typ); !ok {
		return nil, false
	}
	left := makeScalar(s, typ, refs)
	right := makeScalar(s, typ, refs)
	return typedParen(tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(op), left, right), typ), true
}

func makeBinOp(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = s.pickAnyType(typ)
	ops := operators[typ.Oid()]
	if len(ops) == 0 {
		return nil, false
	}
	n := s.rnd.Intn(len(ops))
	op := ops[n]
	if s.postgres {
		if ignorePostgresBinOps[binOpTriple{
			op.LeftType.Family(),
			op.Operator.Symbol,
			op.RightType.Family(),
		}] {
			return nil, false
		}
	}
	if s.postgres {
		if transform, needTransform := postgresBinOpTransformations[binOpTriple{
			op.LeftType.Family(),
			op.Operator.Symbol,
			op.RightType.Family(),
		}]; needTransform {
			op.LeftType = transform.leftType
			op.RightType = transform.rightType
		}
	}
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

type binOpTriple struct {
	left  types.Family
	op    treebin.BinaryOperatorSymbol
	right types.Family
}

type binOpOperands struct {
	leftType  *types.T
	rightType *types.T
}

var ignorePostgresBinOps = map[binOpTriple]bool{
	// Integer division in cockroach returns a different type.
	{types.IntFamily, treebin.Div, types.IntFamily}: true,
	// Float * date isn't exact.
	{types.FloatFamily, treebin.Mult, types.DateFamily}: true,
	{types.DateFamily, treebin.Mult, types.FloatFamily}: true,
	{types.DateFamily, treebin.Div, types.FloatFamily}:  true,

	// Postgres does not have separate floor division operator.
	{types.IntFamily, treebin.FloorDiv, types.IntFamily}:         true,
	{types.FloatFamily, treebin.FloorDiv, types.FloatFamily}:     true,
	{types.DecimalFamily, treebin.FloorDiv, types.DecimalFamily}: true,
	{types.DecimalFamily, treebin.FloorDiv, types.IntFamily}:     true,
	{types.IntFamily, treebin.FloorDiv, types.DecimalFamily}:     true,

	{types.FloatFamily, treebin.Mod, types.FloatFamily}: true,
}

// For certain operations, Postgres is picky about the operand types.
var postgresBinOpTransformations = map[binOpTriple]binOpOperands{
	{types.IntFamily, treebin.Plus, types.DateFamily}:          {types.Int4, types.Date},
	{types.DateFamily, treebin.Plus, types.IntFamily}:          {types.Date, types.Int4},
	{types.IntFamily, treebin.Minus, types.DateFamily}:         {types.Int4, types.Date},
	{types.DateFamily, treebin.Minus, types.IntFamily}:         {types.Date, types.Int4},
	{types.JsonFamily, treebin.JSONFetchVal, types.IntFamily}:  {types.Jsonb, types.Int4},
	{types.JsonFamily, treebin.JSONFetchText, types.IntFamily}: {types.Jsonb, types.Int4},
	{types.JsonFamily, treebin.Minus, types.IntFamily}:         {types.Jsonb, types.Int4},
}

func makeFunc(s *Smither, ctx Context, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = s.pickAnyType(typ)

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
	fn := fns[s.rnd.Intn(len(fns))]
	if s.disableImpureFns && fn.overload.Volatility > tree.VolatilityImmutable {
		return nil, false
	}
	for _, ignore := range s.ignoreFNs {
		if ignore.MatchString(fn.def.Name) {
			return nil, false
		}
	}

	args := make(tree.TypedExprs, 0)
	for _, argTyp := range fn.overload.Types.Types() {
		// Postgres is picky about having Int4 arguments instead of Int8.
		if s.postgres && argTyp.Family() == types.IntFamily {
			argTyp = types.Int4
		}
		var arg tree.TypedExpr
		// If we're a GROUP BY or window function, try to choose a col ref for the arguments.
		if class == tree.AggregateClass || class == tree.WindowClass {
			var ok bool
			arg, ok = makeColRef(s, argTyp, refs)
			if !ok {
				// If we can't find a col ref for our aggregate function, just use a
				// constant.
				arg = makeConstExpr(s, typ, refs)
			}
		}
		if arg == nil {
			arg = makeScalar(s, argTyp, refs)
		}
		args = append(args, castType(arg, argTyp))
	}

	if fn.def.Class == tree.WindowClass && s.disableWindowFuncs {
		return nil, false
	}

	var window *tree.WindowDef
	// Use a window function if:
	// - we chose an aggregate function, then 1/6 chance, but not if we're in a HAVING (noWindow == true)
	// - we explicitly chose a window function
	if fn.def.Class == tree.WindowClass || (!s.disableWindowFuncs && !ctx.noWindow && s.d6() == 1 && fn.def.Class == tree.AggregateClass) {
		var parts tree.Exprs
		s.sample(len(refs), 2, func(i int) {
			parts = append(parts, refs[i].item)
		})
		var (
			order      tree.OrderBy
			orderTypes []*types.T
		)
		s.sample(len(refs)-len(parts), 2, func(i int) {
			ref := refs[i+len(parts)]
			order = append(order, &tree.Order{
				Expr:      ref.item,
				Direction: s.randDirection(),
			})
			orderTypes = append(orderTypes, ref.typ)
		})
		var frame *tree.WindowFrame
		if s.coin() {
			frame = makeWindowFrame(s, refs, orderTypes)
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

var windowFrameModes = []treewindow.WindowFrameMode{
	treewindow.RANGE,
	treewindow.ROWS,
	treewindow.GROUPS,
}

func randWindowFrameMode(s *Smither) treewindow.WindowFrameMode {
	return windowFrameModes[s.rnd.Intn(len(windowFrameModes))]
}

func makeWindowFrame(s *Smither, refs colRefs, orderTypes []*types.T) *tree.WindowFrame {
	var frameMode treewindow.WindowFrameMode
	for {
		frameMode = randWindowFrameMode(s)
		if len(orderTypes) > 0 || frameMode != treewindow.GROUPS {
			// GROUPS mode requires an ORDER BY clause, so if it is not present and
			// GROUPS mode was randomly chosen, we need to generate again; otherwise,
			// we're done.
			break
		}
	}
	// Window frame mode and start bound must always be present whereas end
	// bound can be omitted.
	var startBound tree.WindowFrameBound
	var endBound *tree.WindowFrameBound
	// RANGE mode is special in that if a bound is of type OffsetPreceding or
	// OffsetFollowing, it requires that ORDER BY clause of the window function
	// have exactly one column that can be only of the following types:
	// Int, Float, Decimal, Interval.
	allowRangeWithOffsets := false
	if len(orderTypes) == 1 {
		switch orderTypes[0].Family() {
		case types.IntFamily, types.FloatFamily,
			types.DecimalFamily, types.IntervalFamily:
			allowRangeWithOffsets = true
		}
	}
	if frameMode == treewindow.RANGE && !allowRangeWithOffsets {
		if s.coin() {
			startBound.BoundType = treewindow.UnboundedPreceding
		} else {
			startBound.BoundType = treewindow.CurrentRow
		}
		if s.coin() {
			endBound = new(tree.WindowFrameBound)
			if s.coin() {
				endBound.BoundType = treewindow.CurrentRow
			} else {
				endBound.BoundType = treewindow.UnboundedFollowing
			}
		}
	} else {
		// There are 5 bound types, but only 4 can be used for the start bound.
		startBound.BoundType = treewindow.WindowFrameBoundType(s.rnd.Intn(4))
		if startBound.BoundType == treewindow.OffsetFollowing {
			// With OffsetFollowing as the start bound, the end bound must be
			// present and can either be OffsetFollowing or UnboundedFollowing.
			endBound = new(tree.WindowFrameBound)
			if s.coin() {
				endBound.BoundType = treewindow.OffsetFollowing
			} else {
				endBound.BoundType = treewindow.UnboundedFollowing
			}
		}
		if endBound == nil && s.coin() {
			endBound = new(tree.WindowFrameBound)
			// endBound cannot be "smaller" than startBound, so we will "prohibit" all
			// such choices.
			endBoundProhibitedChoices := int(startBound.BoundType)
			if startBound.BoundType == treewindow.UnboundedPreceding {
				// endBound cannot be UnboundedPreceding, so we always need to skip that
				// choice.
				endBoundProhibitedChoices = 1
			}
			endBound.BoundType = treewindow.WindowFrameBoundType(endBoundProhibitedChoices + s.rnd.Intn(5-endBoundProhibitedChoices))
		}
		// We will set offsets regardless of the bound type, but they will only be
		// used when a bound is either OffsetPreceding or OffsetFollowing. Both
		// ROWS and GROUPS mode need non-negative integers as bounds whereas RANGE
		// mode takes the type as the single ORDER BY clause has.
		typ := types.Int
		if frameMode == treewindow.RANGE {
			typ = orderTypes[0]
		}
		startBound.OffsetExpr = makeScalar(s, typ, refs)
		if endBound != nil {
			endBound.OffsetExpr = makeScalar(s, typ, refs)
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

func makeExists(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}

	selectStmt, _, ok := s.makeSelect(s.makeDesiredTypes(), refs)
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

func makeIn(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}

	t := s.randScalarType()
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
		subq.SetType(types.MakeTuple([]*types.T{t}))
		rhs = subq
	}
	op := treecmp.In
	if s.coin() {
		op = treecmp.NotIn
	}
	return tree.NewTypedComparisonExpr(
		treecmp.MakeComparisonOperator(op),
		// Cast any NULLs to a concrete type.
		castType(makeScalar(s, t, refs), t),
		rhs,
	), true
}

func makeStringComparison(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	stringComparison := s.randStringComparison()
	switch typ.Family() {
	case types.BoolFamily, types.AnyFamily:
	default:
		return nil, false
	}
	return tree.NewTypedComparisonExpr(
		stringComparison,
		makeScalar(s, types.String, refs),
		makeScalar(s, types.String, refs),
	), true
}

func makeTuple(s *Smither, typ *types.T, refs colRefs) *tree.Tuple {
	n := s.rnd.Intn(5)
	// Don't allow empty tuples in simple/postgres mode.
	if n == 0 && s.simpleDatums {
		n++
	}
	exprs := make(tree.Exprs, n)
	for i := range exprs {
		if s.d9() == 1 {
			exprs[i] = makeConstDatum(s, typ)
		} else {
			exprs[i] = makeScalar(s, typ, refs)
		}
	}
	return tree.NewTypedTuple(types.MakeTuple([]*types.T{typ}), exprs)
}

func makeScalarSubquery(s *Smither, typ *types.T, refs colRefs) (tree.TypedExpr, bool) {
	if s.disableLimits {
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

// replaceDatumPlaceholderVisitor replaces occurrences of numeric and bool Datum
// expressions with placeholders, and updates Args with the corresponding Datum
// values. This is used to prepare and execute a statement with placeholders.
type replaceDatumPlaceholderVisitor struct {
	Args []interface{}
}

var _ tree.Visitor = &replaceDatumPlaceholderVisitor{}

// VisitPre satisfies the tree.Visitor interface.
func (v *replaceDatumPlaceholderVisitor) VisitPre(
	expr tree.Expr,
) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case tree.Datum:
		if t.ResolvedType().IsNumeric() || t.ResolvedType() == types.Bool {
			v.Args = append(v.Args, expr)
			placeholder, _ := tree.NewPlaceholder(strconv.Itoa(len(v.Args)))
			return false, placeholder
		}
		return false, expr
	}
	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*replaceDatumPlaceholderVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
