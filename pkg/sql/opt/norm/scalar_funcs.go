// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// NeedSortedUniqueList returns true if the given list is composed entirely of
// constant values that are either not in sorted order or have duplicates. If
// true, then ConstructSortedUniqueList needs to be called on the list to
// normalize it.
func (c *CustomFuncs) NeedSortedUniqueList(list memo.ScalarListExpr) bool {
	if len(list) <= 1 {
		return false
	}
	ls := listSorter{cf: c, list: list}
	var needSortedUniqueList bool
	for i, item := range list {
		if !opt.IsConstValueOp(item) {
			return false
		}
		if i != 0 && !ls.less(i-1, i) {
			needSortedUniqueList = true
		}
	}
	return needSortedUniqueList
}

// ConstructSortedUniqueList sorts the given list and removes duplicates, and
// returns the resulting list. See the comment for listSorter.compare for
// comparison rule details.
func (c *CustomFuncs) ConstructSortedUniqueList(
	list memo.ScalarListExpr,
) (memo.ScalarListExpr, *types.T) {
	// Make a copy of the list, since it needs to stay immutable.
	newList := make(memo.ScalarListExpr, len(list))
	copy(newList, list)
	ls := listSorter{cf: c, list: newList}

	// Sort the list.
	sort.Slice(ls.list, ls.less)

	// Remove duplicates from the list.
	n := 0
	for i := range newList {
		if i == 0 || ls.compare(i-1, i) < 0 {
			newList[n] = newList[i]
			n++
		}
	}
	newList = newList[:n]

	// Construct the type of the tuple.
	contents := make([]*types.T, n)
	for i := range newList {
		contents[i] = newList[i].DataType()
	}
	return newList, types.MakeTuple(contents)
}

// SimplifyCoalesce discards any leading null operands, and then if the next
// operand is a constant, replaces with that constant.
func (c *CustomFuncs) SimplifyCoalesce(args memo.ScalarListExpr) opt.ScalarExpr {
	for i := 0; i < len(args)-1; i++ {
		item := args[i]

		// If item is not a constant value, then its value may turn out to be
		// null, so no more folding. Return operands from then on.
		if !c.IsConstValueOrGroupOfConstValues(item) {
			return c.f.ConstructCoalesce(args[i:])
		}

		if item.Op() != opt.NullOp {
			return item
		}
	}

	// All operands up to the last were null (or the last is the only operand),
	// so return the last operand without the wrapping COALESCE function.
	return args[len(args)-1]
}

// IsConstValueEqual returns whether const1 and const2 are equal.
func (c *CustomFuncs) IsConstValueEqual(const1, const2 opt.ScalarExpr) bool {
	op1 := const1.Op()
	op2 := const2.Op()
	if op1 != op2 || op1 == opt.NullOp {
		return false
	}
	switch op1 {
	case opt.TrueOp, opt.FalseOp:
		return true
	case opt.ConstOp:
		datum1 := const1.(*memo.ConstExpr).Value
		datum2 := const2.(*memo.ConstExpr).Value
		return datum1.Compare(c.f.evalCtx, datum2) == 0
	default:
		panic(errors.AssertionFailedf("unexpected Op type: %v", redact.Safe(op1)))
	}
}

// UnifyComparison attempts to convert a constant expression to the type of the
// variable expression, if that conversion can round-trip and is monotonic.
// Otherwise it returns ok=false.
func (c *CustomFuncs) UnifyComparison(
	v *memo.VariableExpr, cnst *memo.ConstExpr,
) (_ opt.ScalarExpr, ok bool) {
	desiredType := v.DataType()
	originalType := cnst.DataType()

	// Don't bother if they're already the same.
	if desiredType.Equivalent(originalType) {
		return nil, false
	}

	if !isMonotonicConversion(originalType, desiredType) {
		return nil, false
	}

	// Check that the datum can round-trip between the types. If this is true, it
	// means we don't lose any information needed to generate spans, and combined
	// with monotonicity means that it's safe to convert the RHS to the type of
	// the LHS.
	convertedDatum, err := tree.PerformCast(c.f.evalCtx, cnst.Value, desiredType)
	if err != nil {
		return nil, false
	}

	convertedBack, err := tree.PerformCast(c.f.evalCtx, convertedDatum, originalType)
	if err != nil {
		return nil, false
	}

	if convertedBack.Compare(c.f.evalCtx, cnst.Value) != 0 {
		return nil, false
	}

	return c.f.ConstructConst(convertedDatum, desiredType), true
}

// SimplifyWhens removes known unreachable WHEN cases and constructs a new CASE
// statement. Any known true condition is converted to the ELSE. If only the
// ELSE remains, its expression is returned. condition must be a ConstValue.
func (c *CustomFuncs) SimplifyWhens(
	condition opt.ScalarExpr, whens memo.ScalarListExpr, orElse opt.ScalarExpr,
) opt.ScalarExpr {
	newWhens := make(memo.ScalarListExpr, 0, len(whens))
	for _, item := range whens {
		when := item.(*memo.WhenExpr)
		if opt.IsConstValueOp(when.Condition) {
			if !c.IsConstValueEqual(condition, when.Condition) {
				// Ignore known unmatching conditions.
				continue
			}

			// If this is true, we won't ever match anything else, so convert this to
			// the ELSE (or just return it if there are no earlier items).
			if len(newWhens) == 0 {
				return c.ensureTyped(when.Value, memo.InferWhensType(whens, orElse))
			}
			return c.f.ConstructCase(condition, newWhens, when.Value)
		}

		newWhens = append(newWhens, when)
	}

	// The ELSE value.
	if len(newWhens) == 0 {
		// ELSE is the only clause (there are no WHENs), remove the CASE.
		// NULLs in this position will not be typed, so we tag them with
		// a type we observed earlier.
		// typ will never be nil here because the definition of
		// SimplifyCaseWhenConstValue ensures that whens is nonempty.
		return c.ensureTyped(orElse, memo.InferWhensType(whens, orElse))
	}

	return c.f.ConstructCase(condition, newWhens, orElse)
}

// ensureTyped makes sure that any NULL passing through gets tagged with an
// appropriate type.
func (c *CustomFuncs) ensureTyped(d opt.ScalarExpr, typ *types.T) opt.ScalarExpr {
	if d.DataType().Family() == types.UnknownFamily {
		return c.f.ConstructNull(typ)
	}
	return d
}

// OpsAreSame returns true if the two operators are the same.
func (c *CustomFuncs) OpsAreSame(left, right opt.Operator) bool {
	return left == right
}

// ConvertConstArrayToTuple converts a constant ARRAY datum to the equivalent
// homogeneous tuple, so ARRAY[1, 2, 3] becomes (1, 2, 3).
func (c *CustomFuncs) ConvertConstArrayToTuple(scalar opt.ScalarExpr) opt.ScalarExpr {
	darr := scalar.(*memo.ConstExpr).Value.(*tree.DArray)
	elems := make(memo.ScalarListExpr, len(darr.Array))
	ts := make([]*types.T, len(darr.Array))
	for i, delem := range darr.Array {
		elems[i] = c.f.ConstructConstVal(delem, delem.ResolvedType())
		ts[i] = darr.ParamTyp
	}
	return c.f.ConstructTuple(elems, types.MakeTuple(ts))
}

// CastToCollatedString returns the given string or collated string as a
// collated string constant with the given locale.
func (c *CustomFuncs) CastToCollatedString(str opt.ScalarExpr, locale string) opt.ScalarExpr {
	datum := str.(*memo.ConstExpr).Value
	if wrap, ok := datum.(*tree.DOidWrapper); ok {
		datum = wrap.Wrapped
	}

	var value string
	switch t := datum.(type) {
	case *tree.DString:
		value = string(*t)
	case *tree.DCollatedString:
		value = t.Contents
	default:
		panic(errors.AssertionFailedf("unexpected type for COLLATE: %T", t))
	}

	d, err := tree.NewDCollatedString(value, locale, &c.f.evalCtx.CollationEnv)
	if err != nil {
		panic(err)
	}
	return c.f.ConstructConst(d, types.MakeCollatedString(str.DataType(), locale))
}

// MakeUnorderedSubquery returns a SubqueryPrivate that specifies no ordering.
func (c *CustomFuncs) MakeUnorderedSubquery() *memo.SubqueryPrivate {
	return &memo.SubqueryPrivate{}
}

// SubqueryOrdering returns the ordering property on a SubqueryPrivate.
func (c *CustomFuncs) SubqueryOrdering(sub *memo.SubqueryPrivate) props.OrderingChoice {
	var oc props.OrderingChoice
	oc.FromOrdering(sub.Ordering)
	return oc
}

// SubqueryRequestedCol returns the requested column from a SubqueryPrivate.
// This function should only be used with ArrayFlatten expressions.
func (c *CustomFuncs) SubqueryRequestedCol(sub *memo.SubqueryPrivate) opt.ColumnID {
	return sub.RequestedCol
}

// SubqueryCmp returns the comparison operation from a SubqueryPrivate.
func (c *CustomFuncs) SubqueryCmp(sub *memo.SubqueryPrivate) opt.Operator {
	return sub.Cmp
}

// MakeArrayAggCol returns a ColumnID with the given type and an "array_agg"
// label.
func (c *CustomFuncs) MakeArrayAggCol(typ *types.T) opt.ColumnID {
	return c.mem.Metadata().AddColumn("array_agg", typ)
}

// IsLimited indicates whether a limit was pushed under the subquery
// already. See e.g. the rule IntroduceExistsLimit.
func (c *CustomFuncs) IsLimited(sub *memo.SubqueryPrivate) bool {
	return sub.WasLimited
}

// MakeLimited specifies that the subquery has a limit set
// already. This prevents e.g. the rule IntroduceExistsLimit from
// applying twice.
func (c *CustomFuncs) MakeLimited(sub *memo.SubqueryPrivate) *memo.SubqueryPrivate {
	newSub := *sub
	newSub.WasLimited = true
	return &newSub
}

// InlineValues converts a Values operator to a tuple. If there are
// multiple columns, the result is a tuple of tuples.
func (c *CustomFuncs) InlineValues(v memo.RelExpr) *memo.TupleExpr {
	values := v.(*memo.ValuesExpr)
	md := c.mem.Metadata()
	if len(values.Cols) > 1 {
		colTypes := make([]*types.T, len(values.Cols))
		for i, colID := range values.Cols {
			colTypes[i] = md.ColumnMeta(colID).Type
		}
		// Inlining a multi-column VALUES results in a tuple of tuples. Example:
		//
		//   (a,b) IN (VALUES (1,1), (2,2))
		// =>
		//   (a,b) IN ((1,1), (2,2))
		return &memo.TupleExpr{
			Elems: values.Rows,
			Typ:   types.MakeTuple([]*types.T{types.MakeTuple(colTypes)}),
		}
	}
	// Inlining a sngle-column VALUES results in a simple tuple. Example:
	//   a IN (VALUES (1), (2))
	// =>
	//   a IN (1, 2)
	colType := md.ColumnMeta(values.Cols[0]).Type
	tuple := &memo.TupleExpr{
		Elems: make(memo.ScalarListExpr, len(values.Rows)),
		Typ:   types.MakeTuple([]*types.T{colType}),
	}
	for i := range values.Rows {
		tuple.Elems[i] = values.Rows[i].(*memo.TupleExpr).Elems[0]
	}
	return tuple
}

// IsTupleOfVars returns true if the given tuple contains Variables
// corresponding to the given columns (in the same order).
func (c *CustomFuncs) IsTupleOfVars(t *memo.TupleExpr, cols opt.ColList) bool {
	if len(t.Elems) != len(cols) {
		return false
	}
	for i := range t.Elems {
		v, ok := t.Elems[i].(*memo.VariableExpr)
		if !ok || v.Col != cols[i] {
			return false
		}
	}
	return true
}

// VarsAreSame returns true if the two variables are the same.
func (c *CustomFuncs) VarsAreSame(left, right *memo.VariableExpr) bool {
	return left.Col == right.Col
}

// EqualsColumn returns true if the two column IDs are the same.
func (c *CustomFuncs) EqualsColumn(left, right opt.ColumnID) bool {
	return left == right
}

// TuplesHaveSameLength returns true if two tuples have the same number of
// elements.
func (c *CustomFuncs) TuplesHaveSameLength(a, b *memo.TupleExpr) bool {
	return len(a.Elems) == len(b.Elems)
}

// SplitTupleEq splits an equality condition between two tuples into multiple
// equalities, one for each tuple column.
func (c *CustomFuncs) SplitTupleEq(lhs, rhs *memo.TupleExpr) memo.FiltersExpr {
	if len(lhs.Elems) != len(rhs.Elems) {
		panic(errors.AssertionFailedf("unequal tuple lengths"))
	}
	res := make(memo.FiltersExpr, len(lhs.Elems))
	for i := range res {
		res[i] = c.f.ConstructFiltersItem(c.f.ConstructEq(lhs.Elems[i], rhs.Elems[i]))
	}
	return res
}
