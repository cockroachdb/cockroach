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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
) (memo.ScalarListExpr, types.T) {
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
	typ := types.TTuple{Types: make([]types.T, n)}
	for i := range newList {
		typ.Types[i] = newList[i].DataType()
	}

	return newList, typ
}

// SimplifyCoalesce discards any leading null operands, and then if the next
// operand is a constant, replaces with that constant.
func (c *CustomFuncs) SimplifyCoalesce(args memo.ScalarListExpr) opt.ScalarExpr {
	for i := 0; i < len(args)-1; i++ {
		item := args[i]

		// If item is not a constant value, then its value may turn out to be
		// null, so no more folding. Return operands from then on.
		if !c.IsConstValueOrTuple(item) {
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

// IsJSONScalar returns if the JSON value is a number, string, true, false, or null.
func (c *CustomFuncs) IsJSONScalar(value opt.ScalarExpr) bool {
	v := value.(*memo.ConstExpr).Value.(*tree.DJSON)
	return v.JSON.Type() != json.ObjectJSONType && v.JSON.Type() != json.ArrayJSONType
}

// MakeSingleKeyJSONObject returns a JSON object with one entry, mapping key to value.
func (c *CustomFuncs) MakeSingleKeyJSONObject(key, value opt.ScalarExpr) opt.ScalarExpr {
	k := key.(*memo.ConstExpr).Value.(*tree.DString)
	v := value.(*memo.ConstExpr).Value.(*tree.DJSON)

	builder := json.NewObjectBuilder(1)
	builder.Add(string(*k), v.JSON)
	j := builder.Build()

	return c.f.ConstructConst(&tree.DJSON{JSON: j})
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
		panic(pgerror.NewAssertionErrorf("unexpected Op type: %v", log.Safe(op1)))
	}
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
func (c *CustomFuncs) ensureTyped(d opt.ScalarExpr, typ types.T) opt.ScalarExpr {
	if d.DataType() == types.Unknown {
		return c.f.ConstructNull(typ)
	}
	return d
}

// OpsAreSame returns true if the two operators are the same.
func (c *CustomFuncs) OpsAreSame(left, right opt.Operator) bool {
	return left == right
}

// IsConstArray returns true if the expression is a constant array.
func (c *CustomFuncs) IsConstArray(scalar opt.ScalarExpr) bool {
	if cnst, ok := scalar.(*memo.ConstExpr); ok {
		if _, ok := cnst.Value.(*tree.DArray); ok {
			return true
		}
	}
	return false
}

// ConvertConstArrayToTuple converts a constant ARRAY datum to the equivalent
// homogeneous tuple, so ARRAY[1, 2, 3] becomes (1, 2, 3).
func (c *CustomFuncs) ConvertConstArrayToTuple(scalar opt.ScalarExpr) opt.ScalarExpr {
	darr := scalar.(*memo.ConstExpr).Value.(*tree.DArray)
	elems := make(memo.ScalarListExpr, len(darr.Array))
	ts := make([]types.T, len(darr.Array))
	for i, delem := range darr.Array {
		elems[i] = c.f.ConstructConstVal(delem, delem.ResolvedType())
		ts[i] = darr.ParamTyp
	}
	return c.f.ConstructTuple(elems, types.TTuple{Types: ts})
}

// CastToCollatedString returns the given string or collated string as a
// collated string constant with the given locale.
func (c *CustomFuncs) CastToCollatedString(str opt.ScalarExpr, locale string) opt.ScalarExpr {
	var value string
	switch t := str.(*memo.ConstExpr).Value.(type) {
	case *tree.DString:
		value = string(*t)
	case *tree.DCollatedString:
		value = t.Contents
	default:
		panic(pgerror.NewAssertionErrorf("unexpected type for COLLATE: %T", log.Safe(str.(*memo.ConstExpr).Value)))
	}

	d, err := tree.NewDCollatedString(value, locale, &c.f.evalCtx.CollationEnv)
	if err != nil {
		panic(err)
	}
	return c.f.ConstructConst(d)
}

// MakeUnorderedSubquery returns a SubqueryPrivate that specifies no ordering.
func (c *CustomFuncs) MakeUnorderedSubquery() *memo.SubqueryPrivate {
	return &memo.SubqueryPrivate{}
}

// SubqueryOrdering returns the ordering property on a SubqueryPrivate.
func (c *CustomFuncs) SubqueryOrdering(sub *memo.SubqueryPrivate) physical.OrderingChoice {
	var oc physical.OrderingChoice
	oc.FromOrdering(sub.Ordering)
	return oc
}

// MakeArrayAggCol returns a ColPrivate with the given type and an "array_agg" label.
func (c *CustomFuncs) MakeArrayAggCol(typ types.T) *memo.ColPrivate {
	return &memo.ColPrivate{Col: c.mem.Metadata().AddColumn("array_agg", typ)}
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
