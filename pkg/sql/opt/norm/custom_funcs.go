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

package norm

import (
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// CustomFuncs contains all the custom match and replace functions used by
// the normalization rules. These are also imported and used by the explorer.
type CustomFuncs struct {
	f   *Factory
	mem *memo.Memo
}

// Init initializes a new CustomFuncs with the given factory.
func (c *CustomFuncs) Init(f *Factory) {
	c.f = f
	c.mem = f.Memo()
}

// Succeeded returns true if a result expression is not nil.
func (c *CustomFuncs) Succeeded(result opt.Expr) bool {
	return result != nil
}

// ----------------------------------------------------------------------
//
// ScalarList functions
//   General custom match and replace functions used to test and construct
//   scalar lists.
//
// ----------------------------------------------------------------------

// NeedSortedUniqueList returns true if the given list is composed entirely of
// constant values that are either not in sorted order or have duplicates. If
// true, then ConstructSortedUniqueList needs to be called on the list to
// normalize it.
func (c *CustomFuncs) NeedSortedUniqueList(list memo.ScalarListExpr) bool {
	if len(list) <= 1 {
		return false
	}
	ls := listSorter{cf: c, list: list}
	for i, item := range list {
		if !opt.IsConstValueOp(item) {
			return false
		}
		if i != 0 && !ls.less(i-1, i) {
			return true
		}
	}
	return false
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

// ----------------------------------------------------------------------
//
// Typing functions
//   General custom match and replace functions used to test and construct
//   expression data types.
//
// ----------------------------------------------------------------------

// HasColType returns true if the given scalar expression has a static type
// that's equivalent to the requested coltype.
func (c *CustomFuncs) HasColType(scalar opt.ScalarExpr, dstTyp coltypes.T) bool {
	srcTyp, _ := coltypes.DatumTypeToColumnType(scalar.DataType())
	if reflect.TypeOf(srcTyp) != reflect.TypeOf(dstTyp) {
		return false
	}
	return coltypes.ColTypeAsString(srcTyp) == coltypes.ColTypeAsString(dstTyp)
}

// IsString returns true if the given scalar expression is of type String.
func (c *CustomFuncs) IsString(scalar opt.ScalarExpr) bool {
	return scalar.DataType() == types.String
}

// ColTypeToDatumType maps the given column type to a datum type.
func (c *CustomFuncs) ColTypeToDatumType(colTyp coltypes.T) types.T {
	return coltypes.CastTargetToDatumType(colTyp)
}

// BoolType returns the boolean SQL type.
func (c *CustomFuncs) BoolType() types.T {
	return types.Bool
}

// AnyType returns the wildcard Any type.
func (c *CustomFuncs) AnyType() types.T {
	return types.Any
}

// CanConstructBinary returns true if (op left right) has a valid binary op
// overload and is therefore legal to construct. For example, while
// (Minus <date> <int>) is valid, (Minus <int> <date>) is not.
func (c *CustomFuncs) CanConstructBinary(op opt.Operator, left, right opt.ScalarExpr) bool {
	return memo.BinaryOverloadExists(op, left.DataType(), right.DataType())
}

// ----------------------------------------------------------------------
//
// Property functions
//   General custom match and replace functions used to test expression
//   logical properties.
//
// ----------------------------------------------------------------------

// OutputCols returns the set of columns returned by the input expression.
func (c *CustomFuncs) OutputCols(input memo.RelExpr) opt.ColSet {
	return input.Relational().OutputCols
}

// OutputCols2 returns the union of columns returned by the left and right
// expressions.
func (c *CustomFuncs) OutputCols2(left, right memo.RelExpr) opt.ColSet {
	return left.Relational().OutputCols.Union(right.Relational().OutputCols)
}

// CandidateKey returns the candidate key columns from the given input
// expression. If there is no candidate key, CandidateKey returns ok=false.
func (c *CustomFuncs) CandidateKey(input memo.RelExpr) (key opt.ColSet, ok bool) {
	return input.Relational().FuncDeps.StrictKey()
}

// IsColNotNull returns true if the given input column is never null.
func (c *CustomFuncs) IsColNotNull(col opt.ColumnID, input memo.RelExpr) bool {
	return input.Relational().NotNullCols.Contains(int(col))
}

// IsColNotNull2 returns true if the given column is part of the left or right
// expressions' set of not-null columns.
func (c *CustomFuncs) IsColNotNull2(col opt.ColumnID, left, right memo.RelExpr) bool {
	return left.Relational().NotNullCols.Contains(int(col)) ||
		right.Relational().NotNullCols.Contains(int(col))
}

// OuterCols returns the set of outer columns associated with the given
// expression, whether it be a relational or scalar operator.
func (c *CustomFuncs) OuterCols(e opt.Expr) opt.ColSet {
	return c.sharedProps(e).OuterCols
}

// HasOuterCols returns true if the input expression has at least one outer
// column, or in other words, a reference to a variable that is not bound within
// its own scope. For example:
//
//   SELECT * FROM a WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x)
//
// The a.x variable in the EXISTS subquery references a column outside the scope
// of the subquery. It is an "outer column" for the subquery (see the comment on
// RelationalProps.OuterCols for more details).
func (c *CustomFuncs) HasOuterCols(input opt.Expr) bool {
	return !c.OuterCols(input).Empty()
}

// IsBoundBy returns true if all outer references in the source expression are
// bound by the given columns. For example:
//
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     [ ... $item:(FiltersItem (Eq (Variable a.x) (Const 1))) ... ]
//   )
//
// The $item expression is fully bound by the output columns of the (Scan a)
// expression because all of its outer references are satisfied by the columns
// produced by the Scan.
func (c *CustomFuncs) IsBoundBy(src opt.Expr, cols opt.ColSet) bool {
	return c.OuterCols(src).SubsetOf(cols)
}

// IsCorrelated returns true if any variable in the source expression references
// a column from the destination expression. For example:
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     [ ... (FiltersItem $item:(Eq (Variable a.x) (Const 1))) ... ]
//   )
//
// The $item expression is correlated with the (Scan a) expression because it
// references one of its columns. But the $item expression is not correlated
// with the (Scan b) expression.
func (c *CustomFuncs) IsCorrelated(src, dst memo.RelExpr) bool {
	return src.Relational().OuterCols.Intersects(dst.Relational().OutputCols)
}

// HasNoCols returns true if the input expression has zero output columns.
func (c *CustomFuncs) HasNoCols(input memo.RelExpr) bool {
	return input.Relational().OutputCols.Empty()
}

// HasZeroRows returns true if the input expression never returns any rows.
func (c *CustomFuncs) HasZeroRows(input memo.RelExpr) bool {
	return input.Relational().Cardinality.IsZero()
}

// HasOneRow returns true if the input expression always returns exactly one
// row.
func (c *CustomFuncs) HasOneRow(input memo.RelExpr) bool {
	return input.Relational().Cardinality.IsOne()
}

// HasZeroOrOneRow returns true if the input expression returns at most one row.
func (c *CustomFuncs) HasZeroOrOneRow(input memo.RelExpr) bool {
	return input.Relational().Cardinality.IsZeroOrOne()
}

// CanHaveZeroRows returns true if the input expression might return zero rows.
func (c *CustomFuncs) CanHaveZeroRows(input memo.RelExpr) bool {
	return input.Relational().Cardinality.CanBeZero()
}

// ColsAreSubset returns true if the left columns are a subset of the right
// columns.
func (c *CustomFuncs) ColsAreSubset(left, right opt.ColSet) bool {
	return left.SubsetOf(right)
}

// ColsAreEqual returns true if left and right contain the same set of columns.
func (c *CustomFuncs) ColsAreEqual(left, right opt.ColSet) bool {
	return left.Equals(right)
}

// UnionCols returns the union of the left and right column sets.
func (c *CustomFuncs) UnionCols(left, right opt.ColSet) opt.ColSet {
	return left.Union(right)
}

// UnionCols3 returns the union of the three column sets.
func (c *CustomFuncs) UnionCols3(cols1, cols2, cols3 opt.ColSet) opt.ColSet {
	cols := cols1.Union(cols2)
	cols.UnionWith(cols3)
	return cols
}

// UnionCols4 returns the union of the four column sets.
func (c *CustomFuncs) UnionCols4(cols1, cols2, cols3, cols4 opt.ColSet) opt.ColSet {
	cols := cols1.Union(cols2)
	cols.UnionWith(cols3)
	cols.UnionWith(cols4)
	return cols
}

// DifferenceCols returns the difference of the left and right column sets.
func (c *CustomFuncs) DifferenceCols(left, right opt.ColSet) opt.ColSet {
	return left.Difference(right)
}

// sharedProps returns the shared logical properties for the given expression.
// Only relational expressions and certain scalar list items (e.g. FiltersItem,
// ProjectionsItem, AggregationsItem) have shared properties.
func (c *CustomFuncs) sharedProps(e opt.Expr) *props.Shared {
	switch t := e.(type) {
	case memo.RelExpr:
		return &t.Relational().Shared
	case memo.ScalarPropsExpr:
		return &t.ScalarProps(c.mem).Shared
	}
	panic(fmt.Sprintf("no logical properties available for node: %s", e))
}

// ----------------------------------------------------------------------
//
// Ordering functions
//   General custom match and replace functions related to orderings.
//
// ----------------------------------------------------------------------

// HasColsInOrdering returns true if all columns that appear in an ordering are
// output columns of the input expression.
func (c *CustomFuncs) HasColsInOrdering(input memo.RelExpr, ordering physical.OrderingChoice) bool {
	return ordering.CanProjectCols(input.Relational().OutputCols)
}

// OrderingCols returns all non-optional columns that are part of the given
// OrderingChoice.
func (c *CustomFuncs) OrderingCols(ordering physical.OrderingChoice) opt.ColSet {
	return ordering.ColSet()
}

// PruneOrdering removes any columns referenced by an OrderingChoice that are
// not part of the needed column set. Should only be called if HasColsInOrdering
// is true.
func (c *CustomFuncs) PruneOrdering(
	ordering physical.OrderingChoice, needed opt.ColSet,
) physical.OrderingChoice {
	if ordering.SubsetOfCols(needed) {
		return ordering
	}
	ordCopy := ordering.Copy()
	ordCopy.ProjectCols(needed)
	return ordCopy
}

// -----------------------------------------------------------------------
//
// Filter functions
//   General custom match and replace functions used to test and construct
//   filters in Select and Join rules.
//
// -----------------------------------------------------------------------

// FilterOuterCols returns the union of all outer columns from the given filter
// conditions.
func (c *CustomFuncs) FilterOuterCols(filters memo.FiltersExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range filters {
		colSet.UnionWith(filters[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
}

// FilterHasCorrelatedSubquery returns true if any of the filter conditions
// contain a correlated subquery.
func (c *CustomFuncs) FilterHasCorrelatedSubquery(filters memo.FiltersExpr) bool {
	for i := range filters {
		if filters[i].ScalarProps(c.mem).HasCorrelatedSubquery {
			return true
		}
	}
	return false
}

// IsFilterFalse returns true if the filters always evaluate to false. The only
// case that's checked is the fully normalized case, when the list contains a
// single False condition.
func (c *CustomFuncs) IsFilterFalse(filters memo.FiltersExpr) bool {
	return filters.IsFalse()
}

// IsContradiction returns true if the given filter item contains a
// contradiction constraint.
func (c *CustomFuncs) IsContradiction(item *memo.FiltersItem) bool {
	return item.ScalarProps(c.mem).Constraints == constraint.Contradiction
}

// ConcatFilters creates a new Filters operator that contains conditions from
// both the left and right boolean filter expressions.
func (c *CustomFuncs) ConcatFilters(left, right memo.FiltersExpr) memo.FiltersExpr {
	// No need to recompute properties on the new filters, since they should
	// still be valid.
	newFilters := make(memo.FiltersExpr, len(left)+len(right))
	copy(newFilters, left)
	copy(newFilters[len(left):], right)
	return newFilters
}

// RemoveFiltersItem returns a new list that is a copy of the given list, except
// that it does not contain the given search item. If the list contains the item
// multiple times, then only the first instance is removed. If the list does not
// contain the item, then the method panics.
func (c *CustomFuncs) RemoveFiltersItem(
	filters memo.FiltersExpr, search *memo.FiltersItem,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters)-1)
	for i := range filters {
		if search == &filters[i] {
			copy(newFilters, filters[:i])
			copy(newFilters[i:], filters[i+1:])
			return newFilters
		}
	}
	panic(fmt.Sprintf("item to remove is not in the list: %v", search))
}

// ReplaceFiltersItem returns a new list that is a copy of the given list,
// except that the given search item has been replaced by the given replace
// item. If the list contains the search item multiple times, then only the
// first instance is replaced. If the list does not contain the item, then the
// method panics.
func (c *CustomFuncs) ReplaceFiltersItem(
	filters memo.FiltersExpr, search *memo.FiltersItem, replace opt.ScalarExpr,
) memo.FiltersExpr {
	newFilters := make([]memo.FiltersItem, len(filters))
	for i := range filters {
		if search == &filters[i] {
			copy(newFilters, filters[:i])
			newFilters[i].Condition = replace
			copy(newFilters[i+1:], filters[i+1:])
			return newFilters
		}
	}
	panic(fmt.Sprintf("item to replace is not in the list: %v", search))
}

// FiltersBoundBy returns true if all outer references in any of the filter
// conditions are bound by the given columns. For example:
//
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     $filters:[ (FiltersItem (Eq (Variable a.x) (Const 1))) ]
//   )
//
// The $filters expression is fully bound by the output columns of the (Scan a)
// expression because all of its outer references are satisfied by the columns
// produced by the Scan.
func (c *CustomFuncs) FiltersBoundBy(filters memo.FiltersExpr, cols opt.ColSet) bool {
	for i := range filters {
		if !filters[i].ScalarProps(c.mem).OuterCols.SubsetOf(cols) {
			return false
		}
	}
	return true
}

// ExtractBoundConditions returns a new list containing only those expressions
// from the given list that are fully bound by the given columns (i.e. all
// outer references are to one of these columns). For example:
//
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     (Filters [
//       (Eq (Variable a.x) (Variable b.x))
//       (Gt (Variable a.x) (Const 1))
//     ])
//   )
//
// Calling ExtractBoundConditions with the filter conditions list and the output
// columns of (Scan a) would extract the (Gt) expression, since its outer
// references only reference columns from a.
func (c *CustomFuncs) ExtractBoundConditions(
	filters memo.FiltersExpr, cols opt.ColSet,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		if c.IsBoundBy(&filters[i], cols) {
			newFilters = append(newFilters, filters[i])
		}
	}
	return newFilters
}

// ExtractUnboundConditions is the opposite of ExtractBoundConditions. Instead of
// extracting expressions that are bound by the given expression, it extracts
// list expressions that have at least one outer reference that is *not* bound
// by the given columns (i.e. it has a "free" variable).
func (c *CustomFuncs) ExtractUnboundConditions(
	filters memo.FiltersExpr, cols opt.ColSet,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		if !c.IsBoundBy(&filters[i], cols) {
			newFilters = append(newFilters, filters[i])
		}
	}
	return newFilters
}

// ----------------------------------------------------------------------
//
// Project functions
//   General custom match and replace functions used to test and construct
//   Project and Projections operators in Project rules.
//
// ----------------------------------------------------------------------

// CanMergeProjections returns true if the outer Projections operator never
// references any of the inner Projections columns. If true, then the outer does
// not depend on the inner, and the two can be merged into a single set.
func (c *CustomFuncs) CanMergeProjections(outer, inner memo.ProjectionsExpr) bool {
	innerCols := c.ProjectionCols(inner)
	for i := range outer {
		if outer[i].ScalarProps(c.mem).OuterCols.Intersects(innerCols) {
			return false
		}
	}
	return true
}

// MergeProjections concatenates the synthesized columns from the outer
// Projections operator, and the synthesized columns from the inner Projections
// operator that are passed through by the outer. Note that the outer
// synthesized columns must never contain references to the inner synthesized
// columns; this can be verified by first calling CanMergeProjections.
func (c *CustomFuncs) MergeProjections(
	outer, inner memo.ProjectionsExpr, passthrough opt.ColSet,
) memo.ProjectionsExpr {
	// No need to recompute properties on the new projections, since they should
	// still be valid.
	newProjections := make(memo.ProjectionsExpr, len(outer), len(outer)+len(inner))
	copy(newProjections, outer)
	for i := range inner {
		item := &inner[i]
		if passthrough.Contains(int(item.Col)) {
			newProjections = append(newProjections, *item)
		}
	}
	return newProjections
}

// MergeProjectWithValues merges a Project operator with its input Values
// operator. This is only possible in certain circumstances, which are described
// in the MergeProjectWithValues rule comment.
//
// Values columns that are part of the Project passthrough columns are retained
// in the final Values operator, and Project synthesized columns are added to
// it. Any unreferenced Values columns are discarded. For example:
//
//   SELECT column1, 3 FROM (VALUES (1, 2))
//   =>
//   (VALUES (1, 3))
//
func (c *CustomFuncs) MergeProjectWithValues(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, input memo.RelExpr,
) memo.RelExpr {
	newExprs := make(memo.ScalarListExpr, 0, len(projections)+passthrough.Len())
	newTypes := make([]types.T, 0, len(newExprs))
	newCols := make(opt.ColList, 0, len(newExprs))

	values := input.(*memo.ValuesExpr)
	tuple := values.Rows[0].(*memo.TupleExpr)
	for i, colID := range values.Cols {
		if passthrough.Contains(int(colID)) {
			newExprs = append(newExprs, tuple.Elems[i])
			newTypes = append(newTypes, tuple.Elems[i].DataType())
			newCols = append(newCols, colID)
		}
	}

	for i := range projections {
		item := &projections[i]
		newExprs = append(newExprs, item.Element)
		newTypes = append(newTypes, item.Element.DataType())
		newCols = append(newCols, item.Col)
	}

	rows := memo.ScalarListExpr{c.f.ConstructTuple(newExprs, types.TTuple{Types: newTypes})}
	return c.f.ConstructValues(rows, newCols)
}

// ProjectionCols returns the ids of the columns synthesized by the given
// Projections operator.
func (c *CustomFuncs) ProjectionCols(projections memo.ProjectionsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range projections {
		colSet.Add(int(projections[i].Col))
	}
	return colSet
}

// ProjectionOuterCols returns the union of all outer columns from the given
// projection expressions.
func (c *CustomFuncs) ProjectionOuterCols(projections memo.ProjectionsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range projections {
		colSet.UnionWith(projections[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
}

// AreProjectionsCorrelated returns true if any element in the projections
// references any of the given columns.
func (c *CustomFuncs) AreProjectionsCorrelated(
	projections memo.ProjectionsExpr, cols opt.ColSet,
) bool {
	for i := range projections {
		if projections[i].ScalarProps(c.mem).OuterCols.Intersects(cols) {
			return true
		}
	}
	return false
}

// ProjectColMapLeft returns a Projections operator that maps the left side
// columns in a SetPrivate to the output columns in it. Useful for replacing set
// operations with simpler constructs.
func (c *CustomFuncs) ProjectColMapLeft(set *memo.SetPrivate) memo.ProjectionsExpr {
	return c.projectColMapSide(set.OutCols, set.LeftCols)
}

// ProjectColMapRight returns a Project operator that maps the right side
// columns in a SetPrivate to the output columns in it. Useful for replacing set
// operations with simpler constructs.
func (c *CustomFuncs) ProjectColMapRight(set *memo.SetPrivate) memo.ProjectionsExpr {
	return c.projectColMapSide(set.OutCols, set.RightCols)
}

// projectColMapSide implements the side-agnostic logic from ProjectColMapLeft
// and ProjectColMapRight.
func (c *CustomFuncs) projectColMapSide(toList, fromList opt.ColList) memo.ProjectionsExpr {
	items := make(memo.ProjectionsExpr, len(toList))
	for idx, fromCol := range fromList {
		toCol := toList[idx]
		items[idx].Element = c.f.ConstructVariable(fromCol)
		items[idx].Col = toCol
	}
	return items
}

// MakeEmptyColSet returns a column set with no columns in it.
func (c *CustomFuncs) MakeEmptyColSet() opt.ColSet {
	return opt.ColSet{}
}

// ----------------------------------------------------------------------
//
// Select Rules
//   Custom match and replace functions used with select.opt rules.
//
// ----------------------------------------------------------------------

// SimplifyFilters removes True operands from a FiltersExpr, and normalizes any
// False or Null condition to a single False condition. Null values map to False
// because FiltersExpr are only used by Select and Join, both of which treat a
// Null filter conjunct exactly as if it were false.
//
// SimplifyFilters also "flattens" any And operator child by merging its
// conditions into a new FiltersExpr list. If, after simplification, no operands
// remain, then SimplifyFilters returns an empty FiltersExpr.
//
// This method assumes that the NormalizeNestedAnds rule has already run and
// ensured a left deep And tree. If not (maybe because it's a testing scenario),
// then this rule may rematch, but it should still make forward progress).
func (c *CustomFuncs) SimplifyFilters(filters memo.FiltersExpr) memo.FiltersExpr {
	// Start by counting the number of conjuncts that will be flattened so that
	// the capacity of the FiltersExpr list can be determined.
	cnt := 0
	for _, item := range filters {
		cnt++
		condition := item.Condition
		for condition.Op() == opt.AndOp {
			cnt++
			condition = condition.(*memo.AndExpr).Left
		}
	}

	// Construct new filter list.
	newFilters := make(memo.FiltersExpr, 0, cnt)
	for _, item := range filters {
		var ok bool
		if newFilters, ok = c.addConjuncts(item.Condition, newFilters); !ok {
			return memo.FalseFilter
		}
	}

	return newFilters
}

// addConjuncts recursively walks a scalar expression as long as it continues to
// find nested And operators. It adds any conjuncts (ignoring True operators) to
// the given FiltersExpr and returns true. If it finds a False or Null operator,
// it propagates a false return value all the up the call stack, and
// SimplifyFilters maps that to a FiltersExpr that is always false.
func (c *CustomFuncs) addConjuncts(
	scalar opt.ScalarExpr, filters memo.FiltersExpr,
) (_ memo.FiltersExpr, ok bool) {
	switch t := scalar.(type) {
	case *memo.AndExpr:
		var ok bool
		if filters, ok = c.addConjuncts(t.Left, filters); !ok {
			return nil, false
		}
		return c.addConjuncts(t.Right, filters)

	case *memo.FalseExpr, *memo.NullExpr:
		// Filters expression evaluates to False if any operand is False or Null.
		return nil, false

	case *memo.TrueExpr:
		// Filters operator skips True operands.

	default:
		filters = append(filters, memo.FiltersItem{Condition: t})
	}
	return filters, true
}

// ConstructEmptyValues constructs a Values expression with no rows.
func (c *CustomFuncs) ConstructEmptyValues(cols opt.ColSet) memo.RelExpr {
	colList := make(opt.ColList, 0, cols.Len())
	for i, ok := cols.Next(0); ok; i, ok = cols.Next(i + 1) {
		colList = append(colList, opt.ColumnID(i))
	}
	return c.f.ConstructValues(memo.EmptyScalarListExpr, colList)
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// AggregationOuterCols returns the union of all outer columns from the given
// aggregation expressions.
func (c *CustomFuncs) AggregationOuterCols(aggregations memo.AggregationsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range aggregations {
		colSet.UnionWith(aggregations[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
}

// GroupingAndConstCols returns the grouping columns and ConstAgg columns (for
// which the input and output column IDs match). A filter on these columns can
// be pushed through a GroupBy.
func (c *CustomFuncs) GroupingAndConstCols(
	grouping *memo.GroupingPrivate, aggs memo.AggregationsExpr,
) opt.ColSet {
	result := grouping.GroupingCols.Copy()

	// Add any ConstAgg columns.
	for i := range aggs {
		item := &aggs[i]
		if constAgg, ok := item.Agg.(*memo.ConstAggExpr); ok {
			// Verify that the input and output column IDs match.
			if item.Col == constAgg.Input.(*memo.VariableExpr).Col {
				result.Add(int(item.Col))
			}
		}
	}
	return result
}

// GroupingColsAreKey returns true if the input expression's grouping columns
// form a strict key for its output rows. A strict key means that any two rows
// will have unique key column values. Nulls are treated as equal to one another
// (i.e. no duplicate nulls allowed). Having a strict key means that the set of
// key column values uniquely determine the values of all other columns in the
// relation.
func (c *CustomFuncs) GroupingColsAreKey(grouping *memo.GroupingPrivate, input memo.RelExpr) bool {
	colSet := grouping.GroupingCols
	return input.Relational().FuncDeps.ColsAreStrictKey(colSet)
}

// IsUnorderedGrouping returns true if the given grouping ordering is not
// specified.
func (c *CustomFuncs) IsUnorderedGrouping(grouping *memo.GroupingPrivate) bool {
	return grouping.Ordering.Any()
}

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

// LimitGeMaxRows returns true if the given constant limit value is greater than
// or equal to the max number of rows returned by the input expression.
func (c *CustomFuncs) LimitGeMaxRows(limit tree.Datum, input memo.RelExpr) bool {
	limitVal := int64(*limit.(*tree.DInt))
	maxRows := input.Relational().Cardinality.Max
	return limitVal >= 0 && maxRows < math.MaxUint32 && limitVal >= int64(maxRows)
}

// ----------------------------------------------------------------------
//
// ProjectSet Rules
//   Custom match and replace functions used with ProjectSet rules.
//
// ----------------------------------------------------------------------

// IsZipCorrelated returns true if any element in the zip references
// any of the given columns.
func (c *CustomFuncs) IsZipCorrelated(zip memo.ZipExpr, cols opt.ColSet) bool {
	for i := range zip {
		if zip[i].ScalarProps(c.mem).OuterCols.Intersects(cols) {
			return true
		}
	}
	return false
}

// ZipOuterCols returns the union of all outer columns from the given
// zip expressions.
func (c *CustomFuncs) ZipOuterCols(zip memo.ZipExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range zip {
		colSet.UnionWith(zip[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
}

// ----------------------------------------------------------------------
//
// Boolean Rules
//   Custom match and replace functions used with bool.opt rules.
//
// ----------------------------------------------------------------------

// ConcatLeftDeepAnds concatenates any left-deep And expressions in the right
// expression with any left-deep And expressions in the left expression. The
// result is a combined left-deep And expression. Note that NormalizeNestedAnds
// has already guaranteed that both inputs will already be left-deep.
func (c *CustomFuncs) ConcatLeftDeepAnds(left, right opt.ScalarExpr) opt.ScalarExpr {
	if and, ok := right.(*memo.AndExpr); ok {
		return c.f.ConstructAnd(c.ConcatLeftDeepAnds(left, and.Left), and.Right)
	}
	return c.f.ConstructAnd(left, right)
}

// NegateComparison negates a comparison op like:
//   a.x = 5
// to:
//   a.x <> 5
func (c *CustomFuncs) NegateComparison(
	cmp opt.Operator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	negate := opt.NegateOpMap[cmp]
	return c.f.DynamicConstruct(negate, left, right).(opt.ScalarExpr)
}

// CommuteInequality swaps the operands of an inequality comparison expression,
// changing the operator to compensate:
//   5 < x
// to:
//   x > 5
func (c *CustomFuncs) CommuteInequality(
	op opt.Operator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	switch op {
	case opt.GeOp:
		return c.f.ConstructLe(right, left)
	case opt.GtOp:
		return c.f.ConstructLt(right, left)
	case opt.LeOp:
		return c.f.ConstructGe(right, left)
	case opt.LtOp:
		return c.f.ConstructGt(right, left)
	}
	panic(fmt.Sprintf("called commuteInequality with operator %s", op))
}

// FindRedundantConjunct takes the left and right operands of an Or operator as
// input. It examines each conjunct from the left expression and determines
// whether it appears as a conjunct in the right expression. If so, it returns
// the matching conjunct. Otherwise, it returns nil. For example:
//
//   A OR A                               =>  A
//   B OR A                               =>  nil
//   A OR (A AND B)                       =>  A
//   (A AND B) OR (A AND C)               =>  A
//   (A AND B AND C) OR (A AND (D OR E))  =>  A
//
// Once a redundant conjunct has been found, it is extracted via a call to the
// ExtractRedundantConjunct function. Redundant conjuncts are extracted from
// multiple nested Or operators by repeated application of these functions.
func (c *CustomFuncs) FindRedundantConjunct(left, right opt.ScalarExpr) opt.ScalarExpr {
	// Recurse over each conjunct from the left expression and determine whether
	// it's redundant.
	for {
		// Assume a left-deep And expression tree normalized by NormalizeNestedAnds.
		if and, ok := left.(*memo.AndExpr); ok {
			if c.isConjunct(and.Right, right) {
				return and.Right
			}
			left = and.Left
		} else {
			if c.isConjunct(left, right) {
				return left
			}
			return nil
		}
	}
}

// isConjunct returns true if the candidate expression is a conjunct within the
// given conjunction. The conjunction is assumed to be left-deep (normalized by
// the NormalizeNestedAnds rule).
func (c *CustomFuncs) isConjunct(candidate, conjunction opt.ScalarExpr) bool {
	for {
		if and, ok := conjunction.(*memo.AndExpr); ok {
			if and.Right == candidate {
				return true
			}
			conjunction = and.Left
		} else {
			return conjunction == candidate
		}
	}
}

// ExtractRedundantConjunct extracts a redundant conjunct from an Or expression,
// and returns an And of the conjunct with the remaining Or expression (a
// logically equivalent expression). For example:
//
//   (A AND B) OR (A AND C)  =>  A AND (B OR C)
//
// If extracting the conjunct from one of the OR conditions would result in an
// empty condition, the conjunct itself is returned (a logically equivalent
// expression). For example:
//
//   A OR (A AND B)  =>  A
//
// These transformations are useful for finding a conjunct that can be pushed
// down in the query tree. For example, if the redundant conjunct A is fully
// bound by one side of a join, it can be pushed through the join, even if B and
// C cannot.
func (c *CustomFuncs) ExtractRedundantConjunct(
	conjunct, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	if conjunct == left || conjunct == right {
		return conjunct
	}

	return c.f.ConstructAnd(
		conjunct,
		c.f.ConstructOr(
			c.extractConjunct(conjunct, left.(*memo.AndExpr)),
			c.extractConjunct(conjunct, right.(*memo.AndExpr)),
		),
	)
}

// extractConjunct traverses the And subtree looking for the given conjunct,
// which must be present. Once it's located, it's removed from the tree, and
// the remaining expression is returned.
func (c *CustomFuncs) extractConjunct(conjunct opt.ScalarExpr, and *memo.AndExpr) opt.ScalarExpr {
	if and.Right == conjunct {
		return and.Left
	}
	if and.Left == conjunct {
		return and.Right
	}
	return c.f.ConstructAnd(c.extractConjunct(conjunct, and.Left.(*memo.AndExpr)), and.Right)
}

// ----------------------------------------------------------------------
//
// Comparison Rules
//   Custom match and replace functions used with comp.opt rules.
//
// ----------------------------------------------------------------------

// NormalizeTupleEquality remaps the elements of two tuples compared for
// equality, like this:
//   (a, b, c) = (x, y, z)
// into this:
//   (a = x) AND (b = y) AND (c = z)
func (c *CustomFuncs) NormalizeTupleEquality(left, right memo.ScalarListExpr) opt.ScalarExpr {
	if len(left) != len(right) {
		panic("tuple length mismatch")
	}

	var result opt.ScalarExpr
	for i := range left {
		eq := c.f.ConstructEq(left[i], right[i])
		if result == nil {
			result = eq
		} else {
			result = c.f.ConstructAnd(result, eq)
		}
	}
	return result
}

// ----------------------------------------------------------------------
//
// Scalar Rules
//   Custom match and replace functions used with scalar.opt rules.
//
// ----------------------------------------------------------------------

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

// AllowNullArgs returns true if the binary operator with the given inputs
// allows one of those inputs to be null. If not, then the binary operator will
// simply be replaced by null.
func (c *CustomFuncs) AllowNullArgs(op opt.Operator, left, right opt.ScalarExpr) bool {
	return memo.BinaryAllowsNullArgs(op, left.DataType(), right.DataType())
}

// FoldNullUnary replaces the unary operator with a typed null value having the
// same type as the unary operator would have.
func (c *CustomFuncs) FoldNullUnary(op opt.Operator, input opt.ScalarExpr) opt.ScalarExpr {
	return c.f.ConstructNull(memo.InferUnaryType(op, input.DataType()))
}

// FoldNullBinary replaces the binary operator with a typed null value having
// the same type as the binary operator would have.
func (c *CustomFuncs) FoldNullBinary(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	return c.f.ConstructNull(memo.InferBinaryType(op, left.DataType(), right.DataType()))
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
		panic(fmt.Errorf("unexpected Op type: %v", op1))
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
				return when.Value
			}
			return c.f.ConstructCase(condition, newWhens, when.Value)
		}

		newWhens = append(newWhens, when)
	}

	// The ELSE value.
	if len(newWhens) == 0 {
		// ELSE is the only clause (there are no WHENs), remove the CASE.
		return orElse
	}

	return c.f.ConstructCase(condition, newWhens, orElse)
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
	d := scalar.(*memo.ConstExpr).Value.(*tree.DArray)
	elems := make(memo.ScalarListExpr, len(d.Array))
	ts := make([]types.T, len(d.Array))
	for i := range d.Array {
		elems[i] = c.f.ConstructConst(d.Array[i])
		ts[i] = d.ParamTyp
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
		panic(fmt.Sprintf("unexpected type for COLLATE: %T", str.(*memo.ConstExpr).Value))
	}

	return c.f.ConstructConst(tree.NewDCollatedString(value, locale, &c.f.evalCtx.CollationEnv))
}

// ----------------------------------------------------------------------
//
// Numeric Rules
//   Custom match and replace functions used with numeric.opt rules.
//
// ----------------------------------------------------------------------

// EqualsNumber returns true if the given numeric value (decimal, float, or
// integer) is equal to the given integer value.
func (c *CustomFuncs) EqualsNumber(datum tree.Datum, value int64) bool {
	switch t := datum.(type) {
	case *tree.DDecimal:
		if value == 0 {
			return t.Decimal.IsZero()
		} else if value == 1 {
			return t.Decimal.Cmp(&tree.DecimalOne.Decimal) == 0
		}
		var dec apd.Decimal
		dec.SetInt64(value)
		return t.Decimal.Cmp(&dec) == 0

	case *tree.DFloat:
		return *t == tree.DFloat(value)

	case *tree.DInt:
		return *t == tree.DInt(value)
	}
	return false
}

// ----------------------------------------------------------------------
//
// Constant Folding Rules
//   Custom match and replace functions used with fold_constants.opt
//   rules.
//
// ----------------------------------------------------------------------

// IsListOfConstants returns true if elems is a list of constant values or
// tuples.
func (c *CustomFuncs) IsListOfConstants(elems memo.ScalarListExpr) bool {
	for _, elem := range elems {
		if !c.IsConstValueOrTuple(elem) {
			return false
		}
	}
	return true
}

// FoldArray evaluates an Array expression with constant inputs. It returns the
// array as a Const datum with type TArray.
func (c *CustomFuncs) FoldArray(elems memo.ScalarListExpr, typ types.T) opt.ScalarExpr {
	elemType := typ.(types.TArray).Typ
	a := tree.NewDArray(elemType)
	a.Array = make(tree.Datums, len(elems))
	for i := range a.Array {
		a.Array[i] = memo.ExtractConstDatum(elems[i])
		if a.Array[i] == tree.DNull {
			a.HasNulls = true
		}
	}
	return c.f.ConstructConst(a)
}

// IsConstValueOrTuple returns true if the input is a constant or a tuple of
// constants.
func (c *CustomFuncs) IsConstValueOrTuple(input opt.ScalarExpr) bool {
	return memo.CanExtractConstDatum(input)
}

// FoldBinary evaluates a binary expression with constant inputs. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input types, and the evaluation causes no error.
func (c *CustomFuncs) FoldBinary(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)

	o, ok := memo.FindBinaryOverload(op, left.DataType(), right.DataType())
	if !ok {
		return nil
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return nil
	}
	return c.f.ConstructConstVal(result)
}

// FoldUnary evaluates a unary expression with a constant input. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input type, and the evaluation causes no error.
func (c *CustomFuncs) FoldUnary(op opt.Operator, input opt.ScalarExpr) opt.ScalarExpr {
	datum := memo.ExtractConstDatum(input)

	o, ok := memo.FindUnaryOverload(op, input.DataType())
	if !ok {
		return nil
	}

	result, err := o.Fn(c.f.evalCtx, datum)
	if err != nil {
		return nil
	}
	return c.f.ConstructConstVal(result)
}

// isMonotonicConversion returns true if conversion of a value from FROM to
// TO is monotonic.
// That is, if a and b are values of type FROM, then
//
//   1. a = b implies a::TO = b::TO and
//   2. a < b implies a::TO <= b::TO
//
// Property (1) can be violated by cases like:
//
//   '-0'::FLOAT = '0'::FLOAT, but '-0'::FLOAT::STRING != '0'::FLOAT::STRING
//
// Property (2) can be violated by cases like:
//
//   2 < 10, but  2::STRING > 10::STRING.
//
// Note that the stronger version of (2),
//
//   a < b implies a::TO < b::TO
//
// is not required, for instance this is not generally true of conversion from
// a TIMESTAMP to a DATE, but certain such conversions can still generate spans
// in some cases where values under FROM and TO are "the same" (such as where a
// TIMESTAMP precisely falls on a date boundary).  We don't need this property
// because we will subsequently check that the values can round-trip to ensure
// that we don't lose any information by doing the conversion.
// TODO(justin): fill this out with the complete set of such conversions.
func isMonotonicConversion(from, to coltypes.T) bool {
	if from == coltypes.Timestamp ||
		from == coltypes.TimestampWithTZ ||
		from == coltypes.Date {
		return to == coltypes.Timestamp ||
			to == coltypes.TimestampWithTZ ||
			to == coltypes.Date
	}

	if from == coltypes.Int ||
		from == coltypes.Float8 ||
		from == coltypes.Decimal {
		return to == coltypes.Int ||
			to == coltypes.Float8 ||
			to == coltypes.Decimal
	}

	return false
}

// UnifyComparison attempts to convert a constant expression to the type of the
// variable expression, if that conversion can round-trip and is monotonic.
func (c *CustomFuncs) UnifyComparison(left, right opt.ScalarExpr) opt.ScalarExpr {
	v := left.(*memo.VariableExpr)
	cnst := right.(*memo.ConstExpr)

	desiredType := v.DataType()
	originalType := cnst.DataType()

	// Don't bother if they're already the same.
	if desiredType.Equivalent(originalType) {
		return nil
	}

	desiredColType, err := coltypes.DatumTypeToColumnType(desiredType)
	if err != nil {
		return nil
	}

	originalColType, err := coltypes.DatumTypeToColumnType(originalType)
	if err != nil {
		return nil
	}

	if !isMonotonicConversion(originalColType, desiredColType) {
		return nil
	}

	// Check that the datum can round-trip between the types. If this is true, it
	// means we don't lose any information needed to generate spans, and combined
	// with monotonicity means that it's safe to convert the RHS to the type of
	// the LHS.
	convertedDatum, err := tree.PerformCast(c.f.evalCtx, cnst.Value, desiredColType)
	if err != nil {
		return nil
	}

	convertedBack, err := tree.PerformCast(c.f.evalCtx, convertedDatum, originalColType)
	if err != nil {
		return nil
	}

	if convertedBack.Compare(c.f.evalCtx, cnst.Value) != 0 {
		return nil
	}

	return c.f.ConstructConst(convertedDatum)
}

// FoldComparison evaluates a comparison expression with constant inputs. It
// returns a constant expression as long as it finds an appropriate overload
// function for the given operator and input types, and the evaluation causes
// no error.
func (c *CustomFuncs) FoldComparison(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	lDatum, rDatum := memo.ExtractConstDatum(left), memo.ExtractConstDatum(right)

	o, ok := memo.FindComparisonOverload(op, left.DataType(), right.DataType())
	if !ok {
		return nil
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return nil
	}
	return c.f.ConstructConstVal(result)
}
