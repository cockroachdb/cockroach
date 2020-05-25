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
	"reflect"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
// Typing functions
//   General functions used to test and construct expression data types.
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

// ArrayType returns the type of the first output column wrapped
// in an array.
func (c *CustomFuncs) ArrayType(in memo.RelExpr) types.T {
	inCol, _ := c.OutputCols(in).Next(0)
	inTyp := c.mem.Metadata().ColumnMeta(opt.ColumnID(inCol)).Type
	return types.TArray{Typ: inTyp}
}

// BinaryColType returns the column type of the binary overload for the
// given operator and operands.
func (c *CustomFuncs) BinaryColType(op opt.Operator, left, right opt.ScalarExpr) coltypes.T {
	o, _ := memo.FindBinaryOverload(op, left.DataType(), right.DataType())
	colType, _ := coltypes.DatumTypeToColumnType(o.ReturnType)
	return colType
}

// IsAdditive returns true if the type of the expression supports addition and
// subtraction in the natural way. This differs from "has a +/- Numeric
// implementation" because JSON has an implementation for "- INT" which doesn't
// obey x - 0 = x. Additive types include all numeric types as well as
// timestamps and dates.
func (c *CustomFuncs) IsAdditive(e opt.ScalarExpr) bool {
	return types.IsAdditiveType(e.DataType())
}

// ColTypeToDatumType maps the given column type to a datum type.
func (c *CustomFuncs) ColTypeToDatumType(colTyp coltypes.T) types.T {
	return coltypes.CastTargetToDatumType(colTyp)
}

// ----------------------------------------------------------------------
//
// Column functions
//   General custom match and replace functions related to columns.
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

// HasNoCols returns true if the input expression has zero output columns.
func (c *CustomFuncs) HasNoCols(input memo.RelExpr) bool {
	return input.Relational().OutputCols.Empty()
}

// ColsAreEmpty returns true if the column set is empty.
func (c *CustomFuncs) ColsAreEmpty(cols opt.ColSet) bool {
	return cols.Empty()
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

// ColsIntersect returns true if at least one column appears in both the left
// and right sets.
func (c *CustomFuncs) ColsIntersect(left, right opt.ColSet) bool {
	return left.Intersects(right)
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

// MakeEmptyColSet returns a column set with no columns in it.
func (c *CustomFuncs) MakeEmptyColSet() opt.ColSet {
	return opt.ColSet{}
}

// FirstCol returns the first column in the input expression.
func (c *CustomFuncs) FirstCol(in memo.RelExpr) opt.ColumnID {
	inCol, _ := c.OutputCols(in).Next(0)
	return opt.ColumnID(inCol)
}

// ----------------------------------------------------------------------
//
// Outer column functions
//   General custom functions related to outer column references.
//
// ----------------------------------------------------------------------

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

// FilterOuterCols returns the union of all outer columns from the given filter
// conditions.
func (c *CustomFuncs) FilterOuterCols(filters memo.FiltersExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range filters {
		colSet.UnionWith(filters[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
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

// ProjectionOuterCols returns the union of all outer columns from the given
// projection expressions.
func (c *CustomFuncs) ProjectionOuterCols(projections memo.ProjectionsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range projections {
		colSet.UnionWith(projections[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
}

// AggregationOuterCols returns the union of all outer columns from the given
// aggregation expressions.
func (c *CustomFuncs) AggregationOuterCols(aggregations memo.AggregationsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range aggregations {
		colSet.UnionWith(aggregations[i].ScalarProps(c.mem).OuterCols)
	}
	return colSet
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
// Row functions
//   General custom match and replace functions related to rows.
//
// ----------------------------------------------------------------------

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

// ----------------------------------------------------------------------
//
// Key functions
//   General custom match and replace functions related to keys.
//
// ----------------------------------------------------------------------

// CandidateKey returns the candidate key columns from the given input
// expression. If there is no candidate key, CandidateKey returns ok=false.
func (c *CustomFuncs) CandidateKey(input memo.RelExpr) (key opt.ColSet, ok bool) {
	return input.Relational().FuncDeps.StrictKey()
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

// ----------------------------------------------------------------------
//
// Property functions
//   General custom functions related to expression logical properties.
//
// ----------------------------------------------------------------------

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
	panic(pgerror.NewAssertionErrorf("no logical properties available for node: %v", e))
}

// ----------------------------------------------------------------------
//
// Ordering functions
//   General functions related to orderings.
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

// EmptyOrdering returns a pseudo-choice that does not require any
// ordering.
func (c *CustomFuncs) EmptyOrdering() physical.OrderingChoice {
	return physical.OrderingChoice{}
}

// -----------------------------------------------------------------------
//
// Filter functions
//   General functions used to test and construct filters.
//
// -----------------------------------------------------------------------

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
	panic(pgerror.NewAssertionErrorf("item to remove is not in the list: %v", search))
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
	panic(pgerror.NewAssertionErrorf("item to replace is not in the list: %v", search))
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
// extracting expressions that are bound by the given columns, it extracts
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
//   General functions related to Project operators.
//
// ----------------------------------------------------------------------

// ProjectionCols returns the ids of the columns synthesized by the given
// Projections operator.
func (c *CustomFuncs) ProjectionCols(projections memo.ProjectionsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range projections {
		colSet.Add(int(projections[i].Col))
	}
	return colSet
}

// ProjectExtraCol constructs a new Project operator that passes through all
// columns in the given "in" expression, and then adds the given "extra"
// expression as an additional column.
func (c *CustomFuncs) ProjectExtraCol(
	in memo.RelExpr, extra opt.ScalarExpr, extraID opt.ColumnID,
) memo.RelExpr {
	projections := memo.ProjectionsExpr{{
		Element:    extra,
		ColPrivate: memo.ColPrivate{Col: extraID}},
	}
	return c.f.ConstructProject(in, projections, in.Relational().OutputCols)
}

// ----------------------------------------------------------------------
//
// Values functions
//   General functions related to Values operators.
//
// ----------------------------------------------------------------------

// ConstructEmptyValues constructs a Values expression with no rows.
func (c *CustomFuncs) ConstructEmptyValues(cols opt.ColSet) memo.RelExpr {
	colList := make(opt.ColList, 0, cols.Len())
	for i, ok := cols.Next(0); ok; i, ok = cols.Next(i + 1) {
		colList = append(colList, opt.ColumnID(i))
	}
	return c.f.ConstructValues(memo.EmptyScalarListExpr, &memo.ValuesPrivate{
		Cols: colList,
		ID:   c.mem.Metadata().NextValuesID(),
	})
}

// ----------------------------------------------------------------------
//
// Grouping functions
//   General functions related to grouping expressions such as GroupBy,
//   DistinctOn, etc.
//
// ----------------------------------------------------------------------

// AddColsToGrouping returns a new GroupByDef that is a copy of the given
// GroupingPrivate, except with the given set of grouping columns union'ed with
// the existing grouping columns.
func (c *CustomFuncs) AddColsToGrouping(
	private *memo.GroupingPrivate, groupingCols opt.ColSet,
) *memo.GroupingPrivate {
	return &memo.GroupingPrivate{
		GroupingCols: private.GroupingCols.Union(groupingCols),
		Ordering:     private.Ordering,
	}
}

// IsUnorderedGrouping returns true if the given grouping ordering is not
// specified.
func (c *CustomFuncs) IsUnorderedGrouping(grouping *memo.GroupingPrivate) bool {
	return grouping.Ordering.Any()
}

// MakeGrouping constructs a new unordered GroupingPrivate using the given
// grouping columns.
func (c *CustomFuncs) MakeGrouping(groupingCols opt.ColSet) *memo.GroupingPrivate {
	return &memo.GroupingPrivate{GroupingCols: groupingCols}
}

// MakeOrderedGrouping constructs a new GroupingPrivate using the given
// grouping columns and OrderingChoice private.
func (c *CustomFuncs) MakeOrderedGrouping(
	groupingCols opt.ColSet, ordering physical.OrderingChoice,
) *memo.GroupingPrivate {
	return &memo.GroupingPrivate{GroupingCols: groupingCols, Ordering: ordering}
}

// ExtractGroupingOrdering returns the ordering associated with the input
// GroupingPrivate.
func (c *CustomFuncs) ExtractGroupingOrdering(
	private *memo.GroupingPrivate,
) physical.OrderingChoice {
	return private.Ordering
}

// ----------------------------------------------------------------------
//
// Constant value functions
//   General functions related to constant values and datums.
//
// ----------------------------------------------------------------------

// IsPositiveLimit is true if the given limit value is greater than zero.
func (c *CustomFuncs) IsPositiveLimit(limit tree.Datum) bool {
	limitVal := int64(*limit.(*tree.DInt))
	return limitVal > 0
}

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
