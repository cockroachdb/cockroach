// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/errors"
)

// CustomFuncs contains all the custom match and replace functions used by
// the normalization rules. These are also imported and used by the explorer.
type CustomFuncs struct {
	f   *Factory
	mem *memo.Memo
}

// Init initializes a new CustomFuncs with the given factory.
func (c *CustomFuncs) Init(f *Factory) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = CustomFuncs{
		f:   f,
		mem: f.Memo(),
	}
}

// ----------------------------------------------------------------------
//
// Typing functions
//   General functions used to test and construct expression data types.
//
// ----------------------------------------------------------------------

// HasColType returns true if the given scalar expression has a static type
// that's identical to the requested coltype.
func (c *CustomFuncs) HasColType(scalar opt.ScalarExpr, dstTyp *types.T) bool {
	return scalar.DataType().Identical(dstTyp)
}

// IsTimestamp returns true if the given scalar expression is of type Timestamp.
func (c *CustomFuncs) IsTimestamp(scalar opt.ScalarExpr) bool {
	return scalar.DataType().Family() == types.TimestampFamily
}

// IsTimestampTZ returns true if the given scalar expression is of type
// TimestampTZ.
func (c *CustomFuncs) IsTimestampTZ(scalar opt.ScalarExpr) bool {
	return scalar.DataType().Family() == types.TimestampTZFamily
}

// BoolType returns the boolean SQL type.
func (c *CustomFuncs) BoolType() *types.T {
	return types.Bool
}

// AnyType returns the wildcard Any type.
func (c *CustomFuncs) AnyType() *types.T {
	return types.Any
}

// CanConstructBinary returns true if (op left right) has a valid binary op
// overload and is therefore legal to construct. For example, while
// (Minus <date> <int>) is valid, (Minus <int> <date>) is not.
func (c *CustomFuncs) CanConstructBinary(op opt.Operator, left, right opt.ScalarExpr) bool {
	return memo.BinaryOverloadExists(op, left.DataType(), right.DataType())
}

// ArrayType returns the type of the given column wrapped
// in an array.
func (c *CustomFuncs) ArrayType(inCol opt.ColumnID) *types.T {
	inTyp := c.mem.Metadata().ColumnMeta(inCol).Type
	return types.MakeArray(inTyp)
}

// BinaryType returns the type of the binary overload for the given operator and
// operands.
func (c *CustomFuncs) BinaryType(op opt.Operator, left, right opt.ScalarExpr) *types.T {
	o, _ := memo.FindBinaryOverload(op, left.DataType(), right.DataType())
	return o.ReturnType
}

// TypeOf returns the type of the expression.
func (c *CustomFuncs) TypeOf(e opt.ScalarExpr) *types.T {
	return e.DataType()
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

// IsAdditiveType returns true if the given type supports addition and
// subtraction in the natural way. This differs from "has a +/- Numeric
// implementation" because JSON has an implementation for "- INT" which doesn't
// obey x - 0 = x. Additive types include all numeric types as well as
// timestamps and dates.
func (c *CustomFuncs) IsAdditiveType(typ *types.T) bool {
	return types.IsAdditiveType(typ)
}

// IsConstJSON returns true if the given ScalarExpr is a ConstExpr that wraps a
// DJSON datum.
func (c *CustomFuncs) IsConstJSON(expr opt.ScalarExpr) bool {
	if constExpr, ok := expr.(*memo.ConstExpr); ok {
		if _, ok := constExpr.Value.(*tree.DJSON); ok {
			return true
		}
	}
	return false
}

// IsFloatDatum returns true if the given tree.Datum is a DFloat.
func (c *CustomFuncs) IsFloatDatum(datum tree.Datum) bool {
	_, ok := datum.(*tree.DFloat)
	return ok
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

// NotNullCols returns the set of columns returned by the input expression that
// are guaranteed to never be NULL.
func (c *CustomFuncs) NotNullCols(input memo.RelExpr) opt.ColSet {
	return input.Relational().NotNullCols
}

// IsColNotNull returns true if the given input column is never null.
func (c *CustomFuncs) IsColNotNull(col opt.ColumnID, input memo.RelExpr) bool {
	return input.Relational().NotNullCols.Contains(col)
}

// IsColNotNull2 returns true if the given column is part of the left or right
// expressions' set of not-null columns.
func (c *CustomFuncs) IsColNotNull2(col opt.ColumnID, left, right memo.RelExpr) bool {
	return left.Relational().NotNullCols.Contains(col) ||
		right.Relational().NotNullCols.Contains(col)
}

// ColsAreConst returns true if the given columns have the same values for all
// rows in the given input expression.
func (c *CustomFuncs) ColsAreConst(cols opt.ColSet, input memo.RelExpr) bool {
	return cols.SubsetOf(input.Relational().FuncDeps.ConstantCols())
}

// ColsAreEmpty returns true if the given column set is empty.
func (c *CustomFuncs) ColsAreEmpty(cols opt.ColSet) bool {
	return cols.Empty()
}

// MakeEmptyColSet returns a column set with no columns in it.
func (c *CustomFuncs) MakeEmptyColSet() opt.ColSet {
	return opt.ColSet{}
}

// ColsAreLenOne returns true if the input ColSet has exactly one column.
func (c *CustomFuncs) ColsAreLenOne(cols opt.ColSet) bool {
	return cols.Len() == 1
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

// IntersectionCols returns the intersection of the left and right column sets.
func (c *CustomFuncs) IntersectionCols(left, right opt.ColSet) opt.ColSet {
	return left.Intersection(right)
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

// AddColToSet returns a set containing both the given set and the given column.
func (c *CustomFuncs) AddColToSet(set opt.ColSet, col opt.ColumnID) opt.ColSet {
	if set.Contains(col) {
		return set
	}
	newSet := set.Copy()
	newSet.Add(col)
	return newSet
}

// SingleColFromSet returns the single column in s. Panics if s does not contain
// exactly one column.
func (c *CustomFuncs) SingleColFromSet(s opt.ColSet) opt.ColumnID {
	return s.SingleColumn()
}

// RedundantCols returns the subset of the given columns that are functionally
// determined by the remaining columns. In many contexts (such as if they are
// grouping columns), these columns can be dropped. The input expression's
// functional dependencies are used to make the decision.
func (c *CustomFuncs) RedundantCols(input memo.RelExpr, cols opt.ColSet) opt.ColSet {
	reducedCols := input.Relational().FuncDeps.ReduceCols(cols)
	if reducedCols.Equals(cols) {
		return opt.ColSet{}
	}
	return cols.Difference(reducedCols)
}

// DuplicateColumnIDs duplicates a table and set of columns IDs in the metadata.
// It returns the new table's ID and the new set of columns IDs.
func (c *CustomFuncs) DuplicateColumnIDs(
	table opt.TableID, cols opt.ColSet,
) (opt.TableID, opt.ColSet) {
	md := c.mem.Metadata()
	tabMeta := md.TableMeta(table)
	newTableID := md.DuplicateTable(table, c.RemapCols)

	// Build a new set of column IDs from the new TableMeta.
	var newColIDs opt.ColSet
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		ord := tabMeta.MetaID.ColumnOrdinal(col)
		newColID := newTableID.ColumnID(ord)
		newColIDs.Add(newColID)
	}

	return newTableID, newColIDs
}

// RemapCols remaps columns IDs in the input ScalarExpr by replacing occurrences
// of the keys of colMap with the corresponding values. If column IDs are
// encountered in the input ScalarExpr that are not keys in colMap, they are not
// remapped.
func (c *CustomFuncs) RemapCols(scalar opt.ScalarExpr, colMap opt.ColMap) opt.ScalarExpr {
	// Recursively walk the scalar sub-tree looking for references to columns
	// that need to be replaced and then replace them appropriately.
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			dstCol, ok := colMap.Get(int(t.Col))
			if !ok {
				// The column ID is not in colMap so no replacement is required.
				return e
			}
			return c.f.ConstructVariable(opt.ColumnID(dstCol))
		}
		return c.f.Replace(e, replace)
	}

	return replace(scalar).(opt.ScalarExpr)
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
// a column from the given set of output columns. For example:
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     [ ... (FiltersItem $item:(Eq (Variable a.x) (Const 1))) ... ]
//   )
//
// The $item expression is correlated with the (Scan a) expression because it
// references one of its columns. But the $item expression is not correlated
// with the (Scan b) expression.
func (c *CustomFuncs) IsCorrelated(src memo.RelExpr, cols opt.ColSet) bool {
	return src.Relational().OuterCols.Intersects(cols)
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

// ColsAreDeterminedBy returns true if the given columns are functionally
// determined by the "in" ColSet according to the functional dependencies of the
// input expression.
func (c *CustomFuncs) ColsAreDeterminedBy(cols, in opt.ColSet, input memo.RelExpr) bool {
	return input.Relational().FuncDeps.InClosureOf(cols, in)
}

// AreProjectionsCorrelated returns true if any element in the projections
// references any of the given columns.
func (c *CustomFuncs) AreProjectionsCorrelated(
	projections memo.ProjectionsExpr, cols opt.ColSet,
) bool {
	for i := range projections {
		if projections[i].ScalarProps().OuterCols.Intersects(cols) {
			return true
		}
	}
	return false
}

// IsZipCorrelated returns true if any element in the zip references
// any of the given columns.
func (c *CustomFuncs) IsZipCorrelated(zip memo.ZipExpr, cols opt.ColSet) bool {
	for i := range zip {
		if zip[i].ScalarProps().OuterCols.Intersects(cols) {
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
		colSet.UnionWith(filters[i].ScalarProps().OuterCols)
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
		if !filters[i].ScalarProps().OuterCols.SubsetOf(cols) {
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
		colSet.UnionWith(projections[i].ScalarProps().OuterCols)
	}
	return colSet
}

// AggregationOuterCols returns the union of all outer columns from the given
// aggregation expressions.
func (c *CustomFuncs) AggregationOuterCols(aggregations memo.AggregationsExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range aggregations {
		colSet.UnionWith(aggregations[i].ScalarProps().OuterCols)
	}
	return colSet
}

// ZipOuterCols returns the union of all outer columns from the given
// zip expressions.
func (c *CustomFuncs) ZipOuterCols(zip memo.ZipExpr) opt.ColSet {
	var colSet opt.ColSet
	for i := range zip {
		colSet.UnionWith(zip[i].ScalarProps().OuterCols)
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

// HasStrictKey returns true if the input expression has one or more columns
// that form a strict key (see comment for ColsAreStrictKey for definition).
func (c *CustomFuncs) HasStrictKey(input memo.RelExpr) bool {
	inputFDs := &input.Relational().FuncDeps
	_, hasKey := inputFDs.StrictKey()
	return hasKey
}

// ColsAreStrictKey returns true if the given columns form a strict key for the
// given input expression. A strict key means that any two rows will have unique
// key column values. Nulls are treated as equal to one another (i.e. no
// duplicate nulls allowed). Having a strict key means that the set of key
// column values uniquely determine the values of all other columns in the
// relation.
func (c *CustomFuncs) ColsAreStrictKey(cols opt.ColSet, input memo.RelExpr) bool {
	return input.Relational().FuncDeps.ColsAreStrictKey(cols)
}

// PrimaryKeyCols returns the key columns of the primary key of the table.
func (c *CustomFuncs) PrimaryKeyCols(table opt.TableID) opt.ColSet {
	tabMeta := c.mem.Metadata().TableMeta(table)
	return tabMeta.IndexKeyColumns(cat.PrimaryIndex)
}

// ----------------------------------------------------------------------
//
// Property functions
//   General custom functions related to expression properties.
//
// ----------------------------------------------------------------------

// ExprIsNeverNull returns true if we can prove that the given scalar expression
// is always non-NULL. Any variables that refer to columns in the notNullCols
// set are assumed to be non-NULL. See memo.ExprIsNeverNull.
func (c *CustomFuncs) ExprIsNeverNull(e opt.ScalarExpr, notNullCols opt.ColSet) bool {
	return memo.ExprIsNeverNull(e, notNullCols)
}

// sharedProps returns the shared logical properties for the given expression.
// Only relational expressions and certain scalar list items (e.g. FiltersItem,
// ProjectionsItem, AggregationsItem) have shared properties.
func (c *CustomFuncs) sharedProps(e opt.Expr) *props.Shared {
	switch t := e.(type) {
	case memo.RelExpr:
		return &t.Relational().Shared
	case memo.ScalarPropsExpr:
		return &t.ScalarProps().Shared
	default:
		var p props.Shared
		memo.BuildSharedProps(e, &p)
		return &p
	}
}

// ----------------------------------------------------------------------
//
// Ordering functions
//   General functions related to orderings.
//
// ----------------------------------------------------------------------

// OrderingCanProjectCols returns true if the given OrderingChoice can be
// expressed using only the given columns. Or in other words, at least one
// column from every ordering group is a member of the given ColSet.
func (c *CustomFuncs) OrderingCanProjectCols(ordering props.OrderingChoice, cols opt.ColSet) bool {
	return ordering.CanProjectCols(cols)
}

// OrderingCols returns all non-optional columns that are part of the given
// OrderingChoice.
func (c *CustomFuncs) OrderingCols(ordering props.OrderingChoice) opt.ColSet {
	return ordering.ColSet()
}

// PruneOrdering removes any columns referenced by an OrderingChoice that are
// not part of the needed column set. Should only be called if
// OrderingCanProjectCols is true.
func (c *CustomFuncs) PruneOrdering(
	ordering props.OrderingChoice, needed opt.ColSet,
) props.OrderingChoice {
	if ordering.SubsetOfCols(needed) {
		return ordering
	}
	ordCopy := ordering.Copy()
	ordCopy.ProjectCols(needed)
	return ordCopy
}

// EmptyOrdering returns a pseudo-choice that does not require any
// ordering.
func (c *CustomFuncs) EmptyOrdering() props.OrderingChoice {
	return props.OrderingChoice{}
}

// OrderingIntersects returns true if <ordering1> and <ordering2> have an
// intersection. See OrderingChoice.Intersection for more information.
func (c *CustomFuncs) OrderingIntersects(ordering1, ordering2 props.OrderingChoice) bool {
	return ordering1.Intersects(&ordering2)
}

// OrderingIntersection returns the intersection of two orderings. Should only be
// called if it is known that an intersection exists.
// See OrderingChoice.Intersection for more information.
func (c *CustomFuncs) OrderingIntersection(
	ordering1, ordering2 props.OrderingChoice,
) props.OrderingChoice {
	return ordering1.Intersection(&ordering2)
}

// OrdinalityOrdering returns an ordinality operator's ordering choice.
func (c *CustomFuncs) OrdinalityOrdering(private *memo.OrdinalityPrivate) props.OrderingChoice {
	return private.Ordering
}

// IsSameOrdering evaluates whether the two orderings are equal.
func (c *CustomFuncs) IsSameOrdering(first, other props.OrderingChoice) bool {
	return first.Equals(&other)
}

// OrderingImplies returns true if the first OrderingChoice implies the second.
func (c *CustomFuncs) OrderingImplies(first, second props.OrderingChoice) bool {
	return first.Implies(&second)
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
		if filters[i].ScalarProps().HasCorrelatedSubquery {
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

// IsFilterEmpty returns true if filters is empty.
func (c *CustomFuncs) IsFilterEmpty(filters memo.FiltersExpr) bool {
	return len(filters) == 0
}

// IsContradiction returns true if the given filter item contains a
// contradiction constraint.
func (c *CustomFuncs) IsContradiction(item *memo.FiltersItem) bool {
	return item.ScalarProps().Constraints == constraint.Contradiction
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

// DiffFilters creates new Filters that contains all conditions in left that do
// not exist in right. If right is empty, the original left filters are
// returned.
func (c *CustomFuncs) DiffFilters(left, right memo.FiltersExpr) memo.FiltersExpr {
	return left.Difference(right)
}

// RemoveFiltersItem returns a new list that is a copy of the given list, except
// that it does not contain the given search item. If the list contains the item
// multiple times, then only the first instance is removed. If the list does not
// contain the item, then the method panics.
func (c *CustomFuncs) RemoveFiltersItem(
	filters memo.FiltersExpr, search *memo.FiltersItem,
) memo.FiltersExpr {
	return filters.RemoveFiltersItem(search)
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
			newFilters[i] = c.f.ConstructFiltersItem(replace)
			copy(newFilters[i+1:], filters[i+1:])
			return newFilters
		}
	}
	panic(errors.AssertionFailedf("item to replace is not in the list: %v", search))
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
		colSet.Add(projections[i].Col)
	}
	return colSet
}

// ProjectExtraCol constructs a new Project operator that passes through all
// columns in the given "in" expression, and then adds the given "extra"
// expression as an additional column.
func (c *CustomFuncs) ProjectExtraCol(
	in memo.RelExpr, extra opt.ScalarExpr, extraID opt.ColumnID,
) memo.RelExpr {
	projections := memo.ProjectionsExpr{c.f.ConstructProjectionsItem(extra, extraID)}
	return c.f.ConstructProject(in, projections, in.Relational().OutputCols)
}

// ----------------------------------------------------------------------
//
// Values functions
//   General functions related to Values operators.
//
// ----------------------------------------------------------------------

// ValuesCols returns the Cols field of the ValuesPrivate struct.
func (c *CustomFuncs) ValuesCols(valuesPrivate *memo.ValuesPrivate) opt.ColList {
	return valuesPrivate.Cols
}

// ConstructEmptyValues constructs a Values expression with no rows.
func (c *CustomFuncs) ConstructEmptyValues(cols opt.ColSet) memo.RelExpr {
	colList := make(opt.ColList, 0, cols.Len())
	for i, ok := cols.Next(0); ok; i, ok = cols.Next(i + 1) {
		colList = append(colList, i)
	}
	return c.f.ConstructValues(memo.EmptyScalarListExpr, &memo.ValuesPrivate{
		Cols: colList,
		ID:   c.mem.Metadata().NextUniqueID(),
	})
}

// ----------------------------------------------------------------------
//
// Grouping functions
//   General functions related to grouping expressions such as GroupBy,
//   DistinctOn, etc.
//
// ----------------------------------------------------------------------

// GroupingOutputCols returns the output columns of a GroupBy, ScalarGroupBy, or
// DistinctOn expression.
func (c *CustomFuncs) GroupingOutputCols(
	grouping *memo.GroupingPrivate, aggs memo.AggregationsExpr,
) opt.ColSet {
	result := grouping.GroupingCols.Copy()
	for i := range aggs {
		result.Add(aggs[i].Col)
	}
	return result
}

// GroupingCols returns the grouping columns from the given grouping private.
func (c *CustomFuncs) GroupingCols(grouping *memo.GroupingPrivate) opt.ColSet {
	return grouping.GroupingCols
}

// AddColsToGrouping returns a new GroupByDef that is a copy of the given
// GroupingPrivate, except with the given set of grouping columns union'ed with
// the existing grouping columns.
func (c *CustomFuncs) AddColsToGrouping(
	private *memo.GroupingPrivate, groupingCols opt.ColSet,
) *memo.GroupingPrivate {
	p := *private
	p.GroupingCols = private.GroupingCols.Union(groupingCols)
	return &p
}

// ExtractAggInputColumns returns the set of columns the aggregate depends on.
func (c *CustomFuncs) ExtractAggInputColumns(e opt.ScalarExpr) opt.ColSet {
	return memo.ExtractAggInputColumns(e)
}

// IsUnorderedGrouping returns true if the given grouping ordering is not
// specified.
func (c *CustomFuncs) IsUnorderedGrouping(grouping *memo.GroupingPrivate) bool {
	return grouping.Ordering.Any()
}

// MakeGrouping constructs a new GroupingPrivate using the given grouping
// columns and OrderingChoice. ErrorOnDup will be empty and NullsAreDistinct
// will be false.
func (c *CustomFuncs) MakeGrouping(
	groupingCols opt.ColSet, ordering props.OrderingChoice,
) *memo.GroupingPrivate {
	return &memo.GroupingPrivate{GroupingCols: groupingCols, Ordering: ordering}
}

// MakeErrorOnDupGrouping constructs a new GroupingPrivate using the given
// grouping columns, OrderingChoice, and ErrorOnDup text. NullsAreDistinct will
// be false.
func (c *CustomFuncs) MakeErrorOnDupGrouping(
	groupingCols opt.ColSet, ordering props.OrderingChoice, errorText string,
) *memo.GroupingPrivate {
	return &memo.GroupingPrivate{
		GroupingCols: groupingCols, Ordering: ordering, ErrorOnDup: errorText,
	}
}

// NullsAreDistinct returns true if a distinct operator with the given
// GroupingPrivate treats NULL values as not equal to one another
// (i.e. distinct). UpsertDistinctOp and EnsureUpsertDistinctOp treat NULL
// values as distinct, whereas DistinctOp does not.
func (c *CustomFuncs) NullsAreDistinct(private *memo.GroupingPrivate) bool {
	return private.NullsAreDistinct
}

// ErrorOnDup returns the error text contained by the given GroupingPrivate.
func (c *CustomFuncs) ErrorOnDup(private *memo.GroupingPrivate) string {
	return private.ErrorOnDup
}

// ExtractGroupingOrdering returns the ordering associated with the input
// GroupingPrivate.
func (c *CustomFuncs) ExtractGroupingOrdering(private *memo.GroupingPrivate) props.OrderingChoice {
	return private.Ordering
}

// AppendAggCols constructs a new Aggregations operator containing the aggregate
// functions from an existing Aggregations operator plus an additional set of
// aggregate functions, one for each column in the given set. The new functions
// are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols(
	aggs memo.AggregationsExpr, aggOp opt.Operator, cols opt.ColSet,
) memo.AggregationsExpr {
	outAggs := make(memo.AggregationsExpr, len(aggs)+cols.Len())
	copy(outAggs, aggs)
	c.makeAggCols(aggOp, cols, outAggs[len(aggs):])
	return outAggs
}

// MakeAggCols constructs a new Aggregations operator containing an aggregate
// function of the given operator type for each of column in the given set. For
// example, for ConstAggOp and columns (1,2), this expression is returned:
//
//   (Aggregations
//     [(ConstAgg (Variable 1)) (ConstAgg (Variable 2))]
//     [1,2]
//   )
//
func (c *CustomFuncs) MakeAggCols(aggOp opt.Operator, cols opt.ColSet) memo.AggregationsExpr {
	colsLen := cols.Len()
	aggs := make(memo.AggregationsExpr, colsLen)
	c.makeAggCols(aggOp, cols, aggs)
	return aggs
}

// ----------------------------------------------------------------------
//
// Join functions
//   General functions related to join operators.
//
// ----------------------------------------------------------------------

// JoinDoesNotDuplicateLeftRows returns true if the given InnerJoin, LeftJoin or
// FullJoin is guaranteed not to output any given row from its left input more
// than once.
func (c *CustomFuncs) JoinDoesNotDuplicateLeftRows(join memo.RelExpr) bool {
	mult := memo.GetJoinMultiplicity(join)
	return mult.JoinDoesNotDuplicateLeftRows(join.Op())
}

// JoinDoesNotDuplicateRightRows returns true if the given InnerJoin, LeftJoin
// or FullJoin is guaranteed not to output any given row from its right input
// more than once.
func (c *CustomFuncs) JoinDoesNotDuplicateRightRows(join memo.RelExpr) bool {
	mult := memo.GetJoinMultiplicity(join)
	return mult.JoinDoesNotDuplicateRightRows(join.Op())
}

// JoinPreservesLeftRows returns true if the given InnerJoin, LeftJoin or
// FullJoin is guaranteed to output every row from its left input at least once.
func (c *CustomFuncs) JoinPreservesLeftRows(join memo.RelExpr) bool {
	mult := memo.GetJoinMultiplicity(join)
	return mult.JoinPreservesLeftRows(join.Op())
}

// JoinPreservesRightRows returns true if the given InnerJoin, LeftJoin or
// FullJoin is guaranteed to output every row from its right input at least
// once.
func (c *CustomFuncs) JoinPreservesRightRows(join memo.RelExpr) bool {
	mult := memo.GetJoinMultiplicity(join)
	return mult.JoinPreservesRightRows(join.Op())
}

// NoJoinHints returns true if no hints were specified for this join.
func (c *CustomFuncs) NoJoinHints(p *memo.JoinPrivate) bool {
	return p.Flags.Empty()
}

// ----------------------------------------------------------------------
//
// Constant value functions
//   General functions related to constant values and datums.
//
// ----------------------------------------------------------------------

// IsPositiveInt is true if the given Datum value is greater than zero.
func (c *CustomFuncs) IsPositiveInt(datum tree.Datum) bool {
	val := int64(*datum.(*tree.DInt))
	return val > 0
}

// EqualsString returns true if the given strings are equal. This function is
// useful in matching expressions that have string fields.
//
// For example, NormalizeCmpTimeZoneFunction uses this function implicitly to
// match a specific function, like so:
//
//   (Function $args:* (FunctionPrivate "timezone"))
//
func (c *CustomFuncs) EqualsString(left string, right string) bool {
	return left == right
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

// AddConstInts adds the numeric constants together and constructs a Const.
// AddConstInts assumes the sum will not overflow. Call CanAddConstInts on the
// constants to guarantee this.
func (c *CustomFuncs) AddConstInts(first tree.Datum, second tree.Datum) opt.ScalarExpr {
	firstVal := int64(*first.(*tree.DInt))
	secondVal := int64(*second.(*tree.DInt))
	sum, ok := arith.AddWithOverflow(firstVal, secondVal)
	if !ok {
		panic(errors.AssertionFailedf("addition of %d and %d overflowed", firstVal, secondVal))
	}
	return c.f.ConstructConst(tree.NewDInt(tree.DInt(sum)), types.Int)
}

// CanAddConstInts returns true if the addition of the two integers overflows.
func (c *CustomFuncs) CanAddConstInts(first tree.Datum, second tree.Datum) bool {
	firstVal := int64(*first.(*tree.DInt))
	secondVal := int64(*second.(*tree.DInt))
	_, ok := arith.AddWithOverflow(firstVal, secondVal)
	return ok
}

// IntConst constructs a Const holding a DInt.
func (c *CustomFuncs) IntConst(d *tree.DInt) opt.ScalarExpr {
	return c.f.ConstructConst(d, types.Int)
}

// IsGreaterThan returns true if the first datum compares as greater than the
// second.
func (c *CustomFuncs) IsGreaterThan(first, second tree.Datum) bool {
	return first.Compare(c.f.evalCtx, second) == 1
}

// DatumsEqual returns true if the first datum compares as equal to the second.
func (c *CustomFuncs) DatumsEqual(first, second tree.Datum) bool {
	return first.Compare(c.f.evalCtx, second) == 0
}

// ----------------------------------------------------------------------
//
// Scan functions
//   General functions related to scan operators.
//
// ----------------------------------------------------------------------

// DuplicateScanPrivate constructs a new ScanPrivate with new table and column
// IDs. Only the Index, Flags and Locking fields are copied from the old
// ScanPrivate, so the new ScanPrivate will not have constraints even if the old
// one did.
func (c *CustomFuncs) DuplicateScanPrivate(sp *memo.ScanPrivate) *memo.ScanPrivate {
	table, cols := c.DuplicateColumnIDs(sp.Table, sp.Cols)
	return &memo.ScanPrivate{
		Table:   table,
		Index:   sp.Index,
		Cols:    cols,
		Flags:   sp.Flags,
		Locking: sp.Locking,
	}
}
