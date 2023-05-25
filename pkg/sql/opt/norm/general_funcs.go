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
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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

// IsJSON returns true if the given scalar expression is of type
// JSON.
func (c *CustomFuncs) IsJSON(scalar opt.ScalarExpr) bool {
	return scalar.DataType().Family() == types.JsonFamily
}

// IsInt returns true if the given scalar expression is of one of the
// integer types.
func (c *CustomFuncs) IsInt(scalar opt.ScalarExpr) bool {
	return scalar.DataType().Family() == types.IntFamily
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
	newTableID := md.DuplicateTable(table, c.f.RemapCols)

	// Build a new set of column IDs from the new TableMeta.
	var newColIDs opt.ColSet
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		ord := table.ColumnOrdinal(col)
		newColID := newTableID.ColumnID(ord)
		newColIDs.Add(newColID)
	}

	return newTableID, newColIDs
}

// MakeBoolCol creates a new column of type Bool and returns its ID.
func (c *CustomFuncs) MakeBoolCol() opt.ColumnID {
	return c.mem.Metadata().AddColumn("", types.Bool)
}

// CanRemapCols returns true if it's possible to remap every column in the
// "from" set to a column in the "to" set using the given FDs.
func (c *CustomFuncs) CanRemapCols(from, to opt.ColSet, fds *props.FuncDepSet) bool {
	for col, ok := from.Next(0); ok; col, ok = from.Next(col + 1) {
		if !fds.ComputeEquivGroup(col).Intersects(to) {
			// It is not possible to remap this column to one from the "to" set.
			return false
		}
	}
	return true
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
//	SELECT * FROM a WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x)
//
// The a.x variable in the EXISTS subquery references a column outside the scope
// of the subquery. It is an "outer column" for the subquery (see the comment on
// RelationalProps.OuterCols for more details).
func (c *CustomFuncs) HasOuterCols(input opt.Expr) bool {
	return !c.OuterCols(input).Empty()
}

// IsCorrelated returns true if any variable in the source expression references
// a column from the given set of output columns. For example:
//
//	(InnerJoin
//	  (Scan a)
//	  (Scan b)
//	  [ ... (FiltersItem $item:(Eq (Variable a.x) (Const 1))) ... ]
//	)
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
//	(InnerJoin
//	  (Scan a)
//	  (Scan b)
//	  [ ... $item:(FiltersItem (Eq (Variable a.x) (Const 1))) ... ]
//	)
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
//	(InnerJoin
//	  (Scan a)
//	  (Scan b)
//	  $filters:[ (FiltersItem (Eq (Variable a.x) (Const 1))) ]
//	)
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

// HasBoundedCardinality returns true if the input expression returns a bounded
// number of rows.
func (c *CustomFuncs) HasBoundedCardinality(input memo.RelExpr) bool {
	return !input.Relational().Cardinality.IsUnbounded()
}

// GetLimitFromCardinality returns a ConstExpr equal to the upper bound on the
// input expression's cardinality. It panics if the expression is unbounded.
func (c *CustomFuncs) GetLimitFromCardinality(input memo.RelExpr) opt.ScalarExpr {
	if input.Relational().Cardinality.IsUnbounded() {
		panic(errors.AssertionFailedf("called GetLimitFromCardinality on unbounded expression"))
	}
	return c.f.ConstructConstVal(
		tree.NewDInt(tree.DInt(input.Relational().Cardinality.Max)), types.Int,
	)
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
		memo.BuildSharedProps(e, &p, c.f.evalCtx)
		return &p
	}
}

// FuncDeps retrieves the FuncDepSet for the given expression.
func (c *CustomFuncs) FuncDeps(expr memo.RelExpr) *props.FuncDepSet {
	return &expr.Relational().FuncDeps
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
//	(InnerJoin
//	  (Scan a)
//	  (Scan b)
//	  (Filters [
//	    (Eq (Variable a.x) (Variable b.x))
//	    (Gt (Variable a.x) (Const 1))
//	  ])
//	)
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

// ComputedColFilters generates all filters that can be derived from the list of
// computed column expressions from the given table. A computed column can be
// used as a filter when it has a constant value. That is true when:
//
//  1. All other columns it references are constant, because other filters in
//     the query constrain them to be so.
//  2. All functions in the computed column expression can be folded into
//     constants (i.e. they do not have problematic side effects).
//
// Note that computed columns can depend on other computed columns; in general
// the dependencies form an acyclic directed graph. ComputedColFilters will
// return filters for all constant computed columns, regardless of the order of
// their dependencies.
//
// As with checkConstraintFilters, ComputedColFilters do not really filter any
// rows, they are rather facts or guarantees about the data. Treating them as
// filters may allow some indexes to be constrained and used. Consider the
// following example:
//
//	CREATE TABLE t (
//	  k INT NOT NULL,
//	  hash INT AS (k % 4) STORED,
//	  PRIMARY KEY (hash, k)
//	)
//
//	SELECT * FROM t WHERE k = 5
//
// Notice that the filter provided explicitly wouldn't allow the optimizer to
// seek using the primary index (it would have to fall back to a table scan).
// However, column "hash" can be proven to have the constant value of 1, since
// it's dependent on column "k", which has the constant value of 5. This enables
// usage of the primary index:
//
//	scan t
//	 ├── columns: k:1(int!null) hash:2(int!null)
//	 ├── constraint: /2/1: [/1/5 - /1/5]
//	 ├── key: (2)
//	 └── fd: ()-->(1)
//
// The values of both columns in that index are known, enabling a single value
// constraint to be generated.
func (c *CustomFuncs) ComputedColFilters(
	scanPrivate *memo.ScanPrivate, requiredFilters, optionalFilters memo.FiltersExpr,
) memo.FiltersExpr {
	tabMeta := c.mem.Metadata().TableMeta(scanPrivate.Table)
	if len(tabMeta.ComputedCols) == 0 {
		return nil
	}

	// Start with set of constant columns, as derived from the list of filter
	// conditions.
	constCols := make(constColsMap)
	c.findConstantFilterCols(constCols, scanPrivate, requiredFilters)
	c.findConstantFilterCols(constCols, scanPrivate, optionalFilters)
	if len(constCols) == 0 {
		// No constant values could be derived from filters, so assume that there
		// are also no constant computed columns.
		return nil
	}

	// Construct a new filter condition for each computed column that is
	// constant (i.e. all of its variables are in the constCols set).
	var computedColFilters memo.FiltersExpr
	for colID := range tabMeta.ComputedCols {
		if c.tryFoldComputedCol(tabMeta.ComputedCols, colID, constCols) {
			constVal := constCols[colID]
			// Note: Eq is not correct here because of NULLs.
			eqOp := c.f.ConstructIs(c.f.ConstructVariable(colID), constVal)
			computedColFilters = append(computedColFilters, c.f.ConstructFiltersItem(eqOp))
		}
	}
	return computedColFilters
}

// constColsMap maps columns to constant values that we can infer from query
// filters.
//
// Note that for composite types, the constant value is not interchangeable with
// the column in all contexts. Composite types are types which can have
// logically equal but not identical values, like the decimals 1.0 and 1.00.
//
// For example:
//
//	CREATE TABLE t (
//	  d DECIMAL,
//	  c DECIMAL AS (d*10) STORED
//	);
//	INSERT INTO t VALUES (1.0), (1.00), (1.000);
//	SELECT c::STRING FROM t WHERE d=1;
//	----
//	  10.0
//	  10.00
//	  10.000
//
// We can infer that c has a constant value of 1 but we can't replace it with 1
// in any expression.
type constColsMap map[opt.ColumnID]opt.ScalarExpr

// findConstantFilterCols adds to constFilterCols mappings from table column ID
// to the constant value of that column. It does this by iterating over the
// given lists of filters and finding expressions that constrain columns to a
// single constant value. For example:
//
//	x = 5 AND y = 'foo'
//
// This would add a mapping from x => 5 and y => 'foo', which constants can
// then be used to prove that dependent computed columns are also constant.
func (c *CustomFuncs) findConstantFilterCols(
	constFilterCols constColsMap, scanPrivate *memo.ScanPrivate, filters memo.FiltersExpr,
) {
	tab := c.mem.Metadata().Table(scanPrivate.Table)
	for i := range filters {
		// If filter constraints are not tight, then no way to derive constant
		// values.
		props := filters[i].ScalarProps()
		if !props.TightConstraints {
			continue
		}

		// Iterate over constraint conjuncts with a single column and single
		// span having a single key.
		for i, n := 0, props.Constraints.Length(); i < n; i++ {
			cons := props.Constraints.Constraint(i)
			if cons.Columns.Count() != 1 || cons.Spans.Count() != 1 {
				continue
			}

			// Skip columns that aren't in the scanned table.
			colID := cons.Columns.Get(0).ID()
			if !scanPrivate.Cols.Contains(colID) {
				continue
			}

			colTyp := tab.Column(scanPrivate.Table.ColumnOrdinal(colID)).DatumType()

			span := cons.Spans.Get(0)
			if !span.HasSingleKey(c.f.evalCtx) {
				continue
			}

			datum := span.StartKey().Value(0)
			constFilterCols[colID] = c.f.ConstructConstVal(datum, colTyp)
		}
	}
}

// tryFoldComputedCol tries to reduce the computed column with the given column
// ID into a constant value, by evaluating it with respect to a set of other
// columns that are constant. If the computed column is constant, enter it into
// the constCols map and return true. Otherwise, return false.
func (c *CustomFuncs) tryFoldComputedCol(
	ComputedCols map[opt.ColumnID]opt.ScalarExpr, computedColID opt.ColumnID, constCols constColsMap,
) bool {
	// Check whether computed column has already been folded.
	if _, ok := constCols[computedColID]; ok {
		return true
	}

	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		if variable, ok := e.(*memo.VariableExpr); ok {
			// Can variable be folded?
			if constVal, ok := constCols[variable.Col]; ok {
				// Yes, so replace it with its constant value.
				return constVal
			}

			// No, but that may be because the variable refers to a dependent
			// computed column. In that case, try to recursively fold that
			// computed column. There are no infinite loops possible because the
			// dependency graph is guaranteed to be acyclic.
			if _, ok := ComputedCols[variable.Col]; ok {
				if c.tryFoldComputedCol(ComputedCols, variable.Col, constCols) {
					return constCols[variable.Col]
				}
			}

			return e
		}
		return c.f.Replace(e, replace)
	}

	computedCol := ComputedCols[computedColID]
	if memo.CanBeCompositeSensitive(c.f.mem.Metadata(), computedCol) {
		// The computed column expression can return different values for logically
		// equal outer columns (e.g. d::STRING where d is a DECIMAL).
		return false
	}
	replaced := replace(computedCol).(opt.ScalarExpr)

	// If the computed column is constant, enter it into the constCols map.
	if opt.IsConstValueOp(replaced) {
		constCols[computedColID] = replaced
		return true
	}
	return false
}

// CombineComputedColFilters is a generalized version of ComputedColFilters,
// which seeks to fold computed column expressions using values from single-key
// constraint spans in order to build new predicates on those computed columns
// and AND them with predicates built from the constraint span key used to
// compute the computed column value.
//
// New derived keys cannot be saved for the filters as a whole, for later
// processing, because a given combination of span key values is only applicable
// with the scope of that predicate. For example:
//
// Example:
//
//	CREATE TABLE t1 (
//	a INT,
//	b INT,
//	c INT AS (a+b) VIRTUAL,
//	INDEX idx1 (c ASC, b ASC, a ASC));
//
// SELECT * FROM t1 WHERE (a,b) IN ((3,4), (5,6));
//
// Here we derive:
//
//	(a IS 3 AND b IS 4 AND c IS 7) OR
//	(a IS 5 AND b IS 6 AND c IS 11)
//
// The `c IS 7` term is only applicable when a is 3 and b is 4, so those two
// terms must be combined with the `c IS 7` term in the same conjunction for the
// result to be semantically equivalent to the original query.
// Here is the plan built in this case:
//
//	scan t1@idx1
//	├── columns: a:1!null b:2!null c:3!null
//	├── constraint: /3/2/1
//	│    ├── [/7/4/3 - /7/4/3]
//	│    └── [/11/6/5 - /11/6/5]
//	├── cardinality: [0 - 2]
//	├── key: (1,2)
//	└── fd: (1,2)-->(3)
//
// Note that new predicates are derived from constraint spans, not predicates,
// so a predicate such as (a,b) IN ((null,4), (5,6)) will not derive anything
// because the null is not placed in a span, and also IN predicates with nulls
// are not marked as tight, and this function does not process non-tight
// constraints.
//
// Note that `ComputedColFilters` can't be replaced because it can handle cases
// which `combineComputedColFilters` doesn't, for example a computed column
// which is built from constants found in separate `FiltersItem`s.
//
// Also note that `CombineComputedColFilters` must only be called on a tight
// constraint. It is up to the caller to test whether the constraint is tight
// before calling this function.
func CombineComputedColFilters(
	computedCols map[opt.ColumnID]opt.ScalarExpr,
	indexKeyCols opt.ColSet,
	colsInComputedColsExpressions opt.ColSet,
	cons *constraint.Constraint,
	f *Factory,
) memo.FiltersExpr {
	if len(computedCols) == 0 {
		return nil
	}
	if !f.evalCtx.SessionData().OptimizerUseImprovedComputedColumnFiltersDerivation {
		return nil
	}
	var combinedComputedColFilters memo.FiltersExpr
	var orOp opt.ScalarExpr
	if !cons.Columns.ColSet().Intersects(colsInComputedColsExpressions) {
		// If this constraint doesn't involve any columns used to construct a
		// computed column value, no need to process it further.
		return nil
	}
	for k := 0; k < cons.Spans.Count(); k++ {
		filterAdded := false
		span := cons.Spans.Get(k)
		if !span.HasSingleKey(f.evalCtx) {
			// If we don't have a single value, or combination of single values
			// to use in folding the computed column expression, don't use this
			// constraint.
			return nil
		}
		// Build the initial conjunction and constColsMap from the constraint.
		initialConjunction, constFilterCols, ok :=
			buildConjunctionAndConstColsMapFromConstraint(cons, span, f)
		if !ok {
			return nil
		}
		var newOp opt.ScalarExpr
		// Build a new ANDed predicate involving the computed column and columns in
		// the computed column expression.
		newOp, filterAdded =
			buildConjunctionOfEqualityPredsOnComputedAndNonComputedCols(
				initialConjunction, computedCols, constFilterCols, indexKeyCols, f)
		// Only build a new disjunct if terms were derived for this span.
		if filterAdded {
			if orOp == nil {
				// The case of the first span or only one span.
				orOp = newOp
			} else {
				// The spans in a constraint represent a disjunction, so OR them
				// together.
				constructOr := func() {
					orOp = f.ConstructOr(orOp, newOp)
				}
				var disabledRules intsets.Fast
				// Disable this rule as it disallows finding tight constraints in
				// some cases when a conjunct can be factored out, e.g.,
				// `(a=3 AND b=4) OR (a=3 AND b=6)` --> `a=3 AND (b=4 OR b=6)`
				disabledRules.Add(int(opt.ExtractRedundantConjunct))
				f.DisableOptimizationRulesTemporarily(disabledRules, constructOr)
			}
		} else {
			// If we failed to build any of the disjuncts, we must give up.
			return nil
		}
	}
	if orOp != nil {
		combinedComputedColFilters =
			append(combinedComputedColFilters, f.ConstructFiltersItem(orOp))
	}
	return combinedComputedColFilters
}

// buildConstColsMapFromConstraint converts a constraint on one or more columns
// into a scalar expression predicate in the form:
// (constraint_col1 IS span1) AND (constraint_col2 IS span2) AND ...
func buildConjunctionAndConstColsMapFromConstraint(
	cons *constraint.Constraint, span *constraint.Span, f *Factory,
) (conjunction opt.ScalarExpr, constFilterCols constColsMap, ok bool) {
	for i := 0; i < cons.Columns.Count(); i++ {
		colID := cons.Columns.Get(i).ID()
		datum := span.StartKey().Value(i)
		colTyp := datum.ResolvedType()
		originalConstVal := f.ConstructConstVal(datum, colTyp)
		// Mark the value in the map for use by `tryFoldComputedCol`.
		if constFilterCols == nil {
			constFilterCols = make(constColsMap)
		}
		constFilterCols[colID] = originalConstVal
		// Note: Nulls could be handled here, but they are explicitly disallowed for
		// now.
		if _, isNullExpr := originalConstVal.(*memo.NullExpr); isNullExpr {
			ok = false
			break
		}
		// Use IS NOT DISTINCT FROM to handle nulls.
		originalEqOp := f.ConstructIs(f.ConstructVariable(colID), originalConstVal)
		if conjunction == nil {
			conjunction = originalEqOp
		} else {
			// Build a conjunction representing this span.
			conjunction = f.ConstructAnd(conjunction, originalEqOp)
		}
		ok = true
	}
	if !ok {
		if len(constFilterCols) != 0 {
			// Help the garbage collector.
			for k := range constFilterCols {
				delete(constFilterCols, k)
			}
		}
		return nil, nil, false
	}
	return conjunction, constFilterCols, true
}

// buildConjunctionOfEqualityPredsOnComputedAndNonComputedCols takes an
// initialConjunction of IS equality predicates on non-computed columns and
// attempts to build a new conjunction of IS equality predicates on computed
// columns which are part of the index key by folding the computed column
// expressions using the constant values stored in the constFilterCols map. If
// successful, ok=true is returned along with initialConjunction ANDed with the
// predicates on computed columns.
func buildConjunctionOfEqualityPredsOnComputedAndNonComputedCols(
	initialConjunction opt.ScalarExpr,
	computedCols map[opt.ColumnID]opt.ScalarExpr,
	constFilterCols constColsMap,
	indexKeyCols opt.ColSet,
	f *Factory,
) (computedColPlusOriginalColEqualityConjunction opt.ScalarExpr, ok bool) {
	computedColPlusOriginalColEqualityConjunction = initialConjunction
	for computedColID := range computedCols {
		if !indexKeyCols.Contains(computedColID) {
			continue
		}
		if f.CustomFuncs().tryFoldComputedCol(computedCols, computedColID, constFilterCols) {
			constVal := constFilterCols[computedColID]
			// Use IS NOT DISTINCT FROM to handle nulls.
			eqOp := f.ConstructIs(f.ConstructVariable(computedColID), constVal)
			computedColPlusOriginalColEqualityConjunction =
				f.ConstructAnd(computedColPlusOriginalColEqualityConjunction, eqOp)
			ok = true
		}
	}
	if !ok {
		return nil, false
	}
	return computedColPlusOriginalColEqualityConjunction, true
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
//	(Aggregations
//	  [(ConstAgg (Variable 1)) (ConstAgg (Variable 2))]
//	  [1,2]
//	)
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

// IndexJoinPreservesRows returns true if the index join is guaranteed to
// produce the same number of rows as its input.
func (c *CustomFuncs) IndexJoinPreservesRows(p *memo.IndexJoinPrivate) bool {
	return p.Locking.WaitPolicy != tree.LockWaitSkipLocked
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
//	(Function $args:* (FunctionPrivate "timezone"))
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

// DuplicateJoinPrivate copies a JoinPrivate, preserving the Flags and
// SkipReorderJoins field values.
func (c *CustomFuncs) DuplicateJoinPrivate(jp *memo.JoinPrivate) *memo.JoinPrivate {
	return &memo.JoinPrivate{
		Flags:            jp.Flags,
		SkipReorderJoins: jp.SkipReorderJoins,
	}
}
