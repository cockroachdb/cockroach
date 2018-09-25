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

	// scratchItems is a slice that is reused by ListBuilder to store temporary
	// results that are accumulated before passing them to memo.InternList.
	scratchItems []memo.GroupID
}

// Init initializes a new CustomFuncs with the given factory.
func (c *CustomFuncs) Init(f *Factory) {
	c.f = f
	c.mem = f.Memo()
}

// ----------------------------------------------------------------------
//
// List functions
//   General custom match and replace functions used to test and construct
//   lists.
//
// ----------------------------------------------------------------------

// InternSingletonList interns a list containing the single given item and
// returns its id.
func (c *CustomFuncs) InternSingletonList(item memo.GroupID) memo.ListID {
	b := MakeListBuilder(c)
	b.AddItem(item)
	return b.BuildList()
}

// ListOnlyHasNulls if every item in the given list is a Null op. If the list
// is empty, ListOnlyHasNulls returns false.
func (c *CustomFuncs) ListOnlyHasNulls(list memo.ListID) bool {
	if list.Length == 0 {
		return false
	}

	for _, item := range c.mem.LookupList(list) {
		if c.mem.NormOp(item) != opt.NullOp {
			return false
		}
	}
	return true
}

// RemoveListItem returns a new list that is a copy of the given list, except
// that it does not contain the given search item. If the list contains the item
// multiple times, then only the first instance is removed. If the list does not
// contain the item, then the method is a no-op.
func (c *CustomFuncs) RemoveListItem(list memo.ListID, search memo.GroupID) memo.ListID {
	existingList := c.mem.LookupList(list)
	b := MakeListBuilder(c)
	for i, item := range existingList {
		if item == search {
			b.AddItems(existingList[i+1:])
			break
		}
		b.AddItem(item)
	}
	return b.BuildList()
}

// ReplaceListItem returns a new list that is a copy of the given list, except
// that the given search item has been replaced by the given replace item. If
// the list contains the search item multiple times, then only the first
// instance is replaced. If the list does not contain the item, then the method
// is a no-op.
func (c *CustomFuncs) ReplaceListItem(list memo.ListID, search, replace memo.GroupID) memo.ListID {
	existingList := c.mem.LookupList(list)
	b := MakeListBuilder(c)

	for i, item := range existingList {
		if item == search {
			// Replace item and copy remainder of list.
			b.AddItem(replace)
			b.AddItems(existingList[i+1:])
			break
		}
		b.AddItem(item)
	}
	return b.BuildList()
}

// IsSortedUniqueList returns true if the list is in sorted order, with no
// duplicates. See the comment for listSorter.compare for comparison rule
// details.
func (c *CustomFuncs) IsSortedUniqueList(list memo.ListID) bool {
	if list.Length <= 1 {
		return true
	}
	ls := listSorter{cf: c, list: c.mem.LookupList(list)}
	for i := 0; i < int(list.Length-1); i++ {
		if !ls.less(i, i+1) {
			return false
		}
	}
	return true
}

// ConstructSortedUniqueList sorts the given list and removes duplicates, and
// returns the resulting list. See the comment for listSorter.compare for
// comparison rule details.
func (c *CustomFuncs) ConstructSortedUniqueList(list memo.ListID) (memo.ListID, memo.PrivateID) {
	// Make a copy of the list, since it needs to stay immutable.
	lb := MakeListBuilder(c)
	lb.AddItems(c.mem.LookupList(list))
	ls := listSorter{cf: c, list: lb.items}

	// Sort the list.
	sort.Slice(ls.list, ls.less)

	// Remove duplicates from the list.
	n := 0
	for i := 0; i < int(list.Length); i++ {
		if i == 0 || ls.compare(i-1, i) < 0 {
			lb.items[n] = lb.items[i]
			n++
		}
	}
	lb.setLength(n)

	// Construct the type of the tuple.
	typ := types.TTuple{Types: make([]types.T, n)}
	for i := 0; i < n; i++ {
		typ.Types[i] = c.mem.GroupProperties(lb.items[i]).Scalar.Type
	}

	return lb.BuildList(), c.mem.InternType(typ)
}

// ----------------------------------------------------------------------
//
// Typing functions
//   General custom match and replace functions used to test and construct
//   expression data types.
//
// ----------------------------------------------------------------------

// HasColType returns true if the given expression has a static type that's
// equivalent to the requested coltype.
func (c *CustomFuncs) HasColType(group memo.GroupID, colTyp memo.PrivateID) bool {
	srcTyp, _ := coltypes.DatumTypeToColumnType(c.LookupScalar(group).Type)
	dstTyp := c.mem.LookupPrivate(colTyp).(coltypes.T)
	if reflect.TypeOf(srcTyp) != reflect.TypeOf(dstTyp) {
		return false
	}
	return coltypes.ColTypeAsString(srcTyp) == coltypes.ColTypeAsString(dstTyp)
}

// IsString returns true if the given expression is of type String.
func (c *CustomFuncs) IsString(group memo.GroupID) bool {
	return c.LookupScalar(group).Type == types.String
}

// ColTypeToDatumType maps the given column type to a datum type.
func (c *CustomFuncs) ColTypeToDatumType(colTyp memo.PrivateID) memo.PrivateID {
	datumTyp := coltypes.CastTargetToDatumType(c.mem.LookupPrivate(colTyp).(coltypes.T))
	return c.mem.InternType(datumTyp)
}

// BoolType returns the private ID of the boolean SQL type.
func (c *CustomFuncs) BoolType() memo.PrivateID {
	return c.f.InternType(types.Bool)
}

// AnyType returns the private ID of the wildcard Any type.
func (c *CustomFuncs) AnyType() memo.PrivateID {
	return c.f.InternType(types.Any)
}

// CanConstructBinary returns true if (op left right) has a valid binary op
// overload and is therefore legal to construct. For example, while
// (Minus <date> <int>) is valid, (Minus <int> <date>) is not.
func (c *CustomFuncs) CanConstructBinary(op opt.Operator, left, right memo.GroupID) bool {
	leftType := c.LookupScalar(left).Type
	rightType := c.LookupScalar(right).Type
	return memo.BinaryOverloadExists(op, rightType, leftType)
}

// ----------------------------------------------------------------------
//
// Property functions
//   General custom match and replace functions used to test expression
//   logical properties.
//
// ----------------------------------------------------------------------

// Operator returns the type of the given group's normalized expression.
func (c *CustomFuncs) Operator(group memo.GroupID) opt.Operator {
	return c.mem.NormOp(group)
}

// LookupLogical returns the given group's logical properties.
func (c *CustomFuncs) LookupLogical(group memo.GroupID) *props.Logical {
	return c.mem.GroupProperties(group)
}

// LookupRelational returns the given group's logical relational properties.
func (c *CustomFuncs) LookupRelational(group memo.GroupID) *props.Relational {
	return c.LookupLogical(group).Relational
}

// LookupScalar returns the given group's logical scalar properties.
func (c *CustomFuncs) LookupScalar(group memo.GroupID) *props.Scalar {
	return c.LookupLogical(group).Scalar
}

// OutputCols is a helper function that extracts the set of columns projected
// by the given operator. In addition to extracting columns from any relational
// operator, OutputCols can also extract columns from the Projections and
// Aggregations scalar operators, which are used with Project and GroupBy.
func (c *CustomFuncs) OutputCols(group memo.GroupID) opt.ColSet {
	// Handle columns projected by relational operators.
	logical := c.LookupLogical(group)
	if logical.Relational != nil {
		return c.LookupRelational(group).OutputCols
	}

	expr := c.mem.NormExpr(group)
	switch expr.Operator() {
	case opt.AggregationsOp:
		return c.ExtractColList(expr.AsAggregations().Cols()).ToSet()

	case opt.ProjectionsOp:
		return c.ExtractProjectionsOpDef(expr.AsProjections().Def()).AllCols()

	default:
		panic(fmt.Sprintf("OutputCols doesn't support op %s", expr.Operator()))
	}
}

// OutputCols2 returns the union of the OutputCols for the two operators.
func (c *CustomFuncs) OutputCols2(group1, group2 memo.GroupID) opt.ColSet {
	return c.OutputCols(group1).Union(c.OutputCols(group2))
}

// OuterCols returns the set of outer columns associated with the given group,
// whether it be a relational or scalar operator.
func (c *CustomFuncs) OuterCols(group memo.GroupID) opt.ColSet {
	return c.LookupLogical(group).OuterCols()
}

// CandidateKey returns the candidate key columns from the given group. If there
// is no candidate key, CandidateKey returns ok=false.
func (c *CustomFuncs) CandidateKey(group memo.GroupID) (key opt.ColSet, ok bool) {
	return c.LookupLogical(group).Relational.FuncDeps.Key()
}

// IsColNotNull returns true if the given column is part of the input group's
// set of not-null columns.
func (c *CustomFuncs) IsColNotNull(col memo.PrivateID, input memo.GroupID) bool {
	return c.LookupLogical(input).Relational.NotNullCols.Contains(int(c.ExtractColID(col)))
}

// IsColNotNull2 returns true if the given column is part of the left or right
// groups' set of not-null columns.
func (c *CustomFuncs) IsColNotNull2(col memo.PrivateID, left, right memo.GroupID) bool {
	colID := int(c.ExtractColID(col))
	return c.LookupLogical(left).Relational.NotNullCols.Contains(colID) ||
		c.LookupLogical(right).Relational.NotNullCols.Contains(colID)
}

// HasOuterCols returns true if the given group has at least one outer column,
// or in other words, a reference to a variable that is not bound within its
// own scope. For example:
//
//   SELECT * FROM a WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x)
//
// The a.x variable in the EXISTS subquery references a column outside the scope
// of the subquery. It is an "outer column" for the subquery (see the comment on
// RelationalProps.OuterCols for more details).
func (c *CustomFuncs) HasOuterCols(group memo.GroupID) bool {
	return !c.OuterCols(group).Empty()
}

// OnlyConstants returns true if the scalar expression is a "constant
// expression tree", meaning that it will always evaluate to the same result.
// See the CommuteConst pattern comment for more details.
func (c *CustomFuncs) OnlyConstants(group memo.GroupID) bool {
	scalar := c.LookupScalar(group)
	return scalar.OuterCols.Empty() && !scalar.CanHaveSideEffects
}

// HasNoCols returns true if the group has zero output columns.
func (c *CustomFuncs) HasNoCols(group memo.GroupID) bool {
	return c.OutputCols(group).Empty()
}

// HasSameCols returns true if the two groups have an identical set of output
// columns.
func (c *CustomFuncs) HasSameCols(left, right memo.GroupID) bool {
	return c.OutputCols(left).Equals(c.OutputCols(right))
}

// HasSubsetCols returns true if the left group's output columns are a subset of
// the right group's output columns.
func (c *CustomFuncs) HasSubsetCols(left, right memo.GroupID) bool {
	return c.OutputCols(left).SubsetOf(c.OutputCols(right))
}

// HasZeroRows returns true if the given group never returns any rows.
func (c *CustomFuncs) HasZeroRows(group memo.GroupID) bool {
	return c.mem.GroupProperties(group).Relational.Cardinality.IsZero()
}

// HasOneRow returns true if the given group always returns exactly one row.
func (c *CustomFuncs) HasOneRow(group memo.GroupID) bool {
	return c.mem.GroupProperties(group).Relational.Cardinality.IsOne()
}

// HasZeroOrOneRow returns true if the given group returns at most one row.
func (c *CustomFuncs) HasZeroOrOneRow(group memo.GroupID) bool {
	return c.mem.GroupProperties(group).Relational.Cardinality.IsZeroOrOne()
}

// CanHaveZeroRows returns true if the given group might return zero rows.
func (c *CustomFuncs) CanHaveZeroRows(group memo.GroupID) bool {
	return c.mem.GroupProperties(group).Relational.Cardinality.CanBeZero()
}

// HasCorrelatedSubquery returns true if the given scalar group contains a
// subquery within its subtree that has at least one outer column.
func (c *CustomFuncs) HasCorrelatedSubquery(group memo.GroupID) bool {
	return c.LookupScalar(group).HasCorrelatedSubquery
}

// ----------------------------------------------------------------------
//
// Private extraction functions
//   Helper functions that make extracting common private types easier.
//
// ----------------------------------------------------------------------

// ExtractColID extracts an opt.ColumnID from the given private.
func (c *CustomFuncs) ExtractColID(private memo.PrivateID) opt.ColumnID {
	return c.mem.LookupPrivate(private).(opt.ColumnID)
}

// ExtractColList extracts an opt.ColList from the given private.
func (c *CustomFuncs) ExtractColList(private memo.PrivateID) opt.ColList {
	return c.mem.LookupPrivate(private).(opt.ColList)
}

// ExtractOrdering extracts an props.OrderingChoice from the given private.
func (c *CustomFuncs) ExtractOrdering(private memo.PrivateID) *props.OrderingChoice {
	return c.mem.LookupPrivate(private).(*props.OrderingChoice)
}

// ExtractProjectionsOpDef extracts a *memo.ProjectionsOpDef from the given
// private.
func (c *CustomFuncs) ExtractProjectionsOpDef(private memo.PrivateID) *memo.ProjectionsOpDef {
	return c.mem.LookupPrivate(private).(*memo.ProjectionsOpDef)
}

// ExtractType extracts a types.T from the given private.
func (c *CustomFuncs) ExtractType(private memo.PrivateID) types.T {
	return c.mem.LookupPrivate(private).(types.T)
}

// ----------------------------------------------------------------------
//
// Ordering functions
//   General custom match and replace functions related to orderings.
//
// ----------------------------------------------------------------------

// HasColsInOrdering returns true if all columns that appear in an ordering are
// output columns of the given group.
func (c *CustomFuncs) HasColsInOrdering(group memo.GroupID, ordering memo.PrivateID) bool {
	outCols := c.OutputCols(group)
	return c.ExtractOrdering(ordering).CanProjectCols(outCols)
}

// PruneOrdering removes any columns referenced by an OrderingChoice that are
// not output columns of the given group. Can only be called if
// HasColsInOrdering is true.
func (c *CustomFuncs) PruneOrdering(group memo.GroupID, ordering memo.PrivateID) memo.PrivateID {
	outCols := c.OutputCols(group)
	ord := c.ExtractOrdering(ordering)
	if ord.SubsetOfCols(outCols) {
		return ordering
	}
	ordCopy := ord.Copy()
	ordCopy.ProjectCols(outCols)
	return c.f.InternOrderingChoice(&ordCopy)
}

// -----------------------------------------------------------------------
//
// Filter functions
//   General custom match and replace functions used to test and construct
//   filters in Select and Join rules.
//
// -----------------------------------------------------------------------

// IsBoundBy returns true if all outer references in the source expression are
// bound by the given columns. For example:
//
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     (Eq (Variable a.x) (Const 1))
//   )
//
// The (Eq) expression is fully bound by the output columns of the (Scan a)
// expression because all of its outer references are satisfied by the columns
// produced by the Scan.
func (c *CustomFuncs) IsBoundBy(src memo.GroupID, cols opt.ColSet) bool {
	return c.OuterCols(src).SubsetOf(cols)
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
func (c *CustomFuncs) ExtractBoundConditions(list memo.ListID, cols opt.ColSet) memo.ListID {
	lb := MakeListBuilder(c)
	for _, item := range c.mem.LookupList(list) {
		if c.IsBoundBy(item, cols) {
			lb.AddItem(item)
		}
	}
	return lb.BuildList()
}

// ExtractUnboundConditions is the opposite of ExtractBoundConditions. Instead of
// extracting expressions that are bound by the given expression, it extracts
// list expressions that have at least one outer reference that is *not* bound
// by the given columns (i.e. it has a "free" variable).
func (c *CustomFuncs) ExtractUnboundConditions(list memo.ListID, cols opt.ColSet) memo.ListID {
	lb := MakeListBuilder(c)
	for _, item := range c.mem.LookupList(list) {
		if !c.IsBoundBy(item, cols) {
			lb.AddItem(item)
		}
	}
	return lb.BuildList()
}

// ----------------------------------------------------------------------
//
// Projection construction functions
//   General helper functions to construct Projections.
//
// ----------------------------------------------------------------------

// ProjectColsFromBoth returns a Projections operator that combines distinct
// columns from both the provided left and right groups. If the group is a
// Projections operator, then the projected expressions will be directly added
// to the new Projections operator. Otherwise, the group's output columns will
// be added as passthrough columns to the new Projections operator.
func (c *CustomFuncs) ProjectColsFromBoth(left, right memo.GroupID) memo.GroupID {
	pb := projectionsBuilder{f: c.f}
	if c.Operator(left) == opt.ProjectionsOp {
		pb.addProjections(left)
	} else {
		pb.addPassthroughCols(c.OutputCols(left))
	}
	if c.Operator(right) == opt.ProjectionsOp {
		pb.addProjections(right)
	} else {
		pb.addPassthroughCols(c.OutputCols(right))
	}
	return pb.buildProjections()
}

// ProjectColMapLeft returns a Projections operator that maps the left side columns
// in a SetOpColMap to the output columns in it. Useful for replacing set operations
// with simpler constructs.
func (c *CustomFuncs) ProjectColMapLeft(colMapID memo.PrivateID) memo.GroupID {
	colMap := c.mem.LookupPrivate(colMapID).(*memo.SetOpColMap)
	return c.projectColMapSide(colMap.Out, colMap.Left)
}

// ProjectColMapRight returns a Projections operator that maps the right side columns
// in a SetOpColMap to the output columns in it. Useful for replacing set operations
// with simpler constructs.
func (c *CustomFuncs) ProjectColMapRight(colMapID memo.PrivateID) memo.GroupID {
	colMap := c.mem.LookupPrivate(colMapID).(*memo.SetOpColMap)
	return c.projectColMapSide(colMap.Out, colMap.Right)
}

// projectColMapSide implements the side-agnostic logic from ProjectColMapLeft
// and ProjectColMapRight.
func (c *CustomFuncs) projectColMapSide(toList opt.ColList, fromList opt.ColList) memo.GroupID {
	pb := projectionsBuilder{f: c.f}

	for idx, fromCol := range fromList {
		toCol := toList[idx]

		pb.addSynthesized(c.f.ConstructVariable(c.f.InternColumnID(fromCol)), toCol)
	}

	return pb.buildProjections()
}

// ----------------------------------------------------------------------
//
// Select Rules
//   Custom match and replace functions used with select.opt rules.
//
// ----------------------------------------------------------------------

// IsCorrelated returns true if any variable in the source expression references
// a column from the destination expression. For example:
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     (Eq (Variable a.x) (Const 1))
//   )
//
// The (Eq) expression is correlated with the (Scan a) expression because it
// references one of its columns. But the (Eq) expression is not correlated
// with the (Scan b) expression.
func (c *CustomFuncs) IsCorrelated(src, dst memo.GroupID) bool {
	return c.OuterCols(src).Intersects(c.OutputCols(dst))
}

// ConcatFilters creates a new Filters operator that contains conditions from
// both the left and right boolean filter expressions. If the left or right
// expression is itself a Filters operator, then it is "flattened" by merging
// its conditions into the new Filters operator.
func (c *CustomFuncs) ConcatFilters(left, right memo.GroupID) memo.GroupID {
	leftExpr := c.mem.NormExpr(left)
	rightExpr := c.mem.NormExpr(right)

	// Handle cases where left/right filters are constant boolean values.
	if leftExpr.Operator() == opt.TrueOp || rightExpr.Operator() == opt.FalseOp {
		return right
	}
	if rightExpr.Operator() == opt.TrueOp || leftExpr.Operator() == opt.FalseOp {
		return left
	}

	// Determine how large to make the conditions slice (at least 2 slots).
	cnt := 2
	leftFiltersExpr := leftExpr.AsFilters()
	if leftFiltersExpr != nil {
		cnt += int(leftFiltersExpr.Conditions().Length) - 1
	}
	rightFiltersExpr := rightExpr.AsFilters()
	if rightFiltersExpr != nil {
		cnt += int(rightFiltersExpr.Conditions().Length) - 1
	}

	// Create the conditions slice and populate it.
	lb := MakeListBuilder(c)

	if leftFiltersExpr != nil {
		lb.AddItems(c.mem.LookupList(leftFiltersExpr.Conditions()))
	} else {
		lb.AddItem(left)
	}
	if rightFiltersExpr != nil {
		lb.AddItems(c.mem.LookupList(rightFiltersExpr.Conditions()))
	} else {
		lb.AddItem(right)
	}
	return c.f.ConstructFilters(lb.BuildList())
}

// ConstructEmptyValues constructs a Values expression with no rows.
func (c *CustomFuncs) ConstructEmptyValues(cols opt.ColSet) memo.GroupID {
	colList := make(opt.ColList, 0, cols.Len())
	for i, ok := cols.Next(0); ok; i, ok = cols.Next(i + 1) {
		colList = append(colList, opt.ColumnID(i))
	}
	return c.f.ConstructValues(memo.EmptyList, c.f.InternColList(colList))
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// GroupingAndConstCols returns the grouping columns and ConstAgg columns (for
// which the input and output column IDs match). A filter on these columns can
// be pushed through a GroupBy.
func (c *CustomFuncs) GroupingAndConstCols(def memo.PrivateID, aggs memo.GroupID) opt.ColSet {
	result := c.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols.Copy()

	aggsExpr := c.mem.NormExpr(aggs).AsAggregations()
	aggsElems := c.mem.LookupList(aggsExpr.Aggs())
	aggsColList := c.ExtractColList(aggsExpr.Cols())

	// Add any ConstAgg columns.
	for i := range aggsElems {
		if constAgg := c.mem.NormExpr(aggsElems[i]).AsConstAgg(); constAgg != nil {
			// Verify that the input and output column IDs match.
			if aggsColList[i] == c.ExtractColID(c.mem.NormExpr(constAgg.Input()).AsVariable().Col()) {
				result.Add(int(aggsColList[i]))
			}
		}
	}
	return result
}

// GroupingColsAreKey returns true if the given group by's grouping columns form
// a strict key for the output rows of the given group. A strict key means that
// any two rows will have unique key column values. Nulls are treated as equal
// to one another (i.e. no duplicate nulls allowed). Having a strict key means
// that the set of key column values uniquely determine the values of all other
// columns in the relation.
func (c *CustomFuncs) GroupingColsAreKey(def memo.PrivateID, group memo.GroupID) bool {
	colSet := c.mem.LookupPrivate(def).(*memo.GroupByDef).GroupingCols
	return c.LookupLogical(group).Relational.FuncDeps.ColsAreStrictKey(colSet)
}

// IsUnorderedGroupBy returns true if the given input ordering for the group by
// is unspecified.
func (c *CustomFuncs) IsUnorderedGroupBy(def memo.PrivateID) bool {
	return c.mem.LookupPrivate(def).(*memo.GroupByDef).Ordering.Any()
}

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

// LimitGeMaxRows returns true if the given constant limit value is greater than
// or equal to the max number of rows returned by the input group.
func (c *CustomFuncs) LimitGeMaxRows(limit memo.PrivateID, input memo.GroupID) bool {
	limitVal := int64(*c.mem.LookupPrivate(limit).(*tree.DInt))
	maxRows := c.mem.GroupProperties(input).Relational.Cardinality.Max
	return limitVal >= 0 && maxRows < math.MaxUint32 && limitVal >= int64(maxRows)
}

// ----------------------------------------------------------------------
//
// Boolean Rules
//   Custom match and replace functions used with bool.opt rules.
//
// ----------------------------------------------------------------------

// SimplifyAnd removes True operands from an And operator, and eliminates the
// And operator altogether if any operand is False. It also "flattens" any And
// operator child by merging its conditions into the top-level list. Only one
// level of flattening is necessary, since this pattern would have already
// matched any And operator children. If, after simplification, no operands
// remain, then simplifyAnd returns True.
func (c *CustomFuncs) SimplifyAnd(conditions memo.ListID) memo.GroupID {
	lb := MakeListBuilder(c)
	for _, item := range c.mem.LookupList(conditions) {
		itemExpr := c.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.AndOp:
			// Flatten nested And operands.
			lb.AddItems(c.mem.LookupList(itemExpr.AsAnd().Conditions()))

		case opt.TrueOp:
			// And operator skips True operands.

		case opt.FalseOp:
			// Entire And evaluates to False if any operand is False.
			return item

		default:
			lb.AddItem(item)
		}
	}

	if lb.Empty() {
		return c.f.ConstructTrue()
	}
	return c.f.ConstructAnd(lb.BuildList())
}

// SimplifyOr removes False operands from an Or operator, and eliminates the Or
// operator altogether if any operand is True. It also "flattens" any Or
// operator child by merging its conditions into the top-level list. Only one
// level of flattening is necessary, since this pattern would have already
// matched any Or operator children. If, after simplification, no operands
// remain, then simplifyOr returns False.
func (c *CustomFuncs) SimplifyOr(conditions memo.ListID) memo.GroupID {
	lb := MakeListBuilder(c)

	for _, item := range c.mem.LookupList(conditions) {
		itemExpr := c.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.OrOp:
			// Flatten nested Or operands.
			lb.AddItems(c.mem.LookupList(itemExpr.AsOr().Conditions()))

		case opt.FalseOp:
			// Or operator skips False operands.

		case opt.TrueOp:
			// Entire Or evaluates to True if any operand is True.
			return item

		default:
			lb.AddItem(item)
		}
	}

	if lb.Empty() {
		return c.f.ConstructFalse()
	}
	return c.f.ConstructOr(lb.BuildList())
}

// SimplifyFilters behaves the same way as simplifyAnd, with one addition: if
// the conditions include a Null value in any position, then the entire
// expression is False. This works because the Filters expression only appears
// as a Select or Join filter condition, both of which treat a Null filter
// conjunct exactly as if it were False.
func (c *CustomFuncs) SimplifyFilters(conditions memo.ListID) memo.GroupID {
	lb := MakeListBuilder(c)
	for _, item := range c.mem.LookupList(conditions) {
		itemExpr := c.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.AndOp:
			// Flatten nested And operands.
			lb.AddItems(c.mem.LookupList(itemExpr.AsAnd().Conditions()))

		case opt.TrueOp:
			// Filters operator skips True operands.

		case opt.FalseOp:
			// Filters expression evaluates to False if any operand is False.
			return item

		case opt.NullOp:
			// Filters expression evaluates to False if any operand is False.
			return c.f.ConstructFalse()

		default:
			lb.AddItem(item)
		}
	}

	if lb.Empty() {
		return c.f.ConstructTrue()
	}
	return c.f.ConstructFilters(lb.BuildList())
}

// NegateConditions negates all the conditions in a list like:
//   a = 5 AND b < 10
// to:
//   NOT(a = 5) AND NOT(b < 10)
func (c *CustomFuncs) NegateConditions(conditions memo.ListID) memo.ListID {
	lb := MakeListBuilder(c)
	list := c.mem.LookupList(conditions)
	for i := range list {
		lb.AddItem(c.f.ConstructNot(list[i]))
	}
	return lb.BuildList()
}

// NegateComparison negates a comparison op like:
//   a.x = 5
// to:
//   a.x <> 5
func (c *CustomFuncs) NegateComparison(cmp opt.Operator, left, right memo.GroupID) memo.GroupID {
	negate := opt.NegateOpMap[cmp]
	operands := memo.DynamicOperands{memo.DynamicID(left), memo.DynamicID(right)}
	return c.f.DynamicConstruct(negate, operands)
}

// CommuteInequality swaps the operands of an inequality comparison expression,
// changing the operator to compensate:
//   5 < x
// to:
//   x > 5
func (c *CustomFuncs) CommuteInequality(op opt.Operator, left, right memo.GroupID) memo.GroupID {
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

// IsRedundantSubclause checks if the given subclause appears as a conjunct in
// all of the given OR conditions. For example, if A is the given subclause:
//   A OR A                      =>  true
//   B OR A                      =>  false
//   A OR (A AND B)              =>  true
//   (A AND B) OR (A AND C)      =>  true
//   A OR (A AND B) OR (B AND C) =>  false
func (c *CustomFuncs) IsRedundantSubclause(conditions memo.ListID, subclause memo.GroupID) bool {
	for _, item := range c.mem.LookupList(conditions) {
		itemExpr := c.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.AndOp:
			found := false
			for _, item := range c.mem.LookupList(itemExpr.AsAnd().Conditions()) {
				if item == subclause {
					found = true
					break
				}
			}
			if !found {
				return false
			}

		default:
			if item != subclause {
				return false
			}
		}
	}

	return true
}

// ExtractRedundantSubclause extracts a redundant subclause from a list of OR
// conditions, and returns an AND of the subclause with the remaining OR
// expression (a logically equivalent expression). For example:
//   (A AND B) OR (A AND C)  =>  A AND (B OR C)
//
// If extracting the subclause from one of the OR conditions would result in an
// empty condition, the subclause itself is returned (a logically equivalent
// expression). For example:
//   A OR (A AND B)  =>  A
//
// These transformations are useful for finding a conjunct that can be pushed
// down in the query tree. For example, if the redundant subclause A is
// fully bound by one side of a join, it can be pushed through the join, even
// if B AND C cannot.
func (c *CustomFuncs) ExtractRedundantSubclause(
	conditions memo.ListID, subclause memo.GroupID,
) memo.GroupID {
	orList := MakeListBuilder(c)

	for _, item := range c.mem.LookupList(conditions) {
		itemExpr := c.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.AndOp:
			// Remove the subclause from the AND expression, and add the new AND
			// expression to the list of OR conditions.
			remaining := c.RemoveListItem(itemExpr.AsAnd().Conditions(), subclause)
			orList.AddItem(c.f.ConstructAnd(remaining))

		default:
			// In the default case, we have a boolean expression such as
			// `(A AND B) OR A`, which is logically equivalent to `A`.
			// (`A` is the redundant subclause).
			if item == subclause {
				return item
			}

			panic(fmt.Errorf(
				"ExtractRedundantSubclause called with non-redundant subclause:\n%s",
				memo.MakeNormExprView(c.mem, subclause).FormatString(memo.ExprFmtHideScalars),
			))
		}
	}

	or := c.f.ConstructOr(orList.BuildList())

	andList := MakeListBuilder(c)
	andList.AddItem(subclause)
	andList.AddItem(or)

	return c.f.ConstructAnd(andList.BuildList())
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
func (c *CustomFuncs) NormalizeTupleEquality(left, right memo.ListID) memo.GroupID {
	if left.Length != right.Length {
		panic("tuple length mismatch")
	}

	lb := MakeListBuilder(c)
	leftList := c.mem.LookupList(left)
	rightList := c.mem.LookupList(right)
	for i := 0; i < len(leftList); i++ {
		lb.AddItem(c.f.ConstructEq(leftList[i], rightList[i]))
	}
	return c.f.ConstructAnd(lb.BuildList())
}

// ----------------------------------------------------------------------
//
// Scalar Rules
//   Custom match and replace functions used with scalar.opt rules.
//
// ----------------------------------------------------------------------

// SimplifyCoalesce discards any leading null operands, and then if the next
// operand is a constant, replaces with that constant.
func (c *CustomFuncs) SimplifyCoalesce(args memo.ListID) memo.GroupID {
	argList := c.mem.LookupList(args)
	for i := 0; i < int(args.Length-1); i++ {
		// If item is not a constant value, then its value may turn out to be
		// null, so no more folding. Return operands from then on.
		if !c.IsConstValueOrTuple(argList[i]) {
			return c.f.ConstructCoalesce(c.f.InternList(argList[i:]))
		}

		item := c.mem.NormExpr(argList[i])
		if item.Operator() != opt.NullOp {
			return argList[i]
		}
	}

	// All operands up to the last were null (or the last is the only operand),
	// so return the last operand without the wrapping COALESCE function.
	return argList[args.Length-1]
}

// AllowNullArgs returns true if the binary operator with the given inputs
// allows one of those inputs to be null. If not, then the binary operator will
// simply be replaced by null.
func (c *CustomFuncs) AllowNullArgs(op opt.Operator, left, right memo.GroupID) bool {
	leftType := c.LookupScalar(left).Type
	rightType := c.LookupScalar(right).Type
	return memo.BinaryAllowsNullArgs(op, leftType, rightType)
}

// FoldNullUnary replaces the unary operator with a typed null value having the
// same type as the unary operator would have.
func (c *CustomFuncs) FoldNullUnary(op opt.Operator, input memo.GroupID) memo.GroupID {
	typ := c.LookupScalar(input).Type
	return c.f.ConstructNull(c.f.InternType(memo.InferUnaryType(op, typ)))
}

// FoldNullBinary replaces the binary operator with a typed null value having
// the same type as the binary operator would have.
func (c *CustomFuncs) FoldNullBinary(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	leftType := c.LookupScalar(left).Type
	rightType := c.LookupScalar(right).Type
	return c.f.ConstructNull(c.f.InternType(memo.InferBinaryType(op, leftType, rightType)))
}

// IsJSONScalar returns if the JSON value is a number, string, true, false, or null.
func (c *CustomFuncs) IsJSONScalar(value memo.GroupID) bool {
	v := c.ExtractConstValue(value).(*tree.DJSON)
	return v.JSON.Type() != json.ObjectJSONType && v.JSON.Type() != json.ArrayJSONType
}

// MakeSingleKeyJSONObject returns a JSON object with one entry, mapping key to value.
func (c *CustomFuncs) MakeSingleKeyJSONObject(key, value memo.GroupID) memo.GroupID {
	k := c.ExtractConstValue(key).(*tree.DString)
	v := c.ExtractConstValue(value).(*tree.DJSON)

	builder := json.NewObjectBuilder(1)
	builder.Add(string(*k), v.JSON)
	j := builder.Build()

	return c.f.ConstructConst(c.f.InternDatum(&tree.DJSON{JSON: j}))
}

// ExtractConstValue extracts the Datum from a constant value.
func (c *CustomFuncs) ExtractConstValue(group memo.GroupID) interface{} {
	return c.mem.LookupPrivate(c.mem.NormExpr(group).AsConst().Value())
}

// IsConstValueEqual returns whether const1 and const2 are equal.
func (c *CustomFuncs) IsConstValueEqual(const1, const2 memo.GroupID) bool {
	op1 := c.mem.NormOp(const1)
	op2 := c.mem.NormOp(const2)
	if op1 != op2 || op1 == opt.NullOp {
		return false
	}
	switch op1 {
	case opt.TrueOp, opt.FalseOp:
		return true
	case opt.ConstOp:
		datum1 := c.ExtractConstValue(const1).(tree.Datum)
		datum2 := c.ExtractConstValue(const2).(tree.Datum)
		return datum1.Compare(c.f.evalCtx, datum2) == 0
	default:
		panic(fmt.Errorf("unexpected Op type: %v", op1))
	}
}

// SimplifyWhens removes known unreachable WHEN cases and constructs a new CASE
// statement. Any known true condition is converted to the ELSE. If only the
// ELSE remains, its expression is returned. condition must be a ConstValue.
func (c *CustomFuncs) SimplifyWhens(condition memo.GroupID, whens memo.ListID) memo.GroupID {
	lb := MakeListBuilder(c)
	whenList := c.mem.LookupList(whens)
	for _, item := range whenList {
		itemExpr := c.mem.NormExpr(item)

		switch itemExpr.Operator() {
		case opt.WhenOp:
			when := itemExpr.AsWhen()
			nwc := c.mem.NormExpr(when.Condition())
			if nwc.IsConstValue() {
				if !c.IsConstValueEqual(condition, when.Condition()) {
					// Ignore known unmatching conditions.
					continue
				}
				// If this is true, we won't ever match anything else, so convert this to
				// the ELSE (or just return it if there are no earlier items).
				if lb.Empty() {
					return when.Value()
				}
				lb.AddItem(when.Value())
				return c.f.ConstructCase(condition, lb.BuildList())
			}

		// The ELSE value.
		default:
			if lb.Empty() {
				// ELSE is the only clause (there are no WHENs), remove the CASE.
				return item
			}
		}
		lb.AddItem(item)
	}
	return c.f.ConstructCase(condition, lb.BuildList())
}

// ----------------------------------------------------------------------
//
// Numeric Rules
//   Custom match and replace functions used with numeric.opt rules.
//
// ----------------------------------------------------------------------

// EqualsNumber returns true if the given private numeric value (decimal, float,
// or integer) is equal to the given integer value.
func (c *CustomFuncs) EqualsNumber(private memo.PrivateID, value int64) bool {
	d := c.mem.LookupPrivate(private).(tree.Datum)
	switch t := d.(type) {
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
func (c *CustomFuncs) IsListOfConstants(elems memo.ListID) bool {
	for _, elem := range c.mem.LookupList(elems) {
		if !c.IsConstValueOrTuple(elem) {
			return false
		}
	}
	return true
}

// FoldArray evaluates an Array expression with constant inputs. It returns the
// array as a Const datum with type TArray.
func (c *CustomFuncs) FoldArray(elems memo.ListID, typ memo.PrivateID) memo.GroupID {
	elements := c.mem.LookupList(elems)
	elementType := c.ExtractType(typ).(types.TArray).Typ
	a := tree.NewDArray(elementType)
	a.Array = make(tree.Datums, len(elements))
	for i := range a.Array {
		elem := memo.MakeNormExprView(c.mem, elements[i])
		a.Array[i] = memo.ExtractConstDatum(elem)
		if a.Array[i] == tree.DNull {
			a.HasNulls = true
		}
	}
	return c.f.ConstructConst(c.f.InternDatum(a))
}

// Succeeded returns true if the result of an operation is a valid memo group.
func (c *CustomFuncs) Succeeded(result memo.GroupID) bool {
	return result != 0
}

// IsConstValueOrTuple returns true if the input is a constant or a tuple of
// constants.
func (c *CustomFuncs) IsConstValueOrTuple(input memo.GroupID) bool {
	ev := memo.MakeNormExprView(c.mem, input)
	return ev.IsConstValue() || memo.MatchesTupleOfConstants(ev)
}

// FoldBinary evaluates a binary expression with constant inputs. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input types, and the evaluation causes no error.
func (c *CustomFuncs) FoldBinary(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	lEv, rEv := memo.MakeNormExprView(c.mem, left), memo.MakeNormExprView(c.mem, right)
	lDatum, rDatum := memo.ExtractConstDatum(lEv), memo.ExtractConstDatum(rEv)
	lType, rType := lEv.Logical().Scalar.Type, rEv.Logical().Scalar.Type

	o, ok := memo.FindBinaryOverload(op, lType, rType)
	if !ok {
		return 0
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return 0
	}
	return c.f.ConstructConstVal(result)
}

// FoldUnary evaluates a unary expression with a constant input. It returns
// a constant expression as long as it finds an appropriate overload function
// for the given operator and input type, and the evaluation causes no error.
func (c *CustomFuncs) FoldUnary(op opt.Operator, input memo.GroupID) memo.GroupID {
	ev := memo.MakeNormExprView(c.mem, input)
	datum := memo.ExtractConstDatum(ev)
	typ := ev.Logical().Scalar.Type

	o, ok := memo.FindUnaryOverload(op, typ)
	if !ok {
		return 0
	}

	result, err := o.Fn(c.f.evalCtx, datum)
	if err != nil {
		return 0
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

// UnifyComparison attempts to convert right to the type of left, if such a
// conversion can round-trip and is monotonic.
func (c *CustomFuncs) UnifyComparison(left, right memo.GroupID) memo.GroupID {
	desiredType := c.LookupScalar(left).Type
	originalType := c.LookupScalar(right).Type

	// Don't bother if they're already the same.
	if desiredType.Equivalent(originalType) {
		return 0
	}

	desiredColType, err := coltypes.DatumTypeToColumnType(desiredType)
	if err != nil {
		return 0
	}

	originalColType, err := coltypes.DatumTypeToColumnType(originalType)
	if err != nil {
		return 0
	}

	if !isMonotonicConversion(originalColType, desiredColType) {
		return 0
	}

	// Check that the datum can round-trip between the types. If this is true, it
	// means we don't lose any information needed to generate spans, and combined
	// with monotonicity means that it's safe to convert the RHS to the type of
	// the LHS.
	rightDatum := c.ExtractConstValue(right).(tree.Datum)

	convertedRightDatum, err := tree.PerformCast(c.f.evalCtx, rightDatum, desiredColType)
	if err != nil {
		return 0
	}

	convertedBack, err := tree.PerformCast(c.f.evalCtx, convertedRightDatum, originalColType)
	if err != nil {
		return 0
	}

	if convertedBack.Compare(c.f.evalCtx, rightDatum) != 0 {
		return 0
	}

	return c.f.ConstructConst(c.f.mem.InternDatum(convertedRightDatum))
}

// FoldComparison evaluates a comparison expression with constant inputs. It
// returns a constant expression as long as it finds an appropriate overload
// function for the given operator and input types, and the evaluation causes
// no error.
func (c *CustomFuncs) FoldComparison(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	lEv, rEv := memo.MakeNormExprView(c.mem, left), memo.MakeNormExprView(c.mem, right)
	lDatum, rDatum := memo.ExtractConstDatum(lEv), memo.ExtractConstDatum(rEv)
	lType, rType := lEv.Logical().Scalar.Type, rEv.Logical().Scalar.Type

	o, ok := memo.FindComparisonOverload(op, lType, rType)
	if !ok {
		return 0
	}

	result, err := o.Fn(c.f.evalCtx, lDatum, rDatum)
	if err != nil {
		return 0
	}
	return c.f.ConstructConstVal(result)
}
