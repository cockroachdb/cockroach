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

package xfunc

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// CustomFuncs contains all the custom match and replace functions that are
// used by both normalization and exploration rules.
type CustomFuncs struct {
	mem     *memo.Memo
	evalCtx *tree.EvalContext

	// scratchItems is a slice that is reused by ListBuilder to store temporary
	// results that are accumulated before passing them to memo.InternList.
	scratchItems []memo.GroupID
}

// MakeCustomFuncs returns a new CustomFuncs initialized with the given memo
// and evalContext.
func MakeCustomFuncs(mem *memo.Memo, evalCtx *tree.EvalContext) CustomFuncs {
	return CustomFuncs{
		mem:     mem,
		evalCtx: evalCtx,
	}
}

// -----------------------------------------------------------------------
//
// List functions
//   General custom match and replace functions used to test and construct
//   lists.
//
// -----------------------------------------------------------------------

// IsSortedUniqueList returns true if the list is in sorted order, with no
// duplicates. See the comment for listSorter.compare for comparison rule
// details.
func (c *CustomFuncs) IsSortedUniqueList(list memo.ListID) bool {
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

// -----------------------------------------------------------------------
//
// Filter functions
//   General custom match and replace functions used to test and construct
//   filters in Select and Join rules.
//
// -----------------------------------------------------------------------

// IsBoundBy returns true if all outer references in the source expression are
// bound by the destination expression. For example:
//
//   (InnerJoin
//     (Scan a)
//     (Scan b)
//     (Eq (Variable a.x) (Const 1))
//   )
//
// The (Eq) expression is fully bound by the (Scan a) expression because all of
// its outer references are satisfied by the columns produced by the Scan.
func (c *CustomFuncs) IsBoundBy(src, dst memo.GroupID) bool {
	return c.OuterCols(src).SubsetOf(c.OutputCols(dst))
}

// ExtractBoundConditions returns a new list containing only those expressions
// from the given list that are fully bound by the given expression (i.e. all
// outer references are satisfied by it). For example:
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
// Calling extractBoundConditions with the filter conditions list and the output
// columns of (Scan a) would extract the (Gt) expression, since its outer
// references only reference columns from a.
func (c *CustomFuncs) ExtractBoundConditions(list memo.ListID, group memo.GroupID) memo.ListID {
	lb := MakeListBuilder(c)
	for _, item := range c.mem.LookupList(list) {
		if c.IsBoundBy(item, group) {
			lb.AddItem(item)
		}
	}
	return lb.BuildList()
}

// ExtractUnboundConditions is the inverse of extractBoundConditions. Instead of
// extracting expressions that are bound by the given expression, it extracts
// list expressions that have at least one outer reference that is *not* bound
// by the given expression (i.e. it has a "free" variable).
func (c *CustomFuncs) ExtractUnboundConditions(list memo.ListID, group memo.GroupID) memo.ListID {
	lb := MakeListBuilder(c)
	for _, item := range c.mem.LookupList(list) {
		if !c.IsBoundBy(item, group) {
			lb.AddItem(item)
		}
	}
	return lb.BuildList()
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
// List functions
//   Helper functions for manipulating lists.
//
// ----------------------------------------------------------------------

// InternSingletonList interns a list containing the single given item and
// returns its id.
func (c *CustomFuncs) InternSingletonList(item memo.GroupID) memo.ListID {
	b := MakeListBuilder(c)
	b.AddItem(item)
	return b.BuildList()
}

// ----------------------------------------------------------------------
//
// Property functions
//   Helper functions used to test expression logical properties.
//
// ----------------------------------------------------------------------

// Operator returns the type of the given group's normalized expression.
func (c *CustomFuncs) Operator(group memo.GroupID) opt.Operator {
	return c.mem.NormExpr(group).Operator()
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
		return opt.ColListToSet(c.ExtractColList(expr.AsAggregations().Cols()))

	case opt.ProjectionsOp:
		return c.ExtractProjectionsOpDef(expr.AsProjections().Def()).AllCols()

	default:
		panic(fmt.Sprintf("OutputCols doesn't support op %s", expr.Operator()))
	}
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

// IsColNotNull returns true if the given column has the NotNull property.
func (c *CustomFuncs) IsColNotNull(col memo.PrivateID, input memo.GroupID) bool {
	return c.LookupLogical(input).Relational.NotNullCols.Contains(int(c.ExtractColID(col)))
}
