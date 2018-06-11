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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
func (c *CustomFuncs) ConstructSortedUniqueList(list memo.ListID) memo.ListID {
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
	return lb.BuildList()
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
	return c.mem.GroupProperties(src).OuterCols().SubsetOf(
		c.mem.GroupProperties(dst).Relational.OutputCols,
	)
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
