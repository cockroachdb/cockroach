// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// listSorter is a helper struct that implements the sort.Slice "less"
// comparison function.
type listSorter struct {
	evalCtx *eval.Context
	list    ScalarListExpr
}

// less returns true if item i in the list compares less than item j.
// sort.Slice uses this method to sort the list.
func (s listSorter) less(i, j int) bool {
	return s.compare(i, j) < 0
}

// compare returns -1 if item i compares less than item j, 0 if they are equal,
// and 1 if item i compares greater. Constants are sorted according to Datum
// comparison rules.
func (s listSorter) compare(i, j int) int {
	leftD := ExtractConstDatum(s.list[i])
	rightD := ExtractConstDatum(s.list[j])
	return leftD.Compare(s.evalCtx, rightD)
}

// IsSortedUniqueList returns true if the given list is composed entirely values
// that are either not in sorted order or have duplicates. It can only be used
// for a list of constant values.
func IsSortedUniqueList(evalCtx *eval.Context, list ScalarListExpr) bool {
	if len(list) <= 1 {
		return true
	}
	ls := listSorter{evalCtx: evalCtx, list: list}
	for i := range list {
		if i != 0 && !ls.less(i-1, i) {
			return false
		}
	}
	return true
}

// ConstructSortedUniqueList sorts the given list and removes duplicates, and
// returns the resulting list. See the comment for listSorter.compare for
// comparison rule details. The list must contain only constant expressions.
func ConstructSortedUniqueList(
	evalCtx *eval.Context, list ScalarListExpr,
) (ScalarListExpr, *types.T) {
	// Make a copy of the list, since it needs to stay immutable.
	newList := make(ScalarListExpr, len(list))
	copy(newList, list)
	ls := listSorter{evalCtx: evalCtx, list: newList}

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
