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
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// IsConstantsAndPlaceholders returns true if all scalar expressions in the list
// are constants, placeholders or tuples containing constants or placeholders.
// If a tuple nested within a tuple is found, false is returned.
func (e ScalarListExpr) IsConstantsAndPlaceholders() bool {
	return e.isConstantsAndPlaceholders(false /* insideTuple */)
}

func (e ScalarListExpr) isConstantsAndPlaceholders(insideTuple bool) bool {
	for _, scalarExpr := range e {
		if tupleExpr, ok := scalarExpr.(*TupleExpr); ok {
			if insideTuple {
				return false
			}
			if !tupleExpr.Elems.isConstantsAndPlaceholders(true) {
				return false
			}
		} else {
			if scalarExpr == nil {
				panic(errors.AssertionFailedf("nil scalar expression in list"))
			}
			if !opt.IsConstValueOp(scalarExpr) && scalarExpr.Op() != opt.PlaceholderOp {
				return false
			}
		}
	}
	return true
}

// IsConstantsAndPlaceholdersAndVariables returns true if all scalar expressions
// in the list are constants, placeholders, variables, or tuples containing
// constants, placeholders, or variables. If a tuple nested within a tuple is
// found, false is returned.
func (e ScalarListExpr) IsConstantsAndPlaceholdersAndVariables() bool {
	return e.isConstantsAndPlaceholdersAndVariables(false /* insideTuple */)
}

func (e ScalarListExpr) isConstantsAndPlaceholdersAndVariables(insideTuple bool) bool {
	for _, scalarExpr := range e {
		if tupleExpr, ok := scalarExpr.(*TupleExpr); ok {
			if insideTuple {
				return false
			}
			if !tupleExpr.Elems.isConstantsAndPlaceholdersAndVariables(true) {
				return false
			}
		} else {
			if scalarExpr == nil {
				panic(errors.AssertionFailedf("nil scalar expression in list"))
			}
			if !opt.IsConstValueOp(scalarExpr) && scalarExpr.Op() != opt.PlaceholderOp &&
				scalarExpr.Op() != opt.VariableOp {
				return false
			}
		}
	}
	return true
}

// IsSortedUniqueList returns true if the given list is composed entirely values
// that are either not in sorted order or have duplicates. It can only be used
// for a list of constant values.
func IsSortedUniqueList(ctx context.Context, evalCtx *eval.Context, list ScalarListExpr) bool {
	if len(list) <= 1 {
		return true
	}
	ls := listSorter{ctx: ctx, evalCtx: evalCtx, list: list}
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
	ctx context.Context, evalCtx *eval.Context, list ScalarListExpr,
) (ScalarListExpr, *types.T) {
	// Make a copy of the list, since it needs to stay immutable.
	newList := make(ScalarListExpr, len(list))
	copy(newList, list)
	ls := listSorter{ctx: ctx, evalCtx: evalCtx, list: newList}

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

// listSorter is a helper struct that implements the sort.Slice "less"
// comparison function.
type listSorter struct {
	ctx     context.Context
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
	cmp, err := leftD.Compare(s.ctx, s.evalCtx, rightD)
	if err != nil {
		panic(err)
	}
	return cmp
}
