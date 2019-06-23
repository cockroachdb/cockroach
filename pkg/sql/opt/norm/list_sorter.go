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

import "github.com/cockroachdb/cockroach/pkg/sql/opt/memo"

// listSorter is a helper struct that implements the sort.Slice "less"
// comparison function.
type listSorter struct {
	cf   *CustomFuncs
	list memo.ScalarListExpr
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
	leftD := memo.ExtractConstDatum(s.list[i])
	rightD := memo.ExtractConstDatum(s.list[j])
	return leftD.Compare(s.cf.f.evalCtx, rightD)
}
