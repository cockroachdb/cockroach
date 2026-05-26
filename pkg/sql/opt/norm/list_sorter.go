// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	cmp, err := leftD.Compare(s.cf.f.ctx, s.cf.f.evalCtx, rightD)
	if err != nil {
		panic(err)
	}
	return cmp
}
