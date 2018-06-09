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

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// GetInterestingOrderings calculates and returns the
// props.Logical.Rule.InterestingOrderings property of a relational operator.
func GetInterestingOrderings(ev memo.ExprView) []props.Ordering {
	l := ev.Logical().Relational
	if l.Rule.InterestingOrderings != nil {
		return l.Rule.InterestingOrderings
	}

	var res []props.Ordering
	switch ev.Operator() {
	case opt.ScanOp:
		res = interestingOrderingsForScan(ev)

	case opt.SelectOp, opt.LookupJoinOp:
		// Pass through child orderings.
		res = GetInterestingOrderings(ev.Child(0))

	case opt.ProjectOp:
		res = interestingOrderingsForProject(ev)

	case opt.GroupByOp:
		res = interestingOrderingsForGroupBy(ev)

	case opt.LimitOp, opt.OffsetOp:
		res = interestingOrderingsForLimit(ev)

	default:
		if ev.IsJoin() {
			res = interestingOrderingsForJoin(ev)
			break
		}

		res = []props.Ordering{}
	}

	l.Rule.InterestingOrderings = res
	return res
}

func interestingOrderingsForScan(ev memo.ExprView) []props.Ordering {
	def := ev.Private().(*memo.ScanOpDef)
	md := ev.Metadata()
	tab := md.Table(def.Table)
	ord := make([]props.Ordering, 0, tab.IndexCount())
	for i := 0; i < tab.IndexCount(); i++ {
		index := tab.Index(i)
		numIndexCols := index.UniqueColumnCount()
		o := make(props.Ordering, 0, numIndexCols)
		for j := 0; j < numIndexCols; j++ {
			indexCol := index.Column(j)
			colID := md.TableColumn(def.Table, indexCol.Ordinal)
			if !def.Cols.Contains(int(colID)) {
				break
			}
			o = append(o, opt.MakeOrderingColumn(colID, indexCol.Descending))
		}
		ord = addInterestingOrdering(ord, o)
	}
	return ord
}

func interestingOrderingsForProject(ev memo.ExprView) []props.Ordering {
	inOrd := GetInterestingOrderings(ev.Child(0))
	passthroughCols := ev.Child(1).Private().(*memo.ProjectionsOpDef).PassthroughCols
	return restrictToCols(inOrd, passthroughCols)
}

func interestingOrderingsForGroupBy(ev memo.ExprView) []props.Ordering {
	def := ev.Private().(*memo.GroupByDef)

	inOrd := GetInterestingOrderings(ev.Child(0))
	if def.Ordering != nil {
		inOrd = restrictToPrefix(inOrd, def.Ordering)
	}

	// We can only keep orderings on grouping columns.
	return restrictToCols(inOrd, def.GroupingCols)
}

func interestingOrderingsForLimit(ev memo.ExprView) []props.Ordering {
	inOrd := GetInterestingOrderings(ev.Child(0))
	ord := ev.Private().(props.Ordering)
	if ord == nil {
		return inOrd
	}
	return restrictToPrefix(inOrd, ord)
}

func interestingOrderingsForJoin(ev memo.ExprView) []props.Ordering {
	// For a join, we could conceivably preserve the order of one side (even with
	// hash-join, depending on which side we store).
	ordLeft := GetInterestingOrderings(ev.Child(0))
	ordRight := GetInterestingOrderings(ev.Child(1))
	ord := make([]props.Ordering, 0, len(ordLeft)+len(ordRight))
	ord = append(ord, ordLeft...)
	ord = append(ord, ordRight...)
	return ord
}

// addInterestingOrdering adds an ordering to the list, checking whether it is a
// prefix of another ordering (or vice-versa). Can mutates the input slice.
func addInterestingOrdering(in []props.Ordering, o props.Ordering) []props.Ordering {
	for i := range in {
		j := 0
		for ; j < len(o) && j < len(in[i]); j++ {
			if in[i][j] != o[j] {
				break
			}
		}
		if j == len(o) {
			// o is equal to, or a prefix of res[i]. Do nothing.
			return in
		}
		if j == len(in[i]) {
			// in[i] is a prefix of o; replace it.
			in[i] = o
			return in
		}
	}
	return append(in, o)
}

// restrictToPrefix keeps only the interesting orderings that have the required
// ordering as a prefix. If there isn't any, returns just the required ordering.
func restrictToPrefix(in []props.Ordering, required props.Ordering) []props.Ordering {
	res := make([]props.Ordering, 0, len(in))
	for i := range in {
		if in[i].Provides(required) {
			res = append(res, in[i])
		}
	}
	if len(res) == 0 {
		res = append(res, required)
	}
	return res
}

// restrictToCols keeps only the orderings (or prefixes of them) that refer to
// columns in the given set.
func restrictToCols(inOrd []props.Ordering, cols opt.ColSet) []props.Ordering {
	res := make([]props.Ordering, 0, len(inOrd))
	for _, o := range inOrd {
		// Find the longest prefix of the ordering that contains
		// only passthrough columns.
		prefix := 0
		for _, c := range o {
			if !cols.Contains(int(c.ID())) {
				break
			}
			prefix++
		}
		if prefix > 0 {
			o = o[:prefix]
			res = addInterestingOrdering(res, o[:prefix])
		}
	}
	return res
}
