// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import "github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"

// The COUNT(*) aggregate, which counts every row in the result unconditionally.
func newCountRowAgg() *countAgg {
	return &countAgg{countRow: true}
}

// The COUNT(col) aggregate, which counts every row in the result where the
// value of col is not null.
func newCountAgg() *countAgg {
	return &countAgg{countRow: false}
}

// countAgg supports both the COUNT(*) and COUNT(col) aggregates
type countAgg struct {
	groups   []bool
	vec      []int64
	curIdx   int
	done     bool
	countRow bool
}

func (a *countAgg) Init(groups []bool, vec coldata.Vec) {
	a.groups = groups
	a.vec = vec.Int64()
	a.Reset()
}

func (a *countAgg) Reset() {
	a.curIdx = -1
	a.done = false
	copy(a.vec, zeroInt64Column)
}

func (a *countAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *countAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		copy(a.vec[idx+1:], zeroInt64Column)
	}
}

func (a *countAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		a.curIdx++
		a.done = true
		return
	}

	sel := b.Selection()
	checkForNulls := false
	nulls := coldata.NewNulls(0)
	if !a.countRow {
		// If there are nulls in this batch and this is not a COUNT_ROW aggregator
		// we must check each value for nullity.
		nulls = *b.ColVec(int(inputIdxs[0])).Nulls()
		checkForNulls = nulls.HasNulls()
	}

	if checkForNulls {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				x := 0
				if a.groups[i] {
					x = 1
				}
				a.curIdx += x
				if !nulls.NullAt(i) {
					a.vec[a.curIdx]++
				}
			}
		} else {
			for i := range a.groups[:inputLen] {
				x := 0
				if a.groups[i] {
					x = 1
				}
				a.curIdx += x
				if !nulls.NullAt(uint16(i)) {
					a.vec[a.curIdx]++
				}
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				x := 0
				if a.groups[i] {
					x = 1
				}
				a.curIdx += x
				a.vec[a.curIdx]++
			}
		} else {
			for i := range a.groups[:inputLen] {
				x := 0
				if a.groups[i] {
					x = 1
				}
				a.curIdx += x
				a.vec[a.curIdx]++
			}
		}
	}
}
