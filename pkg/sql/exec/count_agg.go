// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package exec

import "github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"

func newCountAgg() *countAgg {
	return &countAgg{}
}

// countAgg is the COUNT(*) aggregate - it takes no arguments.
type countAgg struct {
	groups []bool
	vec    []int64
	curIdx int
	done   bool
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

func (a *countAgg) Compute(b coldata.Batch, _ []uint32) {
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
