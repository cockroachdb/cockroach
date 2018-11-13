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

package exec

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type avgDecimalAgg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// groupSums[i] keeps track of the sum of elements belonging to the ith
		// group.
		groupSums []apd.Decimal
		// groupCounts[i] keeps track of the number of elements that we've seen
		// belonging to the ith group.
		groupCounts []int64
		// vec points to the output vector.
		vec []apd.Decimal
	}
}

var _ aggregateFunc = &avgDecimalAgg{}

func (a *avgDecimalAgg) Init(groups []bool, v ColVec) {
	a.groups = groups
	a.scratch.vec = v.Decimal()
	a.scratch.groupSums = make([]apd.Decimal, len(a.scratch.vec))
	a.scratch.groupCounts = make([]int64, len(a.scratch.vec))
	a.Reset()
}

func (a *avgDecimalAgg) Reset() {
	copy(a.scratch.groupSums, zeroDecimalBatch)
	copy(a.scratch.groupCounts, zeroInt64Batch)
	copy(a.scratch.vec, zeroDecimalBatch)
	a.scratch.curIdx = -1
}

func (a *avgDecimalAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *avgDecimalAgg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		copy(a.scratch.groupSums[idx+1:], zeroDecimalBatch)
		copy(a.scratch.groupCounts[idx+1:], zeroInt64Batch)
		// TODO(asubiotto): We might not have to zero a.scratch.vec since we
		// overwrite with an independent value.
		copy(a.scratch.vec[idx+1:], zeroDecimalBatch)
	}
}

func (a *avgDecimalAgg) Compute(b ColBatch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value.
		if a.scratch.curIdx >= 0 {
			// TODO(asubiotto): Wonder how best to template this part (and below).
			// We'd like to do something similar to AssignFunc, where we have a
			// separate method call on DDecimal per type.
			a.scratch.vec[a.scratch.curIdx].SetInt64(a.scratch.groupCounts[a.scratch.curIdx])
			if _, err := tree.DecimalCtx.Quo(&a.scratch.vec[a.scratch.curIdx], &a.scratch.groupSums[a.scratch.curIdx], &a.scratch.vec[a.scratch.curIdx]); err != nil {
				panic(err)
			}
		}
		a.scratch.curIdx++
		a.done = true
		return
	}
	col, sel := b.ColVec(int(inputIdxs[0])).Decimal(), b.Selection()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			if _, err := tree.DecimalCtx.Add(&a.scratch.groupSums[a.scratch.curIdx], &a.scratch.groupSums[a.scratch.curIdx], &col[i]); err != nil {
				panic(err)
			}
			a.scratch.groupCounts[a.scratch.curIdx]++
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			if _, err := tree.DecimalCtx.Add(&a.scratch.groupSums[a.scratch.curIdx], &a.scratch.groupSums[a.scratch.curIdx], &col[i]); err != nil {
				panic(err)
			}
			a.scratch.groupCounts[a.scratch.curIdx]++
		}
	}

	for i := 0; i < a.scratch.curIdx; i++ {
		a.scratch.vec[i].SetInt64(a.scratch.groupCounts[i])
		if _, err := tree.DecimalCtx.Quo(&a.scratch.vec[i], &a.scratch.groupSums[i], &a.scratch.vec[i]); err != nil {
			panic(err)
		}
	}
}
