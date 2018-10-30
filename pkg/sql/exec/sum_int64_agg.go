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

type sumInt64Agg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// vec points to the output vector we are updating.
		vec []int64
	}
}

var _ aggregateFunc = &sumInt64Agg{}

var zeroInt64Batch = make([]int64, ColBatchSize)

func (a *sumInt64Agg) Init(groups []bool, v ColVec) {
	a.groups = groups
	a.scratch.vec = v.Int64()
	a.Reset()
}

func (a *sumInt64Agg) Reset() {
	copy(a.scratch.vec, zeroInt64Batch)
	a.scratch.curIdx = -1
}

func (a *sumInt64Agg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *sumInt64Agg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		copy(a.scratch.vec[idx+1:], zeroInt64Batch)
	}
}

func (a *sumInt64Agg) Compute(b ColBatch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value.
		a.scratch.curIdx++
		a.done = true
		return
	}
	ints, sel := b.ColVec(int(inputIdxs[0])).Int64(), b.Selection()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			a.scratch.vec[a.scratch.curIdx] += ints[i]
		}
	} else {
		ints = ints[:inputLen]
		for i := range ints {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.scratch.curIdx += x
			a.scratch.vec[a.scratch.curIdx] += ints[i]
		}
	}
}
