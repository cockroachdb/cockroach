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

func (a *countAgg) Init(groups []bool, vec ColVec) {
	a.groups = groups
	a.vec = vec.Int64()
	a.Reset()
}

func (a *countAgg) Reset() {
	a.curIdx = -1
	copy(a.vec, zeroInt64Batch)
}

func (a *countAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *countAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		copy(a.vec[idx+1:], zeroInt64Batch)
	}
}

func (a *countAgg) Compute(b ColBatch, _ []uint32) {
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
		for i := uint16(0); i < inputLen; i++ {
			x := 0
			if a.groups[i] {
				x = 1
			}
			a.curIdx += x
			a.vec[a.curIdx]++
		}
	}
}
