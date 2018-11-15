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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type sortDirection int

const (
	ascending = iota
	descending
)

type sortSpec struct {
	// todo(arjun): add multi-col sort functionality.
	sortCol    int
	direction  sortDirection
	inputTypes []types.T
}

type sorter struct {
	spec sortSpec

	input Operator
	size  int
	// values stores all the values from the input
	values []ColVec
	order  []int

	// emitted is the index of the last value that was emitted by Next()
	emitted int
	output  ColBatch
}

func NewSortOp(input Operator, spec sortSpec) Operator {
	s := &sorter{
		input: input,
		spec:  spec,
	}
	return s
}

func (s *sorter) Len() int {
	return len(s.order)
}

func (s *sorter) Init() {
	s.input.Init()

	batch := s.input.Next()

	s.values = make([]ColVec, len(batch.ColVecs()))

	for i := 0; i < len(batch.ColVecs()); i++ {
		s.values[i] = newMemColumn(s.spec.inputTypes[i], 0)
	}

	for ; batch.Length() != 0; batch = s.input.Next() {
		// First, copy all vecs into values.
		for i := 0; i < len(s.values); i++ {
			if batch.Selection() == nil {
				s.values[i].Append(batch.ColVec(i),
					s.spec.inputTypes[i],
					uint64(s.values[i].Len(s.spec.inputTypes[0])),
					batch.Length(),
				)
			} else {
				s.values[i].AppendWithSel(batch.ColVec(i),
					batch.Selection(),
					batch.Length(),
					s.spec.inputTypes[i],
					uint64(s.values[i].Len(s.spec.inputTypes[0])),
				)
			}
			s.size += int(batch.Length())
		}
	}

	s.order = make([]int, s.values[0].Len(s.spec.inputTypes[0]))
	for i := 0; i < len(s.order); i++ {
		s.order[i] = i
	}

	sort.Sort(s)

	s.output = NewMemBatch(s.spec.inputTypes)
}

func (s *sorter) Next() ColBatch {
	limit := s.emitted + ColBatchSize
	if limit > s.size {
		limit = s.size
	}
	s.output.SetLength(uint16(limit - s.emitted))
	if s.output.Length() == 0 {
		return s.output
	}

	for j := 0; j < len(s.values); j++ {
		if j == s.spec.sortCol {
			// the sortCol'th vec is already sorted, so just fill it directly.
			s.output.ColVec(j).Copy(ColVec(s.values[j]), s.emitted, limit, s.spec.inputTypes[j])
		} else {
			s.fill(s.output.ColVec(j), s.values[j], s.order, s.emitted, limit)
		}
	}
	s.emitted = limit
	return s.output
}
