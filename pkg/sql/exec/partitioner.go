// Copyright 2019 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// NewWindowSortingPartitioner creates a new exec.Operator that orders input
// based on the combination of partitionIdxs columns and ordCols (i.e. it
// handles both PARTITION BY and ORDER BY clauses of a window function) and
// puts true in partitionColIdx'th column (which is appended if needed) for
// every tuple that is the first within its partition.
func NewWindowSortingPartitioner(
	input Operator,
	inputTyps []types.T,
	partitionIdxs []uint32,
	ordCols []distsqlpb.Ordering_Column,
	partitionColIdx int,
) (op Operator, orderingColsIdxs []uint32, err error) {
	partitionAndOrderingCols := make([]distsqlpb.Ordering_Column, 0, len(partitionIdxs)+len(ordCols))
	for _, idx := range partitionIdxs {
		partitionAndOrderingCols = append(partitionAndOrderingCols, distsqlpb.Ordering_Column{ColIdx: idx})
	}
	// If there is a corresponding to an ordering column a partitioning column,
	// we'll reuse it; if there is no such, we'll add a new one for the sorter to
	// order on.
	// TODO(yuzefovich): this case is for a function like
	// rank() OVER (PARTITION BY a, b ORDER BY c, a DESC), the resulting
	// partitionAndOrderingCols will be [a DESC, b ASC, c ASC] and
	// orderingColsIdxs will be [2, 0]. This is silly since all tuples within a
	// partition will have exactly the same value of `a`, so there is no reason
	// to order on it. I think we can safely omit column `a` from the ordering
	// columns, right? Or maybe the optimizer will take care of such case?
	orderingColsIdxs = make([]uint32, len(ordCols))
	for o, ordCol := range ordCols {
		found := false
		for p := range partitionIdxs {
			if partitionAndOrderingCols[p].ColIdx == ordCol.ColIdx {
				orderingColsIdxs[o] = uint32(p)
				// Ordering direction for partitioning doesn't matter, so we use the
				// required by ORDER BY clause direction.
				partitionAndOrderingCols[p].Direction = ordCol.Direction
				found = true
				break
			}
		}
		if !found {
			orderingColsIdxs[o] = uint32(len(partitionAndOrderingCols))
			partitionAndOrderingCols = append(partitionAndOrderingCols, ordCol)
		}
	}
	input, err = NewSorter(input, inputTyps, partitionAndOrderingCols)
	if err != nil {
		return nil, nil, err
	}

	var distinctCol []bool
	input, distinctCol, err = OrderedDistinctColsToOperators(input, partitionIdxs, inputTyps)
	if err != nil {
		return nil, nil, err
	}

	return &windowSortingPartitioner{input: input, distinctCol: distinctCol, partitionColIdx: partitionColIdx}, orderingColsIdxs, nil
}

type windowSortingPartitioner struct {
	input Operator

	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new partition begins with the
	// corresponding tuple.
	distinctCol     []bool
	partitionColIdx int
}

func (p *windowSortingPartitioner) Init() {
	p.input.Init()
}

func (p *windowSortingPartitioner) Next(ctx context.Context) coldata.Batch {
	b := p.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	if p.partitionColIdx == b.Width() {
		b.AppendCol(types.Bool)
	} else if p.partitionColIdx > b.Width() {
		panic("unexpected: column partitionColIdx is neither present nor the next to be appended")
	}
	partitionVec := b.ColVec(p.partitionColIdx).Bool()
	sel := b.Selection()
	if sel != nil {
		for i := uint16(0); i < b.Length(); i++ {
			partitionVec[sel[i]] = p.distinctCol[sel[i]]
		}
	} else {
		copy(partitionVec, p.distinctCol[:b.Length()])
	}
	return b
}
