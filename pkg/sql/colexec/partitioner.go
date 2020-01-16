// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// NewWindowSortingPartitioner creates a new exec.Operator that orders input
// first based on the partitionIdxs columns and second on ordCols (i.e. it
// handles both PARTITION BY and ORDER BY clauses of a window function) and
// puts true in partitionColIdx'th column (which is appended if needed) for
// every tuple that is the first within its partition.
func NewWindowSortingPartitioner(
	allocator *Allocator,
	input Operator,
	inputTyps []coltypes.T,
	partitionIdxs []uint32,
	ordCols []execinfrapb.Ordering_Column,
	partitionColIdx int,
) (op Operator, err error) {
	partitionAndOrderingCols := make([]execinfrapb.Ordering_Column, len(partitionIdxs)+len(ordCols))
	for i, idx := range partitionIdxs {
		partitionAndOrderingCols[i] = execinfrapb.Ordering_Column{ColIdx: idx}
	}
	copy(partitionAndOrderingCols[len(partitionIdxs):], ordCols)
	input, err = NewSorter(allocator, input, inputTyps, partitionAndOrderingCols)
	if err != nil {
		return nil, err
	}

	var distinctCol []bool
	input, distinctCol, err = OrderedDistinctColsToOperators(input, partitionIdxs, inputTyps)
	if err != nil {
		return nil, err
	}

	return &windowSortingPartitioner{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		distinctCol:     distinctCol,
		partitionColIdx: partitionColIdx,
	}, nil
}

type windowSortingPartitioner struct {
	OneInputNode

	allocator *Allocator
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
		return coldata.ZeroBatch
	}
	p.allocator.MaybeAddColumn(b, coltypes.Bool, p.partitionColIdx)
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
