// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewWindowSortingPartitioner creates a new colexecop.Operator that orders
// input first based on the partitionIdxs columns and second on ordCols (i.e. it
// handles both PARTITION BY and ORDER BY clauses of a window function) and puts
// true in partitionColIdx'th column (which is appended if needed) for every
// tuple that is the first within its partition.
func NewWindowSortingPartitioner(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	inputTyps []*types.T,
	partitionIdxs []uint32,
	ordCols []execinfrapb.Ordering_Column,
	partitionColIdx int,
	createDiskBackedSorter func(input colexecop.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column) (colexecop.Operator, error),
) (op colexecop.Operator, err error) {
	partitionAndOrderingCols := make([]execinfrapb.Ordering_Column, len(partitionIdxs)+len(ordCols))
	for i, idx := range partitionIdxs {
		partitionAndOrderingCols[i] = execinfrapb.Ordering_Column{ColIdx: idx}
	}
	copy(partitionAndOrderingCols[len(partitionIdxs):], ordCols)
	input, err = createDiskBackedSorter(input, inputTyps, partitionAndOrderingCols)
	if err != nil {
		return nil, err
	}

	var distinctCol []bool
	input, distinctCol, err = colexecbase.OrderedDistinctColsToOperators(input, partitionIdxs, inputTyps, false /* nullsAreDistinct */)
	if err != nil {
		return nil, err
	}

	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, partitionColIdx)
	return &windowSortingPartitioner{
		OneInputHelper:  colexecop.MakeOneInputHelper(input),
		allocator:       allocator,
		distinctCol:     distinctCol,
		partitionColIdx: partitionColIdx,
	}, nil
}

type windowSortingPartitioner struct {
	colexecop.OneInputHelper

	allocator *colmem.Allocator
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new partition begins with the
	// corresponding tuple.
	distinctCol     []bool
	partitionColIdx int
}

func (p *windowSortingPartitioner) Next() coldata.Batch {
	b := p.Input.Next()
	if b.Length() == 0 {
		return coldata.ZeroBatch
	}
	partitionVec := b.ColVec(p.partitionColIdx)
	if partitionVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		partitionVec.Nulls().UnsetNulls()
	}
	partitionCol := partitionVec.Bool()
	sel := b.Selection()
	if sel != nil {
		for i := 0; i < b.Length(); i++ {
			partitionCol[sel[i]] = p.distinctCol[sel[i]]
		}
	} else {
		copy(partitionCol, p.distinctCol[:b.Length()])
	}
	return b
}
