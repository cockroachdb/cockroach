// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

// NewExternalDistinct returns a new disk-backed unordered distinct operator. It
// uses the in-memory unordered distinct as the "main" strategy for the external
// operator and the external sort + ordered distinct as the "fallback".
func NewExternalDistinct(
	unlimitedAllocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	args *NewColOperatorArgs,
	input colexecbase.Operator,
	inputTypes []*types.T,
	createDiskBackedSorter DiskBackedSorterConstructor,
	inMemUnorderedDistinct colexecbase.Operator,
	diskAcc *mon.BoundAccount,
) colexecbase.Operator {
	distinctSpec := args.Spec.Core.Distinct
	distinctCols := distinctSpec.DistinctColumns
	inMemMainOpConstructor := func(partitionedInputs []*partitionerToOperator) ResettableOperator {
		// Note that the hash-based partitioner will make sure that partitions
		// to process using the in-memory unordered distinct fit under the
		// limit, so we use an unlimited allocator.
		// TODO(yuzefovich): it might be worth increasing the number of buckets.
		return NewUnorderedDistinct(
			unlimitedAllocator, partitionedInputs[0], distinctCols, inputTypes,
		)
	}
	diskBackedFallbackOpConstructor := func(
		partitionedInputs []*partitionerToOperator,
		maxNumberActivePartitions int,
		_ semaphore.Semaphore,
	) ResettableOperator {
		// The distinct operator *must* keep the first tuple from the input
		// among all that are identical on distinctCols. In order to guarantee
		// such behavior in the fallback, we append an ordinality column to
		// every tuple before the disk-backed sorter and include that column in
		// the desired ordering. We then project out that temporary column
		// before feeding the tuples into the ordered distinct.
		ordinalityOp := NewOrdinalityOp(unlimitedAllocator, partitionedInputs[0], len(inputTypes))
		orderingCols := make([]execinfrapb.Ordering_Column, len(distinctCols)+1)
		for i := range distinctCols {
			orderingCols[i].ColIdx = distinctCols[i]
		}
		orderingCols[len(distinctCols)].ColIdx = uint32(len(inputTypes))
		sortTypes := make([]*types.T, 0, len(inputTypes)+1)
		sortTypes = append(sortTypes, inputTypes...)
		sortTypes = append(sortTypes, types.Int)
		diskBackedSorter := createDiskBackedSorter(ordinalityOp, sortTypes, orderingCols, maxNumberActivePartitions)
		projection := make([]uint32, len(inputTypes))
		for i := range projection {
			projection[i] = uint32(i)
		}
		diskBackedWithoutOrdinality := NewSimpleProjectOp(diskBackedSorter, len(sortTypes), projection)
		diskBackedFallbackOp, err := NewOrderedDistinct(diskBackedWithoutOrdinality, distinctCols, inputTypes)
		if err != nil {
			colexecerror.InternalError(err)
		}
		return diskBackedFallbackOp
	}
	// We have to be careful to not emit duplicates of already emitted by the
	// in-memory operator tuples, so we plan a special filterer operator to
	// remove all such tuples.
	input = &unorderedDistinctFilterer{
		OneInputNode: NewOneInputNode(input),
		ht:           inMemUnorderedDistinct.(*unorderedDistinct).ht,
	}
	numRequiredActivePartitions := ExternalSorterMinPartitions
	ed := newHashBasedPartitioner(
		unlimitedAllocator,
		flowCtx,
		args,
		"external unordered distinct", /* name */
		[]colexecbase.Operator{input},
		[][]*types.T{inputTypes},
		[][]uint32{distinctCols},
		inMemMainOpConstructor,
		diskBackedFallbackOpConstructor,
		diskAcc,
		numRequiredActivePartitions,
	)
	// The last thing we need to do is making sure that the output has the
	// desired ordering if any is required. Note that since the input is assumed
	// to be already ordered according to the desired ordering, for the
	// in-memory unordered distinct we get it for "free" since it doesn't change
	// the ordering of tuples. However, that is not that the case with the
	// hash-based partitioner, so we might need to plan an external sort on top
	// of it.
	outputOrdering := args.Spec.Core.Distinct.OutputOrdering
	if len(outputOrdering.Columns) == 0 {
		// No particular output ordering is required.
		return ed
	}
	maxNumberActivePartitions := calculateMaxNumberActivePartitions(flowCtx, args, numRequiredActivePartitions)
	return createDiskBackedSorter(ed, inputTypes, outputOrdering.Columns, maxNumberActivePartitions)
}
