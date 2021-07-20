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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/marusama/semaphore"
)

const (
	// We need at least two buckets per side to make progress. However, the
	// minimum number of partitions necessary are the partitions in use during a
	// fallback to sort and merge join. We'll be using the minimum necessary per
	// input + 2 (1 for each spilling queue that the merge joiner uses). For
	// clarity this is what happens:
	// - The 2 partitions that need to be sorted + merged will use an FD each: 2
	//   FDs. Meanwhile, each sorter will use up to ExternalSorterMinPartitions to
	//   sort and partition this input. At this stage 2 + 2 *
	//   ExternalSorterMinPartitions FDs are used.
	// - Once the inputs (the hash joiner partitions) are finished, both FDs will
	//   be released. The merge joiner will now be in use, which uses two
	//   spillingQueues with 1 FD each for a total of 2. Since each sorter will
	//   use ExternalSorterMinPartitions, the FDs used at this stage are 2 +
	//   (2 * ExternalSorterMinPartitions) as well. Note that as soon as the
	//   sorter emits its first batch, it must be the case that the input to it
	//   has returned a zero batch, and thus the FD has been closed.
	sortMergeNonSortMinFDsOpen = 2
	externalHJMinPartitions    = sortMergeNonSortMinFDsOpen + (colexecop.ExternalSorterMinPartitions * 2)
)

// externalHashJoiner is an operator that performs Grace hash join algorithm
// and can spill to disk. The high level view is that it partitions the left
// and right side into large buckets by a hash function A, writes those buckets
// to disk, then iterates through pairs of those buckets and does a normal hash
// join with a different hash function B.
//
// In order to get different hash functions, we're using the same family of
// hash functions that in-memory hash joiner uses, but we will seed it with a
// different initial hash value.
//
// The operator works in two phases.
//
// Phase 1: partitioning
// In this phase, we iterate through both sides of the join, hashing every row
// using a hash function A that produces n partitions. This will produce n
// partitions for each side of the join, which will be persisted to disk
// separately. As memory fills up, each of these partitions is flushed to disk
// repeatedly until the inputs are exhausted.
//
// Phase 2: join
// Now, we retrieve pairs of partitions from disk and join each pair using the
// ordinary hash join algorithm (and a different hash function B). Since we're
// performing an equality join, we can guarantee that each row on the left side
// of the join, if it has a match, will be in the same partition on the right
// side of the join. So, it's safe to do the join in pieces, partition by
// partition.
//
// If one of the partitions itself runs out of memory, we can recursively apply
// this algorithm. The partition will be divided into sub-partitions by a new
// hash function, spilled to disk, and so on. If repartitioning doesn't reduce
// size of the partitions sufficiently, then such partitions will be handled
// using the combination of disk-backed sort and merge join operators.

// NewExternalHashJoiner returns a disk-backed hash joiner.
// - unlimitedAllocator must have been created with a memory account derived
// from an unlimited memory monitor. It will be used by several internal
// components of the external hash joiner which is responsible for making sure
// that the components stay within the memory limit.
func NewExternalHashJoiner(
	unlimitedAllocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	spec colexecjoin.HashJoinerSpec,
	leftInput, rightInput colexecop.Operator,
	createDiskBackedSorter DiskBackedSorterConstructor,
	diskAcc *mon.BoundAccount,
) colexecop.Operator {
	// This memory limit will restrict the size of the batches output by the
	// in-memory hash joiner in the main strategy as well as by the merge joiner
	// in the fallback strategy.
	memoryLimit := execinfra.GetWorkMemLimit(flowCtx)
	if memoryLimit == 1 {
		// If memory limit is 1, we're likely in a "force disk spill"
		// scenario, but we don't want to artificially limit batches when we
		// have already spilled, so we'll use a larger limit.
		memoryLimit = execinfra.DefaultMemoryLimit
	}
	inMemMainOpConstructor := func(partitionedInputs []*partitionerToOperator) colexecop.ResettableOperator {
		// Note that the hash-based partitioner will make sure that partitions
		// to join using in-memory hash joiner fit under the limit, so we use
		// the same unlimited allocator for both buildSideAllocator and
		// outputUnlimitedAllocator arguments.
		return colexecjoin.NewHashJoiner(
			unlimitedAllocator, unlimitedAllocator, spec, partitionedInputs[0], partitionedInputs[1],
			// We start with relatively large initial number of buckets since we
			// expect each partition to be of significant size.
			uint64(coldata.BatchSize()), memoryLimit,
		)
	}
	diskBackedFallbackOpConstructor := func(
		partitionedInputs []*partitionerToOperator,
		maxNumberActivePartitions int,
		fdSemaphore semaphore.Semaphore,
	) colexecop.ResettableOperator {
		// We need to allocate 2 FDs for reading the partitions (reused by the merge
		// joiner) that we need to join using sort + merge join strategy, and all
		// others are divided between the two inputs.
		externalSorterMaxNumberPartitions := (maxNumberActivePartitions - sortMergeNonSortMinFDsOpen) / 2
		leftOrdering := makeOrdering(spec.Left.EqCols)
		leftPartitionSorter := createDiskBackedSorter(
			partitionedInputs[0], spec.Left.SourceTypes, leftOrdering, externalSorterMaxNumberPartitions,
		)
		rightOrdering := makeOrdering(spec.Right.EqCols)
		rightPartitionSorter := createDiskBackedSorter(
			partitionedInputs[1], spec.Right.SourceTypes, rightOrdering, externalSorterMaxNumberPartitions,
		)
		diskBackedSortMerge, err := colexecjoin.NewMergeJoinOp(
			unlimitedAllocator, memoryLimit, args.DiskQueueCfg, fdSemaphore, spec.JoinType,
			leftPartitionSorter, rightPartitionSorter, spec.Left.SourceTypes,
			spec.Right.SourceTypes, leftOrdering, rightOrdering, diskAcc,
		)
		if err != nil {
			colexecerror.InternalError(err)
		}
		return diskBackedSortMerge
	}
	return newHashBasedPartitioner(
		unlimitedAllocator,
		flowCtx,
		args,
		"external hash joiner", /* name */
		[]colexecop.Operator{leftInput, rightInput},
		[][]*types.T{spec.Left.SourceTypes, spec.Right.SourceTypes},
		[][]uint32{spec.Left.EqCols, spec.Right.EqCols},
		inMemMainOpConstructor,
		diskBackedFallbackOpConstructor,
		diskAcc,
		externalHJMinPartitions,
	)
}
