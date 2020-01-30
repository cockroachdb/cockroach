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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

// externalHashJoinerState indicates the current state of the external hash
// joiner.
type externalHashJoinerState int

const (
	// externalHJPartitioning indicates that the operator is currently reading
	// batches from both inputs and distributing tuples to different partitions
	// based on the hash values. Once both inputs are exhausted, the external
	// hash joiner transitions to externalHJJoinNewPartition state.
	externalHJPartitioning externalHashJoinerState = iota
	// externalHJJoinNewPartition indicates that the operator is setting up the
	// in-memory hash joiner to operate on the new partition. As the first step,
	// the operator searches for next non-empty partition. If there are none, it
	// transitions to externalHJFinished state.
	externalHJJoinNewPartition
	// externalHJJoining indicates that the operator is currently joining tuples
	// from the corresponding partitions from both sides. An in-memory hash join
	// operator is used to perform the join. Once the in-memory operator returns
	// a zero-length batch (indicating that full output for the current
	// partitions has been emitted), the external hash joiner transitions to
	// externalHJJoinNewPartition state.
	externalHJJoining
	// externalHJFinished indicates that the external hash joiner has emitted all
	// tuples already and only zero-length batch will be emitted from now on.
	externalHJFinished
)

// TODO(yuzefovich): make this tunable. Ideally, we would calculate this number
// based on the cardinality of inputs.
const externalHJNumPartitions = 4 << 10 /* 4096 */

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
// If one of the partition hash joins itself runs out of memory, we can
// recursively apply this algorithm. The partition will be divided into
// sub-partitions by a new hash function, spilled to disk, and so on.
// TODO(yuzefovich): actually implement recursive partitioning.
type externalHashJoiner struct {
	twoInputNode
	NonExplainable

	state     externalHashJoinerState
	allocator *Allocator
	spec      hashJoinerSpec

	// Partitioning phase variables.
	leftPartitioner  Partitioner
	rightPartitioner Partitioner
	tupleDistributor *tupleHashDistributor
	// TODO(yuzefovich): use bitmap for this to reduce memory footprint.
	nonEmptyPartition []bool
	numPartitions     int
	scratch           struct {
		// Input sources can have different schemas, so when distributing tuples
		// (i.e. copying them into scratch batch to be spilled) we might need two
		// different batches.
		leftBatch, rightBatch coldata.Batch
	}

	// Join phase variables.
	leftInMemHashJoinerInput, rightInMemHashJoinerInput *partitionerToOperator
	inMemHashJoiner                                     *hashJoiner
	partitionIdxToJoin                                  int
}

var _ Operator = &externalHashJoiner{}

func newExternalHashJoiner(
	allocator *Allocator,
	spec hashJoinerSpec,
	leftInput, rightInput Operator,
	// TODO(yuzefovich): remove this once actual disk queues are in-place.
	diskQueuesUnlimitedAllocator *Allocator,
) Operator {
	leftPartitioner := newDummyPartitioner(diskQueuesUnlimitedAllocator, spec.left.sourceTypes)
	leftInMemHashJoinerInput := newPartitionerToOperator(
		allocator, spec.left.sourceTypes, leftPartitioner, 0, /* partitionIdx */
	)
	rightPartitioner := newDummyPartitioner(diskQueuesUnlimitedAllocator, spec.right.sourceTypes)
	rightInMemHashJoinerInput := newPartitionerToOperator(
		allocator, spec.right.sourceTypes, rightPartitioner, 0, /* partitionIdx */
	)
	ehj := &externalHashJoiner{
		twoInputNode:              newTwoInputNode(leftInput, rightInput),
		allocator:                 allocator,
		spec:                      spec,
		leftPartitioner:           leftPartitioner,
		rightPartitioner:          rightPartitioner,
		numPartitions:             externalHJNumPartitions,
		leftInMemHashJoinerInput:  leftInMemHashJoinerInput,
		rightInMemHashJoinerInput: rightInMemHashJoinerInput,
		inMemHashJoiner: newHashJoiner(
			allocator, spec, leftInMemHashJoinerInput, rightInMemHashJoinerInput,
		).(*hashJoiner),
	}
	ehj.scratch.leftBatch = allocator.NewMemBatch(spec.left.sourceTypes)
	sameSourcesSchema := len(spec.left.sourceTypes) == len(spec.right.sourceTypes)
	for i, leftType := range spec.left.sourceTypes {
		if i < len(spec.right.sourceTypes) && leftType != spec.right.sourceTypes[i] {
			sameSourcesSchema = false
		}
	}
	if sameSourcesSchema {
		// The schemas of both sources are the same, so we can reuse the left
		// scratch batch.
		ehj.scratch.rightBatch = ehj.scratch.leftBatch
	} else {
		ehj.scratch.rightBatch = allocator.NewMemBatch(spec.right.sourceTypes)
	}
	return ehj
}

func (hj *externalHashJoiner) Init() {
	hj.inputOne.Init()
	hj.inputTwo.Init()
	// In the join phase, hash join operator will use the default init hash
	// value, so in order to use a "different" hash function in the partitioning
	// phase we use a different init hash value.
	hj.tupleDistributor = newTupleHashDistributor(
		defaultInitHashValue+1, hj.numPartitions,
	)
	hj.nonEmptyPartition = make([]bool, hj.numPartitions)
	hj.state = externalHJPartitioning
}

func (hj *externalHashJoiner) partitionBatch(
	ctx context.Context,
	batch coldata.Batch,
	scratchBatch coldata.Batch,
	sourceSpec hashJoinerSourceSpec,
	partitioner Partitioner,
) {
	if batch.Length() == 0 {
		return
	}
	selections := hj.tupleDistributor.distribute(
		ctx, batch, sourceSpec.sourceTypes, sourceSpec.eqCols,
	)
	for partitionIdx, sel := range selections {
		if len(sel) > 0 {
			scratchBatch.ResetInternalBatch()
			// The partitioner expects the batches without a selection vector, so we
			// need to copy the tuples according to the selection vector into a
			// scratch batch.
			hj.allocator.PerformOperation(scratchBatch.ColVecs(), func() {
				for i, colvec := range scratchBatch.ColVecs() {
					colvec.Copy(coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							ColType:   sourceSpec.sourceTypes[i],
							Src:       batch.ColVec(i),
							Sel:       sel,
							SrcEndIdx: uint64(len(sel)),
						},
					})
				}
				scratchBatch.SetLength(uint16(len(sel)))
			})
			if err := partitioner.Enqueue(partitionIdx, scratchBatch); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			hj.nonEmptyPartition[partitionIdx] = true
		}
	}
}

func (hj *externalHashJoiner) Next(ctx context.Context) coldata.Batch {
	for {
		switch hj.state {
		case externalHJPartitioning:
			leftBatch := hj.inputOne.Next(ctx)
			rightBatch := hj.inputTwo.Next(ctx)
			if leftBatch.Length() == 0 && rightBatch.Length() == 0 {
				// Both inputs have been partitioned and spilled, so we transition to
				// "joining" phase.
				hj.inMemHashJoiner.Init()
				hj.state = externalHJJoinNewPartition
				continue
			}
			hj.partitionBatch(ctx, leftBatch, hj.scratch.leftBatch, hj.spec.left, hj.leftPartitioner)
			hj.partitionBatch(ctx, rightBatch, hj.scratch.rightBatch, hj.spec.right, hj.rightPartitioner)

		case externalHJJoinNewPartition:
			// Find next non empty partition.
			for hj.partitionIdxToJoin < hj.numPartitions &&
				!hj.nonEmptyPartition[hj.partitionIdxToJoin] {
				hj.partitionIdxToJoin++
			}
			if hj.partitionIdxToJoin == hj.numPartitions {
				// All partitions have been processed, so we clean up the disk
				// infrastructure and transition to finished state.
				if err := hj.leftPartitioner.Close(); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				if err := hj.rightPartitioner.Close(); err != nil {
					execerror.VectorizedInternalPanic(err)
				}
				hj.state = externalHJFinished
				continue
			}
			// Update the inputs to in-memory hash joiner and reset the latter.
			hj.leftInMemHashJoinerInput.partitionIdx = hj.partitionIdxToJoin
			hj.rightInMemHashJoinerInput.partitionIdx = hj.partitionIdxToJoin
			hj.inMemHashJoiner.reset()
			hj.state = externalHJJoining

		case externalHJJoining:
			b := hj.inMemHashJoiner.Next(ctx)
			if b.Length() == 0 {
				hj.partitionIdxToJoin++
				hj.state = externalHJJoinNewPartition
				continue
			}
			return b
		case externalHJFinished:
			return coldata.ZeroBatch
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected externalHashJoinerState %d", hj.state))
		}
	}
}
