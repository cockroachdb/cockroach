// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecdisk

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
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
	args *colexecargs.NewColOperatorArgs,
	input colexecop.Operator,
	inputTypes []*types.T,
	createDiskBackedSorter DiskBackedSorterConstructor,
	inMemUnorderedDistinct colexecop.Operator,
	diskAcc *mon.BoundAccount,
	diskQueueMemAcc *mon.BoundAccount,
) (colexecop.Operator, colexecop.Closer) {
	distinctSpec := args.Spec.Core.Distinct
	distinctCols := distinctSpec.DistinctColumns
	inMemMainOpConstructor := func(partitionedInputs []*partitionerToOperator) colexecop.ResettableOperator {
		// Note that the hash-based partitioner will make sure that partitions
		// to process using the in-memory unordered distinct fit under the
		// limit, so we use an unlimited allocator.
		// TODO(yuzefovich): it might be worth increasing the number of buckets.
		return colexec.NewUnorderedDistinct(
			unlimitedAllocator, partitionedInputs[0], distinctCols,
			inputTypes, distinctSpec.NullsAreDistinct, distinctSpec.ErrorOnDup,
		)
	}
	diskBackedFallbackOpConstructor := func(
		partitionedInputs []*partitionerToOperator,
		maxNumberActivePartitions int,
		_ semaphore.Semaphore,
	) colexecop.ResettableOperator {
		// The distinct operator *must* keep the first tuple from the input
		// among all that are identical on distinctCols. In order to guarantee
		// such behavior in the fallback, we append an ordinality column to
		// every tuple before the disk-backed sorter and include that column in
		// the desired ordering. We then project out that temporary column
		// before feeding the tuples into the ordered distinct.
		ordinalityOp := colexecbase.NewOrdinalityOp(unlimitedAllocator, partitionedInputs[0], len(inputTypes))
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
		diskBackedWithoutOrdinality := colexecbase.NewSimpleProjectOp(diskBackedSorter, len(sortTypes), projection)
		return colexecbase.NewOrderedDistinct(
			diskBackedWithoutOrdinality, distinctCols, inputTypes,
			distinctSpec.NullsAreDistinct, distinctSpec.ErrorOnDup,
		)
	}
	// We have to be careful to not emit duplicates of already emitted by the
	// in-memory operator tuples, so we plan a special filterer operator to
	// remove all such tuples.
	input = &unorderedDistinctFilterer{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		ud:             inMemUnorderedDistinct.(*colexec.UnorderedDistinct),
	}
	numRequiredActivePartitions := colexecop.ExternalSorterMinPartitions
	ed := newHashBasedPartitioner(
		unlimitedAllocator,
		flowCtx,
		args,
		"external unordered distinct", /* name */
		[]colexecop.Operator{input},
		[][]*types.T{inputTypes},
		[][]uint32{distinctCols},
		inMemMainOpConstructor,
		diskBackedFallbackOpConstructor,
		diskAcc,
		diskQueueMemAcc,
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
		return ed, ed
	}
	// TODO(yuzefovich): the fact that we're planning an additional external
	// sort isn't accounted for when considering the number file descriptors to
	// acquire. Not urgent, but it should be fixed.
	maxNumberActivePartitions := calculateMaxNumberActivePartitions(flowCtx, args, numRequiredActivePartitions)
	return createDiskBackedSorter(ed, inputTypes, outputOrdering.Columns, maxNumberActivePartitions), ed
}

// unorderedDistinctFilterer filters out tuples that are duplicates of the
// tuples already emitted by the unordered distinct.
type unorderedDistinctFilterer struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	ud *colexec.UnorderedDistinct
	// seenBatch tracks whether the operator has already read at least one
	// batch.
	seenBatch bool
}

var _ colexecop.Operator = &unorderedDistinctFilterer{}

func (f *unorderedDistinctFilterer) Next() coldata.Batch {
	if f.ud.Ht.Vals.Length() == 0 {
		// The hash table is empty, so there is nothing to filter against.
		return f.Input.Next()
	}
	for {
		batch := f.Input.Next()
		if batch.Length() == 0 {
			return coldata.ZeroBatch
		}
		if !f.seenBatch {
			// This is the first batch we received from bufferExportingOperator
			// and the hash table is not empty; therefore, this batch must be
			// the last input batch coming from the in-memory unordered
			// distinct.
			//
			// That batch has already been updated in-place to contain only
			// distinct tuples all of which have been appended to the hash
			// table, so we don't need to perform filtering on it. However, we
			// might need to repair the hash table in case the OOM error
			// occurred when tuples were being appended to f.ht.Vals.
			//
			// See https://github.com/cockroachdb/cockroach/pull/58006#pullrequestreview-565859919
			// for all the gory details.
			f.ud.Ht.RepairAfterDistinctBuild()
			f.ud.MaybeEmitErrorOnDup(f.ud.LastInputBatchOrigLen, batch.Length())
			f.seenBatch = true
			return batch
		}
		origLen := batch.Length()
		// The unordered distinct has emitted some tuples, so we need to check
		// all tuples in batch against the hash table.
		f.ud.Ht.ComputeHashAndBuildChains(batch)
		// Remove the duplicates within batch itself.
		f.ud.Ht.RemoveDuplicates(
			batch, f.ud.Ht.Keys, f.ud.Ht.ProbeScratch.First, f.ud.Ht.ProbeScratch.Next,
			f.ud.Ht.CheckProbeForDistinct, true, /* probingAgainstItself */
		)
		// Remove the duplicates of already emitted distinct tuples.
		f.ud.Ht.RemoveDuplicates(
			batch, f.ud.Ht.Keys, f.ud.Ht.BuildScratch.First, f.ud.Ht.BuildScratch.Next,
			f.ud.Ht.CheckBuildForDistinct, false, /* probingAgainstItself */
		)
		f.ud.MaybeEmitErrorOnDup(origLen, batch.Length())
		if batch.Length() > 0 {
			return batch
		}
	}
}
