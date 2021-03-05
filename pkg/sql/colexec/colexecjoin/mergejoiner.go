// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecjoin

import (
	"context"
	"math"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// group is an ADT representing a contiguous set of rows that match on their
// equality columns.
type group struct {
	rowStartIdx int
	rowEndIdx   int
	// numRepeats is used when expanding each group into a cross product in the
	// build phase.
	numRepeats int
	// toBuild is used in the build phase to determine the right output count.
	// This field should stay in sync with the builder over time.
	toBuild int
	// nullGroup indicates whether the output corresponding to the group should
	// consist of all nulls.
	nullGroup bool
	// unmatched indicates that the rows in the group do not have matching rows
	// from the other side (i.e. other side's group will be a null group).
	// NOTE: during the probing phase, the assumption is that such group will
	// consist of a single row.
	unmatched bool
}

// mjBuildFrom is an indicator of which source we're building the output from.
type mjBuildFrom int

const (
	// mjBuildFromBatch indicates that we should be building from the current
	// probing batches. Note that in such case we might have multiple groups to
	// build.
	mjBuildFromBatch mjBuildFrom = iota
	// mjBuildFromBufferedGroup indicates that we should be building from the
	// buffered group. Note that in such case we might have at most one group to
	// build.
	mjBuildFromBufferedGroup
)

// mjBuilderState contains all the state required to execute the build phase.
type mjBuilderState struct {
	buildFrom mjBuildFrom

	// Fields to identify the groups in the input sources.
	lGroups []group
	rGroups []group

	// outCount keeps record of the current number of rows in the output.
	outCount int
	// outFinished is used to determine if the builder is finished outputting
	// the groups from input.
	outFinished bool

	totalOutCountFromBufferedGroup  int
	alreadyEmittedFromBufferedGroup int

	// Cross product materialization state.
	left  mjBuilderCrossProductState
	right mjBuilderCrossProductState
}

// mjBuilderCrossProductState is used to keep track of builder state within the
// loops to materialize the cross product. Useful for picking up where we left
// off.
type mjBuilderCrossProductState struct {
	groupsIdx      int
	curSrcStartIdx int
	numRepeatsIdx  int
}

// mjBufferedGroup is a helper struct that stores information about the tuples
// from both inputs for the buffered group.
type mjBufferedGroup struct {
	// firstTuple stores a single tuple that was first in the buffered group.
	firstTuple   []coldata.Vec
	scratchBatch coldata.Batch
}

type mjBufferedGroupState struct {
	// Local buffer for the last left and right groups which is used when the
	// group ends with a batch and the group on each side needs to be saved to
	// state in order to be able to continue it in the next batch.
	left        mjBufferedGroup
	right       mjBufferedGroup
	helper      *crossJoinerBase
	needToReset bool
}

// mjProberState contains all the state required to execute in the probing
// phase.
type mjProberState struct {
	// Fields to save the "working" batches to state in between outputs.
	lBatch  coldata.Batch
	rBatch  coldata.Batch
	lIdx    int
	lLength int
	rIdx    int
	rLength int
}

// mjState represents the state of the merge joiner.
type mjState int

const (
	// mjEntry is the entry state of the merge joiner where all the batches and
	// indices are properly set, regardless if Next was called the first time or
	// the 1000th time. This state also routes into the correct state based on
	// the prober state after setup.
	mjEntry mjState = iota

	// mjSourceFinished is the state in which one of the input sources has no
	// more available batches, thus signaling that the joiner should begin
	// wrapping up execution by outputting any remaining groups in state.
	mjSourceFinished

	// mjFinishBufferedGroup is the state in which the previous state resulted in
	// a group that ended with a batch. Such a group was buffered, and this state
	// finishes that group and builds the output.
	mjFinishBufferedGroup

	// mjProbe is the main probing state in which the groups for the current
	// batch are determined.
	mjProbe

	// mjBuild is the state in which the groups determined by the probing states
	// are built, i.e. materialized to the output member by creating the cross
	// product.
	mjBuild

	// mjDone is the final state of the merge joiner in which it'll be returning
	// only zero-length batches. In this state, the disk infrastructure is
	// cleaned up.
	mjDone
)

type mergeJoinInput struct {
	// eqCols specify the indices of the source table equality columns during the
	// merge join.
	eqCols []uint32

	// directions specifies the ordering direction of each column. Note that each
	// direction corresponds to an equality column at the same location, i.e. the
	// direction of eqCols[x] is encoded at directions[x], or
	// len(eqCols) == len(directions).
	directions []execinfrapb.Ordering_Column_Direction

	// sourceTypes specify the types of the input columns of the source table for
	// the merge joiner.
	sourceTypes []*types.T
	// canonicalTypeFamilies stores the canonical type families from
	// sourceTypes. It is stored explicitly rather than being converted at
	// runtime because that conversion would occur in tight loops and
	// noticeably hurt the performance.
	canonicalTypeFamilies []types.Family

	// The distincter is used in the finishGroup phase, and is used only to
	// determine where the current group ends, in the case that the group ended
	// with a batch.
	distincterInput *colexecop.FeedOperator
	distincter      colexecop.Operator
	distinctOutput  []bool

	// source specifies the input operator to the merge join.
	source colexecop.Operator
}

// The merge join operator uses a probe and build approach to generate the
// join. What this means is that instead of going through and expanding the
// cross product row by row, the operator performs two passes.
// The first pass generates a list of groups of matching rows based on the
// equality columns (where a "group" represents a contiguous set of rows that
// match on the equality columns).
// The second pass is where the groups and their associated cross products are
// materialized into the full output.

// Two buffers are used, one for the group on the left table and one for the
// group on the right table. These buffers are only used if the group ends with
// a batch, to make sure that we don't miss any cross product entries while
// expanding the groups (leftGroups and rightGroups) when a group spans
// multiple batches.

// NewMergeJoinOp returns a new merge join operator with the given spec that
// implements sort-merge join. It performs a merge on the left and right input
// sources, based on the equality columns, assuming both inputs are in sorted
// order.
func NewMergeJoinOp(
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	joinType descpb.JoinType,
	left colexecop.Operator,
	right colexecop.Operator,
	leftTypes []*types.T,
	rightTypes []*types.T,
	leftOrdering []execinfrapb.Ordering_Column,
	rightOrdering []execinfrapb.Ordering_Column,
	diskAcc *mon.BoundAccount,
) (colexecop.ResettableOperator, error) {
	// Merge joiner only supports the case when the physical types in the
	// equality columns in both inputs are the same. We, however, also need to
	// support joining on numeric columns of different types or widths. If we
	// encounter such mismatch, we need to cast one of the vectors to another
	// and use the cast vector for equality check.

	// Make a copy of types and orderings to be sure that we don't modify
	// anything unwillingly.
	actualLeftTypes, actualRightTypes := append([]*types.T{}, leftTypes...), append([]*types.T{}, rightTypes...)
	actualLeftOrdering := make([]execinfrapb.Ordering_Column, len(leftOrdering))
	actualRightOrdering := make([]execinfrapb.Ordering_Column, len(rightOrdering))
	copy(actualLeftOrdering, leftOrdering)
	copy(actualRightOrdering, rightOrdering)

	// Iterate over each equality column and check whether a cast is needed. If
	// it is needed for some column, then a cast operator is planned on top of
	// the input from the corresponding side and the types and ordering are
	// adjusted accordingly. We will also need to project out that temporary
	// column, so a simple project will be planned below.
	var needProjection bool
	var err error
	for i := range leftOrdering {
		leftColIdx := leftOrdering[i].ColIdx
		rightColIdx := rightOrdering[i].ColIdx
		leftType := leftTypes[leftColIdx]
		rightType := rightTypes[rightColIdx]
		if !leftType.Identical(rightType) && leftType.IsNumeric() && rightType.IsNumeric() {
			// The types are different and both are numeric, so we need to plan
			// a cast. There is a hierarchy of valid casts:
			//   INT2 -> INT4 -> INT8 -> FLOAT -> DECIMAL
			// and the cast is valid if 'fromType' is mentioned before 'toType'
			// in this chain.
			castLeftToRight := false
			switch leftType.Family() {
			case types.IntFamily:
				switch leftType.Width() {
				case 16:
					castLeftToRight = true
				case 32:
					castLeftToRight = !rightType.Identical(types.Int2)
				default:
					castLeftToRight = rightType.Family() != types.IntFamily
				}
			case types.FloatFamily:
				castLeftToRight = rightType.Family() == types.DecimalFamily
			}
			if castLeftToRight {
				castColumnIdx := len(actualLeftTypes)
				left, err = colexecbase.GetCastOperator(unlimitedAllocator, left, int(leftColIdx), castColumnIdx, leftType, rightType)
				if err != nil {
					return nil, err
				}
				actualLeftTypes = append(actualLeftTypes, rightType)
				actualLeftOrdering[i].ColIdx = uint32(castColumnIdx)
			} else {
				castColumnIdx := len(actualRightTypes)
				right, err = colexecbase.GetCastOperator(unlimitedAllocator, right, int(rightColIdx), castColumnIdx, rightType, leftType)
				if err != nil {
					return nil, err
				}
				actualRightTypes = append(actualRightTypes, leftType)
				actualRightOrdering[i].ColIdx = uint32(castColumnIdx)
			}
			needProjection = true
		}
	}
	base, err := newMergeJoinBase(
		unlimitedAllocator, memoryLimit, diskQueueCfg, fdSemaphore, joinType, left, right,
		actualLeftTypes, actualRightTypes, actualLeftOrdering, actualRightOrdering, diskAcc,
	)
	if err != nil {
		return nil, err
	}
	var mergeJoinerOp colexecop.ResettableOperator
	switch joinType {
	case descpb.InnerJoin:
		mergeJoinerOp = &mergeJoinInnerOp{base}
	case descpb.LeftOuterJoin:
		mergeJoinerOp = &mergeJoinLeftOuterOp{base}
	case descpb.RightOuterJoin:
		mergeJoinerOp = &mergeJoinRightOuterOp{base}
	case descpb.FullOuterJoin:
		mergeJoinerOp = &mergeJoinFullOuterOp{base}
	case descpb.LeftSemiJoin:
		mergeJoinerOp = &mergeJoinLeftSemiOp{base}
	case descpb.RightSemiJoin:
		mergeJoinerOp = &mergeJoinRightSemiOp{base}
	case descpb.LeftAntiJoin:
		mergeJoinerOp = &mergeJoinLeftAntiOp{base}
	case descpb.RightAntiJoin:
		mergeJoinerOp = &mergeJoinRightAntiOp{base}
	case descpb.IntersectAllJoin:
		mergeJoinerOp = &mergeJoinIntersectAllOp{base}
	case descpb.ExceptAllJoin:
		mergeJoinerOp = &mergeJoinExceptAllOp{base}
	default:
		return nil, errors.AssertionFailedf("merge join of type %s not supported", joinType)
	}
	if !needProjection {
		// We didn't add any cast operators, so we can just return the operator
		// right away.
		return mergeJoinerOp, nil
	}
	// We need to add a projection to remove all the cast columns we have added
	// above. Note that all extra columns were appended to the corresponding
	// types slices, so we simply need to include first len(leftTypes) from the
	// left and first len(rightTypes) from the right (paying attention to the
	// join type).
	numLeftTypes := len(leftTypes)
	numRightTypes := len(rightTypes)
	numActualLeftTypes := len(actualLeftTypes)
	numActualRightTypes := len(actualRightTypes)
	if !joinType.ShouldIncludeLeftColsInOutput() {
		numLeftTypes = 0
		numActualLeftTypes = 0
	}
	if !joinType.ShouldIncludeRightColsInOutput() {
		numRightTypes = 0
		numActualRightTypes = 0
	}
	projection := make([]uint32, 0, numLeftTypes+numRightTypes)
	for i := 0; i < numLeftTypes; i++ {
		projection = append(projection, uint32(i))
	}
	for i := 0; i < numRightTypes; i++ {
		// Merge joiner outputs all columns from both sides, and the columns
		// from the right have indices in [numActualLeftTypes,
		// numActualLeftTypes + numActualRightTypes) range.
		projection = append(projection, uint32(numActualLeftTypes+i))
	}
	return colexecbase.NewSimpleProjectOp(
		mergeJoinerOp, numActualLeftTypes+numActualRightTypes, projection,
	).(colexecop.ResettableOperator), nil
}

// Const declarations for the merge joiner cross product (MJCP) zero state.
const (
	zeroMJCPGroupsIdx = 0
	// The sentinel value for curSrcStartIdx is -1, as this:
	// a) indicates that a src has not been started
	// b) panics if the sentinel isn't checked
	zeroMJCPCurSrcStartIdx = -1
	zeroMJCPNumRepeatsIdx  = 0
)

// Package level struct for easy access to the MJCP zero state.
var zeroMJBuilderState = mjBuilderCrossProductState{
	groupsIdx:      zeroMJCPGroupsIdx,
	curSrcStartIdx: zeroMJCPCurSrcStartIdx,
	numRepeatsIdx:  zeroMJCPNumRepeatsIdx,
}

func (s *mjBuilderCrossProductState) reset() {
	s.setBuilderColumnState(zeroMJBuilderState)
}

func (s *mjBuilderCrossProductState) setBuilderColumnState(target mjBuilderCrossProductState) {
	s.groupsIdx = target.groupsIdx
	s.curSrcStartIdx = target.curSrcStartIdx
	s.numRepeatsIdx = target.numRepeatsIdx
}

func newMergeJoinBase(
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	joinType descpb.JoinType,
	left colexecop.Operator,
	right colexecop.Operator,
	leftTypes []*types.T,
	rightTypes []*types.T,
	leftOrdering []execinfrapb.Ordering_Column,
	rightOrdering []execinfrapb.Ordering_Column,
	diskAcc *mon.BoundAccount,
) (*mergeJoinBase, error) {
	lEqCols := make([]uint32, len(leftOrdering))
	lDirections := make([]execinfrapb.Ordering_Column_Direction, len(leftOrdering))
	for i, c := range leftOrdering {
		lEqCols[i] = c.ColIdx
		lDirections[i] = c.Direction
	}

	rEqCols := make([]uint32, len(rightOrdering))
	rDirections := make([]execinfrapb.Ordering_Column_Direction, len(rightOrdering))
	for i, c := range rightOrdering {
		rEqCols[i] = c.ColIdx
		rDirections[i] = c.Direction
	}

	diskQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeReuseCache
	diskQueueCfg.SetDefaultBufferSizeBytesForCacheMode()
	base := &mergeJoinBase{
		joinHelper:         newJoinHelper(left, right),
		unlimitedAllocator: unlimitedAllocator,
		memoryLimit:        memoryLimit,
		diskQueueCfg:       diskQueueCfg,
		fdSemaphore:        fdSemaphore,
		joinType:           joinType,
		left: mergeJoinInput{
			source:                left,
			sourceTypes:           leftTypes,
			canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(leftTypes),
			eqCols:                lEqCols,
			directions:            lDirections,
		},
		right: mergeJoinInput{
			source:                right,
			sourceTypes:           rightTypes,
			canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(rightTypes),
			eqCols:                rEqCols,
			directions:            rDirections,
		},
		diskAcc: diskAcc,
	}
	var err error
	base.left.distincterInput = &colexecop.FeedOperator{}
	base.left.distincter, base.left.distinctOutput, err = colexecbase.OrderedDistinctColsToOperators(
		base.left.distincterInput, lEqCols, leftTypes, false, /* nullsAreDistinct */
	)
	if err != nil {
		return base, err
	}
	base.right.distincterInput = &colexecop.FeedOperator{}
	base.right.distincter, base.right.distinctOutput, err = colexecbase.OrderedDistinctColsToOperators(
		base.right.distincterInput, rEqCols, rightTypes, false, /* nullsAreDistinct */
	)
	if err != nil {
		return base, err
	}
	return base, err
}

// mergeJoinBase extracts the common logic between all merge join operators.
type mergeJoinBase struct {
	*joinHelper
	colexecop.CloserHelper

	unlimitedAllocator *colmem.Allocator
	memoryLimit        int64
	diskQueueCfg       colcontainer.DiskQueueCfg
	fdSemaphore        semaphore.Semaphore
	joinType           descpb.JoinType
	left               mergeJoinInput
	right              mergeJoinInput

	// Output buffer definition.
	output      coldata.Batch
	outputTypes []*types.T
	// outputReady is a flag to indicate that merge joiner is ready to emit an
	// output batch.
	outputReady bool

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer

	state         mjState
	bufferedGroup mjBufferedGroupState
	proberState   mjProberState
	builderState  mjBuilderState

	diskAcc *mon.BoundAccount
}

var _ colexecop.Resetter = &mergeJoinBase{}
var _ colexecop.Closer = &mergeJoinBase{}

func (o *mergeJoinBase) Reset(ctx context.Context) {
	if r, ok := o.left.source.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	if r, ok := o.right.source.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	o.outputReady = false
	o.state = mjEntry
	o.bufferedGroup.helper.Reset(ctx)
	o.bufferedGroup.needToReset = false
	o.proberState.lBatch = nil
	o.proberState.rBatch = nil
	o.resetBuilderCrossProductState()
}

func (o *mergeJoinBase) Init(ctx context.Context) {
	if !o.init(ctx) {
		return
	}
	o.outputTypes = o.joinType.MakeOutputTypes(o.left.sourceTypes, o.right.sourceTypes)
	o.bufferedGroup.left.firstTuple = o.unlimitedAllocator.NewMemBatchWithFixedCapacity(
		o.left.sourceTypes, 1, /* capacity */
	).ColVecs()
	o.bufferedGroup.right.firstTuple = o.unlimitedAllocator.NewMemBatchWithFixedCapacity(
		o.right.sourceTypes, 1, /* capacity */
	).ColVecs()
	o.bufferedGroup.helper = newCrossJoinerBase(
		o.unlimitedAllocator, o.joinType, o.left.sourceTypes, o.right.sourceTypes,
		o.memoryLimit, o.diskQueueCfg, o.fdSemaphore, o.diskAcc,
	)
	o.bufferedGroup.helper.init(o.Ctx)

	o.builderState.lGroups = make([]group, 1)
	o.builderState.rGroups = make([]group, 1)

	const sizeOfGroup = int(unsafe.Sizeof(group{}))
	o.unlimitedAllocator.AdjustMemoryUsage(int64(8 * coldata.BatchSize() * sizeOfGroup))
	o.groups = makeGroupsBuffer(coldata.BatchSize())
	o.resetBuilderCrossProductState()
}

func (o *mergeJoinBase) resetBuilderCrossProductState() {
	o.builderState.left.reset()
	o.builderState.right.reset()
}

// appendToBufferedGroup appends all the tuples from batch that are part of the
// same group as the ones in the buffered group that corresponds to the input
// source. This needs to happen when a group starts at the end of an input
// batch and can continue into the following batches.
// A zero-length batch needs to be appended when no more batches will be
// appended to the buffered group.
func (o *mergeJoinBase) appendToBufferedGroup(
	input *mergeJoinInput, batch coldata.Batch, sel []int, groupStartIdx int, groupLength int,
) {
	var (
		bufferedGroup     *mjBufferedGroup
		sourceTypes       []*types.T
		bufferedTuples    *colexecutils.SpillingQueue
		numBufferedTuples int
	)
	if input == &o.left {
		sourceTypes = o.left.sourceTypes
		bufferedGroup = &o.bufferedGroup.left
		bufferedTuples = o.bufferedGroup.helper.left.tuples
		numBufferedTuples = o.bufferedGroup.helper.left.numTuples
		o.bufferedGroup.helper.left.numTuples += groupLength
	} else {
		sourceTypes = o.right.sourceTypes
		bufferedGroup = &o.bufferedGroup.right
		bufferedTuples = o.bufferedGroup.helper.right.tuples
		numBufferedTuples = o.bufferedGroup.helper.right.numTuples
		o.bufferedGroup.helper.right.numTuples += groupLength
	}
	if batch.Length() == 0 || groupLength == 0 {
		// We have finished appending to this buffered group, so we need to
		// Enqueue a zero-length batch per the contract of the spilling queue.
		bufferedTuples.Enqueue(o.Ctx, coldata.ZeroBatch)
		return
	}
	// TODO(yuzefovich): for LEFT/RIGHT ANTI joins we only need to store the
	// first tuple (in order to find the boundaries of the groups) since all
	// of the buffered tuples do have a match and, thus, don't contribute to
	// the output.
	// TODO(yuzefovich): for INTERSECT/EXCEPT ALL joins we can buffer only
	// tuples from the left side and count the number of tuples on the right.
	// TODO(yuzefovich): for LEFT/RIGHT SEMI joins we only need to buffer tuples
	// from one side (left/right respectively).
	if numBufferedTuples == 0 {
		o.unlimitedAllocator.PerformOperation(bufferedGroup.firstTuple, func() {
			for colIdx := range sourceTypes {
				bufferedGroup.firstTuple[colIdx].Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:         batch.ColVec(colIdx),
							Sel:         sel,
							DestIdx:     0,
							SrcStartIdx: groupStartIdx,
							SrcEndIdx:   groupStartIdx + 1,
						},
					},
				)
			}
		})
	}

	// For now, we don't enforce any footprint-based memory limit.
	// TODO(yuzefovich): refactor this.
	const maxBatchMemSize = math.MaxInt64
	bufferedGroup.scratchBatch, _ = o.unlimitedAllocator.ResetMaybeReallocate(
		input.sourceTypes, bufferedGroup.scratchBatch, groupLength, maxBatchMemSize,
	)
	o.unlimitedAllocator.PerformOperation(bufferedGroup.scratchBatch.ColVecs(), func() {
		for colIdx := range input.sourceTypes {
			bufferedGroup.scratchBatch.ColVec(colIdx).Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:         batch.ColVec(colIdx),
						Sel:         sel,
						DestIdx:     0,
						SrcStartIdx: groupStartIdx,
						SrcEndIdx:   groupStartIdx + groupLength,
					},
				},
			)
		}
		bufferedGroup.scratchBatch.SetLength(groupLength)
	})
	bufferedTuples.Enqueue(o.Ctx, bufferedGroup.scratchBatch)
}

// setBuilderSourceToBatch sets the builder state to use groups from the
// circular group buffer and the batches from input. This happens when we have
// groups that are fully contained within a single input batch from each of the
// sources.
func (o *mergeJoinBase) setBuilderSourceToBatch() {
	o.builderState.lGroups, o.builderState.rGroups = o.groups.getGroups()
	o.builderState.buildFrom = mjBuildFromBatch
}

// initProberState sets the batches, lengths, and current indices to the right
// locations given the last iteration of the operator.
func (o *mergeJoinBase) initProberState() {
	// If this is the first batch or we're done with the current batch, get the
	// next batch.
	if o.proberState.lBatch == nil || (o.proberState.lLength != 0 && o.proberState.lIdx == o.proberState.lLength) {
		o.proberState.lIdx, o.proberState.lBatch = 0, o.left.source.Next()
		o.proberState.lLength = o.proberState.lBatch.Length()
	}
	if o.proberState.rBatch == nil || (o.proberState.rLength != 0 && o.proberState.rIdx == o.proberState.rLength) {
		o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next()
		o.proberState.rLength = o.proberState.rBatch.Length()
	}
	if o.bufferedGroup.needToReset {
		o.bufferedGroup.helper.Reset(o.Ctx)
		o.bufferedGroup.needToReset = false
	}
}

// nonEmptyBufferedGroup returns true if there is a buffered group that needs
// to be finished.
func (o *mergeJoinBase) nonEmptyBufferedGroup() bool {
	return o.bufferedGroup.helper.left.numTuples > 0 || o.bufferedGroup.helper.right.numTuples > 0
}

// sourceFinished returns true if either of input sources has no more rows.
func (o *mergeJoinBase) sourceFinished() bool {
	return o.proberState.lLength == 0 || o.proberState.rLength == 0
}

// finishBufferedGroup appends a zero-length batch to the buffered group which
// is required by the contract of the spilling queue.
func (o *mergeJoinBase) finishBufferedGroup(input *mergeJoinInput) {
	o.appendToBufferedGroup(
		input, coldata.ZeroBatch, nil, /* sel */
		0 /* groupStartIdx */, 0, /* groupLength */
	)
}

// completeBufferedGroup extends the buffered group corresponding to input.
// First, we check that the first row in batch is still part of the same group.
// If this is the case, we use the Distinct operator to find the first
// occurrence in batch (or subsequent batches) that doesn't match the current
// group.
// NOTE: we will be buffering all batches until we find such non-matching tuple
// (or until we exhaust the input).
// TODO(yuzefovich): this can be refactored so that only the right side does
// unbounded buffering.
// SIDE EFFECT: can append to the buffered group corresponding to the source.
func (o *mergeJoinBase) completeBufferedGroup(
	input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) (_ coldata.Batch, idx int, batchLength int) {
	batchLength = batch.Length()
	if o.isBufferedGroupFinished(input, batch, rowIdx) {
		o.finishBufferedGroup(input)
		return batch, rowIdx, batchLength
	}

	isBufferedGroupComplete := false
	// It is ok that we might call Init() multiple times - it'll be a noop after
	// the first one.
	input.distincter.Init(o.Ctx)
	input.distincter.(colexecop.Resetter).Reset(o.Ctx)
	// Ignore the first row of the distincter in the first pass since we already
	// know that we are in the same group and, thus, the row is not distinct,
	// regardless of what the distincter outputs.
	loopStartIndex := 1
	var sel []int
	for !isBufferedGroupComplete {
		// Note that we're not resetting the distincter on every loop iteration
		// because if we're doing the second, third, etc, iteration, then all the
		// previous iterations had only the matching tuples to the buffered group,
		// so the distincter - in a sense - compares the incoming tuples to the
		// first tuple of the first iteration (which we know is the same group).
		input.distincterInput.SetBatch(batch)
		input.distincter.Next()

		sel = batch.Selection()
		var groupLength int
		if sel != nil {
			for groupLength = loopStartIndex; groupLength < batchLength; groupLength++ {
				if input.distinctOutput[sel[groupLength]] {
					// We found the beginning of a new group!
					isBufferedGroupComplete = true
					break
				}
			}
		} else {
			for groupLength = loopStartIndex; groupLength < batchLength; groupLength++ {
				if input.distinctOutput[groupLength] {
					// We found the beginning of a new group!
					isBufferedGroupComplete = true
					break
				}
			}
		}

		// Zero out the distinct output for the next pass.
		copy(input.distinctOutput[:batchLength], colexecutils.ZeroBoolColumn)
		loopStartIndex = 0

		// Buffer all the tuples that are part of the buffered group.
		o.appendToBufferedGroup(input, batch, sel, rowIdx, groupLength)
		rowIdx += groupLength

		if !isBufferedGroupComplete {
			// The buffered group is still not complete which means that we have
			// just appended all the tuples from batch to it, so we need to get a
			// fresh batch from the input.
			rowIdx, batch = 0, input.source.Next()
			batchLength = batch.Length()
			if batchLength == 0 {
				// The input has been exhausted, so the buffered group is now complete.
				isBufferedGroupComplete = true
				o.finishBufferedGroup(input)
			}
		}
	}

	return batch, rowIdx, batchLength
}

// finishProbe completes the buffered groups on both sides of the input.
func (o *mergeJoinBase) finishProbe() {
	o.proberState.lBatch, o.proberState.lIdx, o.proberState.lLength = o.completeBufferedGroup(
		&o.left,
		o.proberState.lBatch,
		o.proberState.lIdx,
	)
	o.proberState.rBatch, o.proberState.rIdx, o.proberState.rLength = o.completeBufferedGroup(
		&o.right,
		o.proberState.rBatch,
		o.proberState.rIdx,
	)
}

func (o *mergeJoinBase) Close() error {
	if !o.CloserHelper.Close() {
		return nil
	}
	var lastErr error
	for _, op := range []colexecop.Operator{o.left.source, o.right.source} {
		if c, ok := op.(colexecop.Closer); ok {
			if err := c.Close(); err != nil {
				lastErr = err
			}
		}
	}
	if h := o.bufferedGroup.helper; h != nil {
		if err := h.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
