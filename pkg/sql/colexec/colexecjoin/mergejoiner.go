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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

// mjBuilderState contains all the state required to execute the build phase.
type mjBuilderState struct {
	// Fields to identify the groups in the input sources.
	lGroups []group
	rGroups []group

	// outCount keeps record of the current number of rows in the output.
	outCount int

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

type mjBufferedGroupState struct {
	// leftFirstTuple is the first tuple of the left buffered group. It is set
	// only in case the left buffered group spans more than one input batch.
	leftFirstTuple []coldata.Vec
	// leftGroupStartIdx is the position within the current left batch where the
	// left buffered group starts. If the group spans multiple batches, this
	// will be set to 0 on all consecutive batches.
	//
	// Note that proberState.lIdx indicates the exclusive end position for the
	// left buffered group within the current batch.
	leftGroupStartIdx int
	// leftBatchDone indicates whether the output from the current left batch
	// has been fully built.
	leftBatchDone bool
	// rightFirstTuple is the first tuple of the right buffered group. It is set
	//only in case the right buffered group spans more than one input batch.
	rightFirstTuple []coldata.Vec
	// scratchSel is a scratch selection vector initialized only when needed.
	scratchSel []int

	// helper is the building facility for the cross join of the buffered group.
	helper *crossJoinerBase
}

// mjProberState contains all the state required to execute in the probing
// phase.
type mjProberState struct {
	// Fields to save the "working" batches to state in between outputs.
	lBatch coldata.Batch
	rBatch coldata.Batch
	// lIdx indicates the index of the first left tuple that hasn't been probed
	// yet.
	lIdx    int
	lLength int
	// rIdx indicates the index of the first right tuple that hasn't been probed
	// yet.
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
	// wrapping up execution by outputting any remaining groups in state. After
	// reaching this state, we can only build from the batch.
	mjSourceFinished

	// mjProbe is the main probing state in which the groups for the current
	// batch are determined.
	mjProbe

	// mjBuildFromBatch indicates that we should be building from the current
	// probing batches. Note that in such case we might have multiple groups to
	// build.
	mjBuildFromBatch

	// mjBuildFromBufferedGroup indicates that we should be building from the
	// current left batch and the right buffered group. Note that in such case
	// we have at most one group to build and are building the output one batch
	// from the left input at a time.
	mjBuildFromBufferedGroup

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
	evalCtx *tree.EvalContext,
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
				left, err = colexecbase.GetCastOperator(unlimitedAllocator, left, int(leftColIdx), castColumnIdx, leftType, rightType, evalCtx)
				if err != nil {
					return nil, err
				}
				actualLeftTypes = append(actualLeftTypes, rightType)
				actualLeftOrdering[i].ColIdx = uint32(castColumnIdx)
			} else {
				castColumnIdx := len(actualRightTypes)
				right, err = colexecbase.GetCastOperator(unlimitedAllocator, right, int(rightColIdx), castColumnIdx, rightType, leftType, evalCtx)
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
	output         coldata.Batch
	outputCapacity int
	outputTypes    []*types.T

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
	o.state = mjEntry
	o.bufferedGroup.helper.Reset(ctx)
	o.proberState.lBatch = nil
	o.proberState.rBatch = nil
	o.resetBuilderCrossProductState()
}

func (o *mergeJoinBase) Init(ctx context.Context) {
	if !o.init(ctx) {
		return
	}
	o.outputTypes = o.joinType.MakeOutputTypes(o.left.sourceTypes, o.right.sourceTypes)
	o.bufferedGroup.leftFirstTuple = o.unlimitedAllocator.NewMemBatchWithFixedCapacity(
		o.left.sourceTypes, 1, /* capacity */
	).ColVecs()
	o.bufferedGroup.rightFirstTuple = o.unlimitedAllocator.NewMemBatchWithFixedCapacity(
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

// startLeftBufferedGroup initializes the left buffered group. It will set the
// first tuple in case the left buffered group doesn't end in the current left
// batch.
func (o *mergeJoinBase) startLeftBufferedGroup(sel []int, groupStartIdx int, groupLength int) {
	if groupStartIdx+groupLength < o.proberState.lLength {
		// The left buffered group is complete within the current left batch, so
		// we don't need to copy the first tuple.
		return
	}
	o.unlimitedAllocator.PerformOperation(o.bufferedGroup.leftFirstTuple, func() {
		for colIdx := range o.left.sourceTypes {
			o.bufferedGroup.leftFirstTuple[colIdx].Copy(
				coldata.SliceArgs{
					Src:         o.proberState.lBatch.ColVec(colIdx),
					Sel:         sel,
					DestIdx:     0,
					SrcStartIdx: groupStartIdx,
					SrcEndIdx:   groupStartIdx + 1,
				},
			)
		}
	})
}

// appendToRightBufferedGroup appends the tuples in
// [groupStartIdx; groupStartIdx+groupLength) range from the current right
// batch. This needs to happen when a group starts at the end of an input
// batch and can continue into the following batches.
//
// A zero-length batch needs to be appended when no more batches will be
// appended to the buffered group (which can be achieved by specifying an empty
// range with groupLength == 0).
func (o *mergeJoinBase) appendToRightBufferedGroup(sel []int, groupStartIdx int, groupLength int) {
	bufferedTuples := o.bufferedGroup.helper.rightTuples
	if groupLength == 0 {
		// We have finished appending to this buffered group, so we need to
		// Enqueue a zero-length batch per the contract of the spilling queue.
		bufferedTuples.Enqueue(o.Ctx, coldata.ZeroBatch)
		return
	}
	sourceTypes := o.right.sourceTypes
	numBufferedTuples := o.bufferedGroup.helper.numRightTuples
	o.bufferedGroup.helper.numRightTuples += groupLength
	if numBufferedTuples == 0 && groupStartIdx+groupLength == o.proberState.rLength {
		// Set the right first tuple only if this is the first call to this
		// method for the current right buffered group and if the group doesn't
		// end in the current batch.
		o.unlimitedAllocator.PerformOperation(o.bufferedGroup.rightFirstTuple, func() {
			for colIdx := range sourceTypes {
				o.bufferedGroup.rightFirstTuple[colIdx].Copy(
					coldata.SliceArgs{
						Src:         o.proberState.rBatch.ColVec(colIdx),
						Sel:         sel,
						DestIdx:     0,
						SrcStartIdx: groupStartIdx,
						SrcEndIdx:   groupStartIdx + 1,
					},
				)
			}
		})
	}

	// TODO(yuzefovich): check whether it's worth templating this method out as
	// well as having join-type-specific crossJoinerBase.
	switch o.joinType {
	case descpb.LeftSemiJoin, descpb.RightAntiJoin:
		// For LEFT SEMI and RIGHT ANTI joins we only need to store the first
		// tuple (in order to find the boundaries of the groups) since all of
		// the buffered tuples don't/do have a match and, thus, do/don't
		// contribute to the output.
		return
	case descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		// For INTERSECT/EXCEPT ALL joins we only need the number of tuples on
		// the right side (which we have already updated above).
		return
	}

	// Update the selection on the probing batch to only include tuples from the
	// buffered group.
	rBatch, rLength := o.proberState.rBatch, o.proberState.rLength
	rSel := rBatch.Selection()
	rBatchHasSel := rSel != nil
	// No need to modify the batch if the whole batch is part of the buffered
	// group.
	needToModify := groupStartIdx != 0 || groupLength != rLength
	if needToModify {
		if rBatchHasSel {
			o.bufferedGroup.scratchSel = colexecutils.EnsureSelectionVectorLength(o.bufferedGroup.scratchSel, rLength)
			copy(o.bufferedGroup.scratchSel, rSel)
			for idx := 0; idx < groupLength; idx++ {
				rSel[idx] = o.bufferedGroup.scratchSel[groupStartIdx+idx]
			}
		} else {
			rBatch.SetSelection(true)
			rSel = rBatch.Selection()
			copy(rSel, defaultSelectionVector[groupStartIdx:groupStartIdx+groupLength])
		}
		rBatch.SetLength(groupLength)
	}

	bufferedTuples.Enqueue(o.Ctx, rBatch)

	// If we had to modify the batch, then restore the original state now.
	if needToModify {
		if rBatchHasSel {
			copy(rSel, o.bufferedGroup.scratchSel)
		} else {
			rBatch.SetSelection(false)
		}
		rBatch.SetLength(rLength)
	}
}

// defaultSelectionVector contains all integers in [0, coldata.MaxBatchSize)
// range.
var defaultSelectionVector []int

func init() {
	defaultSelectionVector = make([]int, coldata.MaxBatchSize)
	for i := range defaultSelectionVector {
		defaultSelectionVector[i] = i
	}
}

// sourceFinished returns true if either of input sources has no more rows.
func (o *mergeJoinBase) sourceFinished() bool {
	return o.proberState.lLength == 0 || o.proberState.rLength == 0
}

// continueLeftBufferedGroup fetches the next batch from the left input and
// and updates the probing and buffered group states accordingly.
func (o *mergeJoinBase) continueLeftBufferedGroup() {
	// Get the next batch from the left.
	o.proberState.lIdx, o.proberState.lBatch = 0, o.left.source.Next()
	o.proberState.lLength = o.proberState.lBatch.Length()
	o.bufferedGroup.leftGroupStartIdx = 0
	if o.proberState.lLength == 0 {
		// The left input has been fully exhausted.
		return
	}
	// Check whether the first tuple of this batch is still part of the left
	// buffered group.
	if o.isBufferedGroupFinished(&o.left, o.bufferedGroup.leftFirstTuple, o.proberState.lBatch, 0 /* rowIdx */) {
		return
	}

	// It is ok that we might call Init() multiple times - it'll be a noop after
	// the first one.
	o.left.distincter.Init(o.Ctx)
	o.left.distincter.(colexecop.Resetter).Reset(o.Ctx)
	// Ignore the first row of the distincter in the first pass since we already
	// know that we are in the same group and, thus, the row is not distinct,
	// regardless of what the distincter outputs.
	groupLength := 1
	var sel []int
	o.left.distincterInput.SetBatch(o.proberState.lBatch)
	o.left.distincter.Next()

	sel = o.proberState.lBatch.Selection()
	if sel != nil {
		for ; groupLength < o.proberState.lLength; groupLength++ {
			if o.left.distinctOutput[sel[groupLength]] {
				// We found the beginning of a new group!
				break
			}
		}
	} else {
		for ; groupLength < o.proberState.lLength; groupLength++ {
			if o.left.distinctOutput[groupLength] {
				// We found the beginning of a new group!
				break
			}
		}
	}

	// Zero out the distinct output for the next pass.
	copy(o.left.distinctOutput[:o.proberState.lLength], colexecutils.ZeroBoolColumn)
	o.proberState.lIdx += groupLength
}

// finishRightBufferedGroup appends a zero-length batch to the right buffered
// group which is required by the contract of the spilling queue. Note that it
// is safe to call this method multiple times (only the first one is not a
// noop).
func (o *mergeJoinBase) finishRightBufferedGroup() {
	o.appendToRightBufferedGroup(
		nil /* sel */, 0 /* groupStartIdx */, 0, /* groupLength */
	)
}

// completeRightBufferedGroup extends the right buffered group. It will read all
// tuples from the right input that are part of the current right buffered group
// (which must have been initialized via appendToRightBufferedGroup).
//
// NOTE: we will be buffering all batches until we find such non-matching tuple
// (or until we exhaust the right input).
func (o *mergeJoinBase) completeRightBufferedGroup() {
	// Get the next batch from the right.
	o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next()
	o.proberState.rLength = o.proberState.rBatch.Length()
	// The right input has been fully exhausted.
	if o.proberState.rLength == 0 {
		o.finishRightBufferedGroup()
		return
	}
	// Check whether the first tuple of this batch is still part of the right
	// buffered group.
	if o.isBufferedGroupFinished(&o.right, o.bufferedGroup.rightFirstTuple, o.proberState.rBatch, 0 /* rowIdx */) {
		o.finishRightBufferedGroup()
		return
	}

	isBufferedGroupComplete := false
	// It is ok that we might call Init() multiple times - it'll be a noop after
	// the first one.
	o.right.distincter.Init(o.Ctx)
	o.right.distincter.(colexecop.Resetter).Reset(o.Ctx)
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
		o.right.distincterInput.SetBatch(o.proberState.rBatch)
		o.right.distincter.Next()

		sel = o.proberState.rBatch.Selection()
		var groupLength int
		if sel != nil {
			for groupLength = loopStartIndex; groupLength < o.proberState.rLength; groupLength++ {
				if o.right.distinctOutput[sel[groupLength]] {
					// We found the beginning of a new group!
					isBufferedGroupComplete = true
					break
				}
			}
		} else {
			for groupLength = loopStartIndex; groupLength < o.proberState.rLength; groupLength++ {
				if o.right.distinctOutput[groupLength] {
					// We found the beginning of a new group!
					isBufferedGroupComplete = true
					break
				}
			}
		}

		// Zero out the distinct output for the next pass.
		copy(o.right.distinctOutput[:o.proberState.rLength], colexecutils.ZeroBoolColumn)
		loopStartIndex = 0

		// Buffer all the tuples that are part of the buffered group.
		o.appendToRightBufferedGroup(sel, o.proberState.rIdx, groupLength)
		o.proberState.rIdx += groupLength

		if !isBufferedGroupComplete {
			// The buffered group is still not complete which means that we have
			// just appended all the tuples from batch to it, so we need to get a
			// fresh batch from the input.
			o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next()
			o.proberState.rLength = o.proberState.rBatch.Length()
			if o.proberState.rLength == 0 {
				// The input has been exhausted, so the buffered group is now complete.
				isBufferedGroupComplete = true
			}
		}
	}
	o.finishRightBufferedGroup()
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
