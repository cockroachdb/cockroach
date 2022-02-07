// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// NewCrossJoiner returns a vectorized cross join operator.
func NewCrossJoiner(
	unlimitedAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	joinType descpb.JoinType,
	left colexecop.Operator,
	right colexecop.Operator,
	leftTypes []*types.T,
	rightTypes []*types.T,
	diskAcc *mon.BoundAccount,
) colexecop.Operator {
	return &crossJoiner{
		crossJoinerBase: newCrossJoinerBase(
			unlimitedAllocator,
			joinType,
			leftTypes,
			rightTypes,
			memoryLimit,
			diskQueueCfg,
			fdSemaphore,
			diskAcc,
		),
		joinHelper:            newJoinHelper(left, right),
		unlimitedAllocator:    unlimitedAllocator,
		outputTypes:           joinType.MakeOutputTypes(leftTypes, rightTypes),
		maxOutputBatchMemSize: memoryLimit,
	}
}

type crossJoiner struct {
	*crossJoinerBase
	*joinHelper

	unlimitedAllocator    *colmem.Allocator
	rightInputConsumed    bool
	outputTypes           []*types.T
	maxOutputBatchMemSize int64
	// isLeftAllNulls and isRightAllNulls indicate whether the output vectors
	// corresponding to the left and right inputs, respectively, should consist
	// only of NULL values. This is the case when we have right or left,
	// respectively, unmatched tuples.
	isLeftAllNulls, isRightAllNulls bool
	// done indicates that the cross joiner has fully built its output and
	// closed the spilling queue. Once set to true, only zero-length batches are
	// emitted.
	done bool
}

var _ colexecop.ClosableOperator = &crossJoiner{}
var _ colexecop.ResettableOperator = &crossJoiner{}

func (c *crossJoiner) Init(ctx context.Context) {
	if !c.joinHelper.init(ctx) {
		return
	}
	// Note that c.joinHelper.Ctx might contain an updated context, so we use
	// that rather than ctx.
	c.crossJoinerBase.init(c.joinHelper.Ctx)
}

func (c *crossJoiner) Next() coldata.Batch {
	if c.done {
		return coldata.ZeroBatch
	}
	if !c.rightInputConsumed {
		c.consumeRightInput(c.Ctx)
		c.setupForBuilding()
	}
	willEmit := c.willEmit()
	if willEmit == 0 {
		if err := c.Close(c.Ctx); err != nil {
			colexecerror.InternalError(err)
		}
		c.done = true
		return coldata.ZeroBatch
	}
	c.output, _ = c.unlimitedAllocator.ResetMaybeReallocate(
		c.outputTypes, c.output, willEmit, c.maxOutputBatchMemSize,
	)
	if willEmit > c.output.Capacity() {
		willEmit = c.output.Capacity()
	}
	if c.joinType.ShouldIncludeLeftColsInOutput() {
		if c.isLeftAllNulls {
			setAllNulls(c.output.ColVecs()[:len(c.left.types)], willEmit)
		} else {
			c.buildFromLeftInput(c.Ctx, 0 /* destStartIdx */)
		}
	}
	if c.joinType.ShouldIncludeRightColsInOutput() {
		if c.isRightAllNulls {
			setAllNulls(c.output.ColVecs()[c.builderState.rightColOffset:], willEmit)
		} else {
			c.buildFromRightInput(c.Ctx, 0 /* destStartIdx */)
		}
	}
	c.output.SetLength(willEmit)
	c.builderState.numEmittedCurLeftBatch += willEmit
	c.builderState.numEmittedTotal += willEmit
	return c.output
}

// readNextLeftBatch fetches the next batch from the left input, prepares the
// builder for it (assuming that all rows in the batch contribute to the cross
// product), and returns the length of the batch.
func (c *crossJoiner) readNextLeftBatch() int {
	leftBatch := c.inputOne.Next()
	c.prepareForNextLeftBatch(leftBatch, 0 /* startIdx */, leftBatch.Length())
	return leftBatch.Length()
}

// consumeRightInput determines the kind of information the cross joiner needs
// from its right input (in some cases, we don't need to buffer all tuples from
// the right) and consumes the right input accordingly. It also checks whether
// we need any tuples from the left and possibly reads a single batch, depending
// on the join type.
func (c *crossJoiner) consumeRightInput(ctx context.Context) {
	c.rightInputConsumed = true
	var needRightTuples, needOnlyNumRightTuples bool
	switch c.joinType {
	case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
		c.needLeftTuples = true
		needRightTuples = true
	case descpb.LeftSemiJoin:
		// With LEFT SEMI join we only need to know whether the right input is
		// empty or not.
		c.numRightTuples = c.inputTwo.Next().Length()
		c.needLeftTuples = c.numRightTuples != 0
	case descpb.RightSemiJoin:
		// With RIGHT SEMI join we only need to know whether the left input is
		// empty or not.
		needRightTuples = c.readNextLeftBatch() != 0
	case descpb.LeftAntiJoin:
		// With LEFT ANTI join we only need to know whether the right input is
		// empty or not.
		c.numRightTuples = c.inputTwo.Next().Length()
		c.needLeftTuples = c.numRightTuples == 0
	case descpb.RightAntiJoin:
		// With RIGHT ANTI join we only need to know whether the left input is
		// empty or not.
		needRightTuples = c.readNextLeftBatch() == 0
	case descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		// With set-operation joins we only need the number of tuples from the
		// right input.
		c.needLeftTuples = true
		needOnlyNumRightTuples = true
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected join type %s", c.joinType.String()))
	}
	if needRightTuples || needOnlyNumRightTuples {
		for {
			batch := c.inputTwo.Next()
			if needRightTuples {
				c.rightTuples.Enqueue(ctx, batch)
			}
			if batch.Length() == 0 {
				break
			}
			c.numRightTuples += batch.Length()
		}
	}
}

// setupForBuilding prepares the cross joiner to build the output. This method
// must be called after the right input has been fully "processed" (which might
// mean it wasn't fully read, depending on the join type).
func (c *crossJoiner) setupForBuilding() {
	switch c.joinType {
	case descpb.LeftOuterJoin:
		c.isRightAllNulls = c.numRightTuples == 0
	case descpb.RightOuterJoin:
		c.isLeftAllNulls = c.readNextLeftBatch() == 0
	case descpb.FullOuterJoin:
		c.isLeftAllNulls = c.readNextLeftBatch() == 0
		c.isRightAllNulls = c.numRightTuples == 0
	case descpb.ExceptAllJoin:
		// For EXCEPT ALL joins we build # left tuples - # right tuples output
		// rows (if positive), so we have to discard first numRightTuples rows
		// from the left.
		for c.numRightTuples > 0 {
			leftBatch := c.inputOne.Next()
			c.builderState.left.currentBatch = leftBatch
			if leftBatch.Length() == 0 {
				break
			} else if leftBatch.Length() > c.numRightTuples {
				// The current left batch is the first one that contains tuples
				// without a "match".
				c.prepareForNextLeftBatch(leftBatch, c.numRightTuples, leftBatch.Length())
				break
			}
			c.numRightTuples -= leftBatch.Length()
		}
	}
	// In order for canEmit method to work in the unmatched cases, we "lie"
	// that there is a single tuple on the right side which results in the
	// builder method repeating the tuples only once, and that's exactly what we
	// want.
	if c.isRightAllNulls {
		c.numRightTuples = 1
	}
	c.setupLeftBuilder()
}

// willEmit returns the number of tuples the cross joiner will emit based on the
// current left batch. If the current left batch is exhausted, then a new left
// batch is fetched. If 0 is returned, then the cross joiner has fully emitted
// the output.
func (c *crossJoiner) willEmit() int {
	if c.needLeftTuples {
		if c.isLeftAllNulls {
			if c.isRightAllNulls {
				// This can happen only in FULL OUTER join when both inputs are
				// empty.
				return 0
			}
			// All tuples from the right are unmatched and will be emitted once.
			c.builderState.setup.rightNumRepeats = 1
			return c.numRightTuples - c.builderState.numEmittedCurLeftBatch
		}
		if c.builderState.left.currentBatch == nil || c.canEmit() == 0 {
			// Get the next left batch if we haven't fetched one yet or we
			// have fully built the output using the current left batch.
			if c.readNextLeftBatch() == 0 {
				return 0
			}
		}
		return c.canEmit()
	}
	switch c.joinType {
	case descpb.LeftSemiJoin, descpb.LeftAntiJoin:
		// We don't need the left tuples, and in case of LEFT SEMI/ANTI this
		// means that the right input was empty/non-empty, so the cross join
		// is empty.
		return 0
	case descpb.RightSemiJoin, descpb.RightAntiJoin:
		if c.numRightTuples == 0 {
			// For RIGHT SEMI, we didn't fetch any right tuples if the left
			// input was empty; for RIGHT ANTI - if the left input wasn't
			// empty. In both such cases the cross join is empty.
			return 0
		}
		return c.canEmit()
	default:
		colexecerror.InternalError(errors.AssertionFailedf(
			"unexpectedly don't need left tuples for %s", c.joinType,
		))
		// This code is unreachable, but the compiler cannot infer that.
		return 0
	}
}

// setAllNulls sets all tuples in vecs with indices in [0, length) range to
// null.
func setAllNulls(vecs []coldata.Vec, length int) {
	for i := range vecs {
		vecs[i].Nulls().SetNullRange(0 /* startIdx */, length)
	}
}

func (c *crossJoiner) Reset(ctx context.Context) {
	if r, ok := c.inputOne.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	if r, ok := c.inputTwo.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	c.crossJoinerBase.Reset(ctx)
	c.rightInputConsumed = false
	c.isLeftAllNulls = false
	c.isRightAllNulls = false
	c.done = false
}

func newCrossJoinerBase(
	unlimitedAllocator *colmem.Allocator,
	joinType descpb.JoinType,
	leftTypes, rightTypes []*types.T,
	memoryLimit int64,
	cfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAcc *mon.BoundAccount,
) *crossJoinerBase {
	base := &crossJoinerBase{
		joinType: joinType,
		left: cjState{
			unlimitedAllocator:    unlimitedAllocator,
			types:                 leftTypes,
			canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(leftTypes),
		},
		right: cjState{
			unlimitedAllocator:    unlimitedAllocator,
			types:                 rightTypes,
			canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(rightTypes),
		},
		rightTuples: colexecutils.NewRewindableSpillingQueue(
			&colexecutils.NewSpillingQueueArgs{
				UnlimitedAllocator: unlimitedAllocator,
				Types:              rightTypes,
				MemoryLimit:        memoryLimit,
				DiskQueueCfg:       cfg,
				FDSemaphore:        fdSemaphore,
				DiskAcc:            diskAcc,
			},
		),
	}
	if joinType.ShouldIncludeLeftColsInOutput() {
		base.builderState.rightColOffset = len(leftTypes)
	}
	return base
}

type crossJoinerBase struct {
	initHelper     colexecop.InitHelper
	joinType       descpb.JoinType
	left, right    cjState
	numRightTuples int
	rightTuples    *colexecutils.SpillingQueue
	needLeftTuples bool
	builderState   struct {
		setup       cjBuilderSetupState
		left, right cjMutableBuilderState

		// numEmittedCurLeftBatch tracks the number of joined rows returned
		// based on the current left batch. It is reset on every call to
		// prepareForNextLeftBatch.
		numEmittedCurLeftBatch int

		// numEmittedTotal tracks the number of rows that have been emitted
		// since the crossJoinerBase has been reset. It is only used in RIGHT
		// SEMI, RIGHT ANTI, and INTERSECT ALL joins.
		numEmittedTotal int

		// rightColOffset indicates the number of vectors in the output batch
		// that should be "skipped" when building from the right input.
		rightColOffset int
	}
	output coldata.Batch
}

func (b *crossJoinerBase) init(ctx context.Context) {
	b.initHelper.Init(ctx)
}

func (b *crossJoinerBase) setupLeftBuilder() {
	switch b.joinType {
	case descpb.LeftSemiJoin, descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		b.builderState.setup.leftNumRepeats = 1
	case descpb.LeftAntiJoin:
		// LEFT ANTI cross join emits all left tuples repeated once only if the
		// right input is empty.
		if b.numRightTuples == 0 {
			b.builderState.setup.leftNumRepeats = 1
		}
	default:
		b.builderState.setup.leftNumRepeats = b.numRightTuples
	}
}

// prepareForNextLeftBatch sets up the crossJoinerBase to build based on a new
// batch coming from the left input. Only rows with ordinals in
// [startIdx, endIdx) range will be used for the cross join.
func (b *crossJoinerBase) prepareForNextLeftBatch(batch coldata.Batch, startIdx, endIdx int) {
	b.builderState.numEmittedCurLeftBatch = 0
	b.builderState.left.currentBatch = batch
	b.builderState.left.curSrcStartIdx = startIdx
	b.builderState.left.numRepeatsIdx = 0
	b.builderState.right.numRepeatsIdx = 0

	if b.joinType == descpb.IntersectAllJoin {
		// Intersect all is special because we need to count how many tuples
		// from the right we have already used up.
		if b.builderState.numEmittedTotal+endIdx-startIdx >= b.numRightTuples {
			// The current left batch is the last one that contains tuples with
			// a "match".
			b.builderState.setup.leftSrcEndIdx = b.numRightTuples - b.builderState.numEmittedTotal + startIdx
		} else {
			// The current left batch is still emitted fully.
			b.builderState.setup.leftSrcEndIdx = endIdx
		}
	} else {
		b.builderState.setup.leftSrcEndIdx = endIdx
	}

	switch b.joinType {
	case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
		b.builderState.setup.rightNumRepeats = endIdx - startIdx
	case descpb.RightSemiJoin, descpb.RightAntiJoin:
		b.builderState.setup.rightNumRepeats = 1
	}
}

// canEmit returns the number of output rows that can still be emitted based on
// the current left batch. It supports only the case when both left and right
// inputs are not empty.
func (b *crossJoinerBase) canEmit() int {
	switch b.joinType {
	case descpb.LeftSemiJoin, descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		return b.builderState.setup.leftSrcEndIdx - b.builderState.left.curSrcStartIdx
	case descpb.LeftAntiJoin:
		if b.numRightTuples != 0 {
			return 0
		}
		return b.builderState.setup.leftSrcEndIdx - b.builderState.left.curSrcStartIdx
	case descpb.RightSemiJoin:
		// RIGHT SEMI cross join emits all right tuples repeated once iff the
		// left input is not empty.
		if b.builderState.setup.leftSrcEndIdx == b.builderState.left.curSrcStartIdx {
			return 0
		}
		return b.numRightTuples - b.builderState.numEmittedTotal
	case descpb.RightAntiJoin:
		// RIGHT ANTI cross join emits all right tuples repeated once iff the
		// left input is empty.
		if b.builderState.setup.leftSrcEndIdx != b.builderState.left.curSrcStartIdx {
			return 0
		}
		return b.numRightTuples - b.builderState.numEmittedTotal
	default:
		return b.builderState.setup.rightNumRepeats*b.numRightTuples - b.builderState.numEmittedCurLeftBatch
	}
}

func (b *crossJoinerBase) Reset(ctx context.Context) {
	if b.rightTuples != nil {
		b.rightTuples.Reset(ctx)
	}
	b.numRightTuples = 0
	b.builderState.left.reset()
	b.builderState.right.reset()
	b.builderState.numEmittedCurLeftBatch = 0
	b.builderState.numEmittedTotal = 0
}

func (b *crossJoinerBase) Close(ctx context.Context) error {
	if b.rightTuples != nil {
		return b.rightTuples.Close(ctx)
	}
	return nil
}

type cjState struct {
	unlimitedAllocator    *colmem.Allocator
	types                 []*types.T
	canonicalTypeFamilies []types.Family
}

type cjBuilderSetupState struct {
	// leftSrcEndIdx indicates the index of the tuple at which the building from
	// the left input should stop. For some join types this number is less than
	// the total number of left tuples (whereas for all join types if building
	// from the right input is necessary, all right tuples are used).
	leftSrcEndIdx int
	// leftNumRepeats and rightNumRepeats indicate the number of times a "group"
	// needs to be repeated (where "group" means a single tuple on the left side
	// and all tuples on the right side).
	leftNumRepeats, rightNumRepeats int
}

// cjMutableBuilderState contains the modifiable state of the builder from one
// side.
type cjMutableBuilderState struct {
	// currentBatch is the batch that we're building from at the moment.
	currentBatch coldata.Batch
	// curSrcStartIdx is the index of the tuple in currentBatch that we're
	// building from at the moment.
	curSrcStartIdx int
	// numRepeatsIdx tracks the number of times a "group" has already been
	// repeated.
	numRepeatsIdx int
}

func (s *cjMutableBuilderState) reset() {
	s.currentBatch = nil
	s.curSrcStartIdx = 0
	s.numRepeatsIdx = 0
}
