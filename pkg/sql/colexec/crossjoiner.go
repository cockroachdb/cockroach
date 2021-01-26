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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
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
	left colexecbase.Operator,
	right colexecbase.Operator,
	leftTypes []*types.T,
	rightTypes []*types.T,
	diskAcc *mon.BoundAccount,
) colexecbase.Operator {
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
		twoInputNode:       newTwoInputNode(left, right),
		unlimitedAllocator: unlimitedAllocator,
		outputTypes:        joinType.MakeOutputTypes(leftTypes, rightTypes),
	}
}

type crossJoiner struct {
	*crossJoinerBase
	twoInputNode

	unlimitedAllocator   *colmem.Allocator
	inputsConsumed       bool
	outputTypes          []*types.T
	numTotalOutputTuples int
	numAlreadyEmitted    int
	// isLeftAllNulls and isRightAllNulls indicate whether the output vectors
	// corresponding to the left and right inputs, respectively, should consist
	// only of NULL values. This is the case when we have right or left,
	// respectively, unmatched tuples. Note that only one can be set to true.
	isLeftAllNulls, isRightAllNulls bool
}

var _ closableOperator = &crossJoiner{}
var _ ResettableOperator = &crossJoiner{}

func (c *crossJoiner) Init() {
	c.inputOne.Init()
	c.inputTwo.Init()
}

func (c *crossJoiner) Next(ctx context.Context) coldata.Batch {
	if !c.inputsConsumed {
		c.consumeInputs(ctx)
		c.setupForBuilding()
	}
	if c.numTotalOutputTuples == c.numAlreadyEmitted {
		if err := c.Close(ctx); err != nil {
			colexecerror.InternalError(err)
		}
		return coldata.ZeroBatch
	}
	willEmit := c.numTotalOutputTuples - c.numAlreadyEmitted
	if willEmit > coldata.BatchSize() {
		willEmit = coldata.BatchSize()
	}
	c.output, _ = c.unlimitedAllocator.ResetMaybeReallocate(c.outputTypes, c.output, willEmit)
	if c.joinType.ShouldIncludeLeftColsInOutput() {
		if c.isLeftAllNulls {
			setAllNulls(c.output.ColVecs()[:len(c.left.types)], willEmit)
		} else {
			c.buildFromLeftInput(ctx, 0 /* destStartIdx */)
		}
	}
	if c.joinType.ShouldIncludeRightColsInOutput() {
		if c.isRightAllNulls {
			setAllNulls(c.output.ColVecs()[c.builderState.rightColOffset:], willEmit)
		} else {
			c.buildFromRightInput(ctx, 0 /* destStartIdx */)
		}
	}
	c.output.SetLength(willEmit)
	c.numAlreadyEmitted += willEmit
	return c.output
}

// consumeInputs determines the kind of information the cross joiner needs from
// its inputs (in some cases, we don't need to buffer all input tuples) and
// consumes the inputs accordingly.
func (c *crossJoiner) consumeInputs(ctx context.Context) {
	c.inputsConsumed = true
	var needLeftTuples bool
	var needRightTuples, needOnlyNumRightTuples bool
	switch c.joinType {
	case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
		needLeftTuples = true
		needRightTuples = true
	case descpb.LeftSemiJoin:
		// With LEFT SEMI join we only need to know whether the right input is
		// empty or not.
		c.right.numTuples = c.inputTwo.Next(ctx).Length()
		needLeftTuples = c.right.numTuples != 0
	case descpb.RightSemiJoin:
		// With RIGHT SEMI join we only need to know whether the left input is
		// empty or not.
		c.left.numTuples = c.inputOne.Next(ctx).Length()
		needRightTuples = c.left.numTuples != 0
	case descpb.LeftAntiJoin:
		// With LEFT ANTI join we only need to know whether the right input is
		// empty or not.
		c.right.numTuples = c.inputTwo.Next(ctx).Length()
		needLeftTuples = c.right.numTuples == 0
	case descpb.RightAntiJoin:
		// With RIGHT ANTI join we only need to know whether the left input is
		// empty or not.
		c.left.numTuples = c.inputOne.Next(ctx).Length()
		needRightTuples = c.left.numTuples == 0
	case descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		// With set-operation joins we only need the number of tuples from the
		// right input.
		needLeftTuples = true
		needOnlyNumRightTuples = true
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected join type %s", c.joinType.String()))
	}
	if needRightTuples && needOnlyNumRightTuples {
		colexecerror.InternalError(errors.AssertionFailedf("both needRightTuples and needOnlyNumRightTuples are true"))
	}

	if needLeftTuples {
		for {
			batch := c.inputOne.Next(ctx)
			if err := c.left.tuples.enqueue(ctx, batch); err != nil {
				colexecerror.InternalError(err)
			}
			if batch.Length() == 0 {
				break
			}
			c.left.numTuples += batch.Length()
		}
	}
	if needRightTuples {
		for {
			batch := c.inputTwo.Next(ctx)
			if err := c.right.tuples.enqueue(ctx, batch); err != nil {
				colexecerror.InternalError(err)
			}
			if batch.Length() == 0 {
				break
			}
			c.right.numTuples += batch.Length()
		}
	}
	if needOnlyNumRightTuples {
		for {
			batch := c.inputTwo.Next(ctx)
			if batch.Length() == 0 {
				break
			}
			c.right.numTuples += batch.Length()
		}
	}
}

func (c *crossJoiner) setupForBuilding() {
	c.numTotalOutputTuples = c.calculateOutputCount()

	switch c.joinType {
	case descpb.LeftOuterJoin:
		c.isRightAllNulls = c.right.numTuples == 0
	case descpb.RightOuterJoin:
		c.isLeftAllNulls = c.left.numTuples == 0
	case descpb.FullOuterJoin:
		c.isLeftAllNulls = c.left.numTuples == 0
		c.isRightAllNulls = c.right.numTuples == 0
	}
	// In order for buildFrom*Input methods to work in the unmatched cases, we
	// "lie" that there is a single tuple on the opposite side which results in
	// the builder methods repeating the tuples only once, and that's exactly
	// what we want.
	if c.isLeftAllNulls {
		c.left.numTuples = 1
	}
	if c.isRightAllNulls {
		c.right.numTuples = 1
	}
	c.setupBuilder()
	if c.isLeftAllNulls {
		c.left.numTuples = 0
	}
	if c.isRightAllNulls {
		c.right.numTuples = 0
	}
}

// setAllNulls sets all tuples in vecs with indices in [0, length) range to
// null.
func setAllNulls(vecs []coldata.Vec, length int) {
	for i := range vecs {
		vecs[i].Nulls().SetNullRange(0 /* startIdx */, length)
	}
}

func (c *crossJoiner) reset(ctx context.Context) {
	if r, ok := c.inputOne.(resetter); ok {
		r.reset(ctx)
	}
	if r, ok := c.inputTwo.(resetter); ok {
		r.reset(ctx)
	}
	c.crossJoinerBase.reset(ctx)
	c.inputsConsumed = false
	c.numTotalOutputTuples = 0
	c.numAlreadyEmitted = 0
	c.isLeftAllNulls = false
	c.isRightAllNulls = false
}

// TODO(yuzefovich): use two separate unlimited allocators giving the right side
// larger limit (since it might need to be read multiple times).
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
			tuples: newSpillingQueue(
				&NewSpillingQueueArgs{
					UnlimitedAllocator: unlimitedAllocator,
					Types:              leftTypes,
					MemoryLimit:        memoryLimit,
					DiskQueueCfg:       cfg,
					FDSemaphore:        fdSemaphore,
					DiskAcc:            diskAcc,
				},
			),
		},
		right: cjState{
			unlimitedAllocator:    unlimitedAllocator,
			types:                 rightTypes,
			canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(rightTypes),
			tuples: newRewindableSpillingQueue(
				&NewSpillingQueueArgs{
					UnlimitedAllocator: unlimitedAllocator,
					Types:              rightTypes,
					MemoryLimit:        memoryLimit,
					DiskQueueCfg:       cfg,
					FDSemaphore:        fdSemaphore,
					DiskAcc:            diskAcc,
				},
			),
		},
	}
	if joinType.ShouldIncludeLeftColsInOutput() {
		base.builderState.rightColOffset = len(leftTypes)
	}
	return base
}

type crossJoinerBase struct {
	joinType     descpb.JoinType
	left, right  cjState
	builderState struct {
		setup       cjBuilderSetupState
		left, right cjMutableBuilderState
		// rightColOffset indicates the number of vectors in the output batch
		// that should be "skipped" when building from the right input.
		rightColOffset int
	}
	output coldata.Batch
}

func (b *crossJoinerBase) setupBuilder() {
	switch b.joinType {
	case descpb.IntersectAllJoin:
		// For INTERSECT ALL joins we build min(left.numTuples, right.numTuples)
		// tuples.
		if b.left.numTuples < b.right.numTuples {
			b.builderState.setup.leftSrcEndIdx = b.left.numTuples
		} else {
			b.builderState.setup.leftSrcEndIdx = b.right.numTuples
		}
	case descpb.ExceptAllJoin:
		// For EXCEPT ALL joins we build left.numTuples-right.numTuples tuples
		// (if positive).
		if b.left.numTuples > b.right.numTuples {
			b.builderState.setup.leftSrcEndIdx = b.left.numTuples - b.right.numTuples
		}
	default:
		b.builderState.setup.leftSrcEndIdx = b.left.numTuples
	}
	switch b.joinType {
	case descpb.LeftSemiJoin, descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		b.builderState.setup.leftNumRepeats = 1
	case descpb.LeftAntiJoin:
		// LEFT ANTI cross join emits all left tuples repeated once only if the
		// right input is empty.
		if b.right.numTuples == 0 {
			b.builderState.setup.leftNumRepeats = 1
		}
	default:
		b.builderState.setup.leftNumRepeats = b.right.numTuples
	}
	switch b.joinType {
	case descpb.RightSemiJoin:
		b.builderState.setup.rightNumRepeats = 1
	case descpb.RightAntiJoin:
		// RIGHT ANTI cross join emits all right tuples repeated once only if
		// the left input is empty.
		if b.left.numTuples == 0 {
			b.builderState.setup.rightNumRepeats = 1
		}
	default:
		b.builderState.setup.rightNumRepeats = b.left.numTuples
	}
}

// calculateOutputCount returns the total number of tuples that are emitted by
// the cross join given already initialized left and right side states.
func (b *crossJoinerBase) calculateOutputCount() int {
	switch b.joinType {
	case descpb.InnerJoin:
		return b.left.numTuples * b.right.numTuples
	case descpb.LeftOuterJoin:
		if b.right.numTuples == 0 {
			return b.left.numTuples
		}
		return b.left.numTuples * b.right.numTuples
	case descpb.RightOuterJoin:
		if b.left.numTuples == 0 {
			return b.right.numTuples
		}
		return b.left.numTuples * b.right.numTuples
	case descpb.FullOuterJoin:
		if b.left.numTuples == 0 || b.right.numTuples == 0 {
			return b.left.numTuples + b.right.numTuples
		}
		return b.left.numTuples * b.right.numTuples
	case descpb.LeftSemiJoin:
		if b.right.numTuples == 0 {
			return 0
		}
		return b.left.numTuples
	case descpb.RightSemiJoin:
		if b.left.numTuples == 0 {
			return 0
		}
		return b.right.numTuples
	case descpb.LeftAntiJoin:
		if b.right.numTuples != 0 {
			return 0
		}
		return b.left.numTuples
	case descpb.RightAntiJoin:
		if b.left.numTuples != 0 {
			return 0
		}
		return b.right.numTuples
	case descpb.IntersectAllJoin:
		if b.right.numTuples < b.left.numTuples {
			return b.right.numTuples
		}
		return b.left.numTuples
	case descpb.ExceptAllJoin:
		if b.right.numTuples > b.left.numTuples {
			return 0
		}
		return b.left.numTuples - b.right.numTuples
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected join type %s", b.joinType.String()))
		// Unreachable code.
		return 0
	}
}

func (b *crossJoinerBase) reset(ctx context.Context) {
	if b.left.tuples != nil {
		b.left.tuples.reset(ctx)
	}
	if b.right.tuples != nil {
		b.right.tuples.reset(ctx)
	}
	b.left.numTuples = 0
	b.right.numTuples = 0
	b.builderState.left.reset()
	b.builderState.right.reset()
}

func (b *crossJoinerBase) Close(ctx context.Context) error {
	var lastErr error
	if b.left.tuples != nil {
		lastErr = b.left.tuples.close(ctx)
	}
	if b.right.tuples != nil {
		if err := b.right.tuples.close(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

type cjState struct {
	unlimitedAllocator    *colmem.Allocator
	types                 []*types.T
	canonicalTypeFamilies []types.Family
	tuples                *spillingQueue
	numTuples             int
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
	// setOpLeftSrcIdx tracks the current tuple's index from the left input for
	// set operation joins. INTERSECT ALL and EXCEPT ALL joins are special
	// because they need to build the output partially (namely, for exactly
	// leftSrcEndIdx number of tuples which could span multiple batches).
	setOpLeftSrcIdx int
}

func (s *cjMutableBuilderState) reset() {
	s.currentBatch = nil
	s.curSrcStartIdx = 0
	s.numRepeatsIdx = 0
	s.setOpLeftSrcIdx = 0
}
