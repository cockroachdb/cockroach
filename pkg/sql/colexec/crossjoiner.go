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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// TODO(yuzefovich): use two separate unlimited allocators and give each
// spillingQueue a half of the memory limit.
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
			tuples:                newSpillingQueue(unlimitedAllocator, leftTypes, memoryLimit, cfg, fdSemaphore, diskAcc),
		},
		right: cjState{
			unlimitedAllocator:    unlimitedAllocator,
			types:                 rightTypes,
			canonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(rightTypes),
			tuples:                newRewindableSpillingQueue(unlimitedAllocator, rightTypes, memoryLimit, cfg, fdSemaphore, diskAcc),
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
	default:
		b.builderState.setup.leftNumRepeats = b.right.numTuples
	}
	switch b.joinType {
	case descpb.RightSemiJoin:
		b.builderState.setup.rightNumRepeats = 1
	default:
		b.builderState.setup.rightNumRepeats = b.left.numTuples
	}
}

// calculateOutputCount returns the total number of tuples that are emitted by
// the cross join given already initialized left and right side states.
func (b *crossJoinerBase) calculateOutputCount() int {
	switch b.joinType {
	case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
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
	case descpb.LeftAntiJoin, descpb.RightAntiJoin:
		return 0
	case descpb.IntersectAllJoin:
		if b.right.numTuples < b.left.numTuples {
			return b.right.numTuples
		}
		return b.left.numTuples
	case descpb.ExceptAllJoin:
		if b.right.numTuples < b.left.numTuples {
			return b.left.numTuples - b.right.numTuples
		}
		return 0
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
