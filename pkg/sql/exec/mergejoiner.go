// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	// unmatched indicates that the row in the group does not have matching rows
	// from the other side (i.e. other side's group will be a null group).
	// NOTE: at the moment, the assumption is that such group will consist of a
	// single row.
	// TODO(yuzefovich): update the logic if the assumption ever changes.
	unmatched bool
}

// mjBuilderState contains all the state required to execute the build phase.
type mjBuilderState struct {
	// Fields to hold the input sources batches from which to build the cross
	// products.
	lBatch coldata.Batch
	rBatch coldata.Batch

	// Fields to identify the groups in the input sources.
	lGroups []group
	rGroups []group

	// outCount keeps record of the current number of rows in the output.
	outCount uint16
	// outFinished is used to determine if the builder is finished outputting
	// the groups from input.
	outFinished bool

	// Cross product materialization state.
	left  mjBuilderCrossProductState
	right mjBuilderCrossProductState
}

// mjBuilderCrossProductState is used to keep track of builder state within the
// loops to materialize the cross product. Useful for picking up where we left
// off.
type mjBuilderCrossProductState struct {
	colIdx         int
	groupsIdx      int
	curSrcStartIdx int
	numRepeatsIdx  int
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

	// Local buffer for the last left and right groups which is used when the
	// group ends with a batch and the group on each side needs to be saved to
	// state in order to be able to continue it in the next batch.
	lBufferedGroup *mjBufferedGroup
	rBufferedGroup *mjBufferedGroup
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
)

type mergeJoinInput struct {
	// eqCols specify the indices of the source table equality columns during the
	// merge join.
	eqCols []uint32

	// outCols specify the indices of the columns that should be outputted by the
	// merge joiner.
	outCols []uint32

	// directions specifies the ordering direction of each column. Note that each
	// direction corresponds to an equality column at the same location, i.e. the
	// direction of eqCols[x] is encoded at directions[x], or
	// len(eqCols) == len(directions).
	directions []distsqlpb.Ordering_Column_Direction

	// sourceTypes specify the types of the input columns of the source table for
	// the merge joiner.
	sourceTypes []types.T

	// The distincter is used in the finishGroup phase, and is used only to
	// determine where the current group ends, in the case that the group ended
	// with a batch.
	distincterInput feedOperator
	distincter      Operator
	distinctOutput  []bool

	// source specifies the input operator to the merge join.
	source Operator
}

// feedOperator is used to feed the distincter with input by manually setting
// the next batch.
type feedOperator struct {
	batch coldata.Batch
}

func (feedOperator) Init() {}

func (o *feedOperator) Next(context.Context) coldata.Batch {
	return o.batch
}

var _ Operator = &feedOperator{}

// mergeJoinOp is an operator that implements sort-merge join. It performs a
// merge on the left and right input sources, based on the equality columns,
// assuming both inputs are in sorted order.

// The merge join operator uses a probe and build approach to generate the
// join. What this means is that instead of going through and expanding the
// cross product row by row, the operator performs two passes.
// The first pass generates a list of groups of matching rows based on the
// equality columns (where a "group" represents a contiguous set of rows that
// match on the equality columns).
// The second pass is where the groups and their associated cross products are
// materialized into the full output.

// TODO(georgeutsin): Add outer joins functionality and templating to support
// different equality types.

// Two buffers are used, one for the group on the left table and one for the
// group on the right table. These buffers are only used if the group ends with
// a batch, to make sure that we don't miss any cross product entries while
// expanding the groups (leftGroups and rightGroups) when a group spans
// multiple batches.
type mergeJoinOp struct {
	joinType sqlbase.JoinType
	left     mergeJoinInput
	right    mergeJoinInput

	// Output buffer definition.
	output            coldata.Batch
	needToResetOutput bool
	outputBatchSize   uint16
	// outputReady is a flag to indicate that merge joiner is ready to emit an
	// output batch.
	outputReady bool

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer

	state        mjState
	proberState  mjProberState
	builderState mjBuilderState
}

var _ Operator = &mergeJoinOp{}

// NewMergeJoinOp returns a new merge join operator with the given spec.
func NewMergeJoinOp(
	joinType sqlbase.JoinType,
	left Operator,
	right Operator,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []types.T,
	rightTypes []types.T,
	leftOrdering []distsqlpb.Ordering_Column,
	rightOrdering []distsqlpb.Ordering_Column,
) (Operator, error) {
	lEqCols := make([]uint32, len(leftOrdering))
	lDirections := make([]distsqlpb.Ordering_Column_Direction, len(leftOrdering))
	for i, c := range leftOrdering {
		lEqCols[i] = c.ColIdx
		lDirections[i] = c.Direction
	}

	rEqCols := make([]uint32, len(rightOrdering))
	rDirections := make([]distsqlpb.Ordering_Column_Direction, len(rightOrdering))
	for i, c := range rightOrdering {
		rEqCols[i] = c.ColIdx
		rDirections[i] = c.Direction
	}

	c := &mergeJoinOp{
		joinType: joinType,
		left: mergeJoinInput{
			source:      left,
			outCols:     leftOutCols,
			sourceTypes: leftTypes,
			eqCols:      lEqCols,
			directions:  lDirections,
		},
		right: mergeJoinInput{
			source:      right,
			outCols:     rightOutCols,
			sourceTypes: rightTypes,
			eqCols:      rEqCols,
			directions:  rDirections,
		},
	}

	var err error
	c.left.distincterInput = feedOperator{}
	c.left.distincter, c.left.distinctOutput, err = OrderedDistinctColsToOperators(
		&c.left.distincterInput, lEqCols, leftTypes)
	if err != nil {
		return nil, err
	}
	c.right.distincterInput = feedOperator{}
	c.right.distincter, c.right.distinctOutput, err = OrderedDistinctColsToOperators(
		&c.right.distincterInput, rEqCols, rightTypes)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (o *mergeJoinOp) Init() {
	o.initWithBatchSize(coldata.BatchSize)
}

func (o *mergeJoinOp) initWithBatchSize(outBatchSize uint16) {
	outColTypes := make([]types.T, len(o.left.sourceTypes)+len(o.right.sourceTypes))
	copy(outColTypes, o.left.sourceTypes)
	copy(outColTypes[len(o.left.sourceTypes):], o.right.sourceTypes)

	o.output = coldata.NewMemBatchWithSize(outColTypes, int(outBatchSize))
	o.left.source.Init()
	o.right.source.Init()
	o.outputBatchSize = outBatchSize
	// If there are no output columns, then the operator is for a COUNT query,
	// in which case we treat the output batch size as the max uint16.
	if o.output.Width() == 0 {
		o.outputBatchSize = 1<<16 - 1
	}

	o.proberState.lBufferedGroup = newMJBufferedGroup(o.left.sourceTypes)
	o.proberState.rBufferedGroup = newMJBufferedGroup(o.right.sourceTypes)

	o.builderState.lGroups = make([]group, 1)
	o.builderState.rGroups = make([]group, 1)

	o.groups = makeGroupsBuffer(coldata.BatchSize)
	o.resetBuilderCrossProductState()
}

// Const declarations for the merge joiner cross product (MJCP) zero state.
const (
	zeroMJCPcolIdx    = 0
	zeroMJCPgroupsIdx = 0
	// The sentinel value for curSrcStartIdx is -1, as this:
	// a) indicates that a src has not been started
	// b) panics if the sentinel isn't checked
	zeroMJCPcurSrcStartIdx = -1
	zeroMJCPnumRepeatsIdx  = 0
)

// Package level struct for easy access to the MJCP zero state.
var zeroMJBuilderState = mjBuilderCrossProductState{
	colIdx:         zeroMJCPcolIdx,
	groupsIdx:      zeroMJCPgroupsIdx,
	curSrcStartIdx: zeroMJCPcurSrcStartIdx,
	numRepeatsIdx:  zeroMJCPnumRepeatsIdx,
}

func (o *mergeJoinOp) resetBuilderCrossProductState() {
	o.builderState.left.reset()
	o.builderState.right.reset()
}

func (s *mjBuilderCrossProductState) reset() {
	s.setBuilderColumnState(zeroMJBuilderState)
	s.colIdx = zeroMJCPcolIdx
}

func (s *mjBuilderCrossProductState) setBuilderColumnState(target mjBuilderCrossProductState) {
	s.groupsIdx = target.groupsIdx
	s.curSrcStartIdx = target.curSrcStartIdx
	s.numRepeatsIdx = target.numRepeatsIdx
}

// calculateOutputCount uses the toBuild field of each group and the output
// batch size to determine the output count. Note that as soon as a group is
// materialized partially or fully to output, its toBuild field is updated
// accordingly.
func (o *mergeJoinOp) calculateOutputCount(groups []group) uint16 {
	count := int(o.builderState.outCount)

	for i := 0; i < len(groups); i++ {
		count += groups[i].toBuild
		groups[i].toBuild = 0
		if count > int(o.outputBatchSize) {
			groups[i].toBuild = count - int(o.outputBatchSize)
			count = int(o.outputBatchSize)
			return uint16(count)
		}
	}
	o.builderState.outFinished = true
	return uint16(count)
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
func (o *mergeJoinOp) completeBufferedGroup(
	ctx context.Context, input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) (_ coldata.Batch, idx int, batchLength int) {
	batchLength = int(batch.Length())
	if o.isBufferedGroupFinished(input, batch, rowIdx) {
		return batch, rowIdx, batchLength
	}

	isBufferedGroupComplete := false
	input.distincter.(resetter).reset()
	// Ignore the first row of the distincter in the first pass since we already
	// know that we are in the same group and, thus, the row is not distinct,
	// regardless of what the distincter outputs.
	loopStartIndex := 1
	var sel []uint16
	for !isBufferedGroupComplete {
		// Note that we're not resetting the distincter on every loop iteration
		// because if we're doing the second, third, etc, iteration, then all the
		// previous iterations had only the matching tuples to the buffered group,
		// so the distincter - in a sense - compares the incoming tuples to the
		// first tuple of the first iteration (which we know is the same group).
		input.distincterInput.batch = batch
		input.distincter.Next(ctx)

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
		copy(input.distinctOutput, zeroBoolColumn)
		loopStartIndex = 0

		// Buffer all the tuples that are part of the buffered group.
		o.appendToBufferedGroup(input, batch, sel, rowIdx, groupLength)
		rowIdx += groupLength

		if !isBufferedGroupComplete {
			// The buffered group is still not complete which means that we have
			// just appended all the tuples from batch to it, so we need to get a
			// fresh batch from the input.
			rowIdx, batch = 0, input.source.Next(ctx)
			batchLength = int(batch.Length())
			if batchLength == 0 {
				// The input has been exhausted, so the buffered group is now complete.
				isBufferedGroupComplete = true
			}
		}
	}

	return batch, rowIdx, batchLength
}

// appendToBufferedGroup appends all the tuples from batch that are part of the
// same group as the ones in the buffered group that corresponds to the input
// source. This needs to happen when a group starts at the end of an input
// batch and can continue into the following batches.
func (o *mergeJoinOp) appendToBufferedGroup(
	input *mergeJoinInput, batch coldata.Batch, sel []uint16, groupStartIdx int, groupLength int,
) {
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
	}
	destStartIdx := bufferedGroup.length
	groupEndIdx := groupStartIdx + groupLength
	for cIdx, cType := range input.sourceTypes {
		bufferedGroup.ColVec(cIdx).Append(
			coldata.AppendArgs{
				ColType:     cType,
				Src:         batch.ColVec(cIdx),
				Sel:         sel,
				DestIdx:     uint64(destStartIdx),
				SrcStartIdx: uint16(groupStartIdx),
				SrcEndIdx:   uint16(groupEndIdx),
			},
		)
		if sel != nil {
			bufferedGroup.ColVec(cIdx).Nulls().ExtendWithSel(
				batch.ColVec(cIdx).Nulls(),
				uint64(destStartIdx),
				uint16(groupStartIdx),
				uint16(groupLength),
				sel,
			)
		} else {
			bufferedGroup.ColVec(cIdx).Nulls().Extend(
				batch.ColVec(cIdx).Nulls(),
				uint64(destStartIdx),
				uint16(groupStartIdx),
				uint16(groupLength),
			)
		}
	}

	// We've added groupLength number of tuples to bufferedGroup, so we need to
	// adjust its length.
	bufferedGroup.length += uint64(groupLength)
}

// setBuilderSourceToBatch sets the builder state to use groups from the
// circular group buffer and the batches from input. This happens when we have
// groups that are fully contained within a single input batch from each of the
// sources.
func (o *mergeJoinOp) setBuilderSourceToBatch() {
	o.builderState.lGroups, o.builderState.rGroups = o.groups.getGroups()
	o.builderState.lBatch = o.proberState.lBatch
	o.builderState.rBatch = o.proberState.rBatch
}

// probe is where we generate the groups slices that are used in the build
// phase. We do this by first assuming that every row in both batches
// contributes to the cross product. Then, with every equality column, we
// filter out the rows that don't contribute to the cross product (i.e. they
// don't have a matching row on the other side in the case of an inner join),
// and set the correct cardinality.
// Note that in this phase, we do this for every group, except the last group
// in the batch.
func (o *mergeJoinOp) probe() {
	o.groups.reset(o.proberState.lIdx, o.proberState.lLength, o.proberState.rIdx, o.proberState.rLength)
	lSel := o.proberState.lBatch.Selection()
	rSel := o.proberState.rBatch.Selection()
	if lSel != nil {
		if rSel != nil {
			o.probeBodyLSeltrueRSeltrue()
		} else {
			o.probeBodyLSeltrueRSelfalse()
		}
	} else {
		if rSel != nil {
			o.probeBodyLSelfalseRSeltrue()
		} else {
			o.probeBodyLSelfalseRSelfalse()
		}
	}
}

// finishProbe completes the buffered groups on both sides of the input.
func (o *mergeJoinOp) finishProbe(ctx context.Context) {
	o.proberState.lBatch, o.proberState.lIdx, o.proberState.lLength = o.completeBufferedGroup(
		ctx,
		&o.left,
		o.proberState.lBatch,
		o.proberState.lIdx,
	)
	o.proberState.rBatch, o.proberState.rIdx, o.proberState.rLength = o.completeBufferedGroup(
		ctx,
		&o.right,
		o.proberState.rBatch,
		o.proberState.rIdx,
	)
}

// setBuilderSourceToBufferedGroup sets up the builder state to use the
// buffered group.
func (o *mergeJoinOp) setBuilderSourceToBufferedGroup() {
	lGroupEndIdx := int(o.proberState.lBufferedGroup.length)
	rGroupEndIdx := int(o.proberState.rBufferedGroup.length)
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.lGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   lGroupEndIdx,
		numRepeats:  rGroupEndIdx,
		toBuild:     lGroupEndIdx * rGroupEndIdx,
	}
	o.builderState.rGroups = o.builderState.rGroups[:1]
	o.builderState.rGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   rGroupEndIdx,
		numRepeats:  lGroupEndIdx,
		toBuild:     rGroupEndIdx * lGroupEndIdx,
	}

	o.builderState.lBatch = o.proberState.lBufferedGroup
	o.builderState.rBatch = o.proberState.rBufferedGroup

	// We cannot yet reset the buffered groups because the builder will be taking
	// input from them. The actual reset will take place on the next call to
	// initProberState().
	o.proberState.lBufferedGroup.needToReset = true
	o.proberState.rBufferedGroup.needToReset = true
}

// exhaustLeftSourceForLeftOuter sets up the builder state for emitting
// remaining tuples on the left with nulls for the right side of output. It
// should only be called once the right source has been exhausted, and if
// we're doing LEFT OUTER join.
func (o *mergeJoinOp) exhaustLeftSourceForLeftOuter() {
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.lGroups[0] = group{
		rowStartIdx: o.proberState.lIdx,
		rowEndIdx:   o.proberState.lLength,
		numRepeats:  1,
		toBuild:     o.proberState.lLength - o.proberState.lIdx,
		unmatched:   true,
	}
	o.builderState.rGroups = o.builderState.rGroups[:1]
	o.builderState.rGroups[0] = group{
		rowStartIdx: o.proberState.lIdx,
		rowEndIdx:   o.proberState.lLength,
		numRepeats:  1,
		toBuild:     o.proberState.lLength - o.proberState.lIdx,
		nullGroup:   true,
	}
	o.builderState.lBatch = o.proberState.lBatch
	o.builderState.rBatch = o.proberState.rBatch

	o.proberState.lIdx = o.proberState.lLength
}

// build creates the cross product, and writes it to the output member.
func (o *mergeJoinOp) build() {
	if o.output.Width() != 0 {
		outStartIdx := o.builderState.outCount
		o.buildLeftGroups(o.builderState.lGroups, 0 /* colOffset */, &o.left, o.builderState.lBatch, outStartIdx)
		o.buildRightGroups(o.builderState.rGroups, len(o.left.sourceTypes), &o.right, o.builderState.rBatch, outStartIdx)
	}
	o.builderState.outCount = o.calculateOutputCount(o.builderState.lGroups)
}

// initProberState sets the batches, lengths, and current indices to the right
// locations given the last iteration of the operator.
func (o *mergeJoinOp) initProberState(ctx context.Context) {
	// If this is the first batch or we're done with the current batch, get the
	// next batch.
	if o.proberState.lBatch == nil || (o.proberState.lLength != 0 && o.proberState.lIdx == o.proberState.lLength) {
		o.proberState.lIdx, o.proberState.lBatch = 0, o.left.source.Next(ctx)
		o.proberState.lLength = int(o.proberState.lBatch.Length())
	}
	if o.proberState.rBatch == nil || (o.proberState.rLength != 0 && o.proberState.rIdx == o.proberState.rLength) {
		o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next(ctx)
		o.proberState.rLength = int(o.proberState.rBatch.Length())
	}
	if o.proberState.lBufferedGroup.needToReset {
		o.proberState.lBufferedGroup.reset()
	}
	if o.proberState.rBufferedGroup.needToReset {
		o.proberState.rBufferedGroup.reset()
	}
}

// nonEmptyBufferedGroup returns true if there is a buffered group that needs
// to be finished.
func (o *mergeJoinOp) nonEmptyBufferedGroup() bool {
	return o.proberState.lBufferedGroup.length > 0 || o.proberState.rBufferedGroup.length > 0
}

// sourceFinished returns true if either of input sources has no more rows.
func (o *mergeJoinOp) sourceFinished() bool {
	// TODO (georgeutsin): update this logic to be able to support joins other than INNER.
	return o.proberState.lLength == 0 || o.proberState.rLength == 0
}

func (o *mergeJoinOp) Next(ctx context.Context) coldata.Batch {
	for {
		switch o.state {
		case mjEntry:
			if o.needToResetOutput {
				o.needToResetOutput = false
				for _, vec := range o.output.ColVecs() {
					// We only need to explicitly reset nulls since the values will be
					// copied over and the correct length will be set.
					vec.Nulls().UnsetNulls()
				}
			}
			o.initProberState(ctx)

			if o.nonEmptyBufferedGroup() {
				o.state = mjFinishBufferedGroup
				break
			}

			if o.sourceFinished() {
				o.state = mjSourceFinished
				break
			}

			o.state = mjProbe
		case mjSourceFinished:
			if o.joinType == sqlbase.JoinType_LEFT_OUTER {
				// At least one of the sources is finished. If it was the right one,
				// then we need to emit remaining tuples from the left source with
				// nulls corresponding to the right one. But if the left source is
				// finished, then there is nothing left to do.
				if o.proberState.lIdx < o.proberState.lLength {
					o.exhaustLeftSourceForLeftOuter()
					// We do not set outputReady here to true because we want to put as
					// many unmatched tuples from the left into the output batch. Once
					// outCount reaches the desired output batch size, the output will be
					// returned.
				} else {
					o.outputReady = true
				}
			} else {
				o.setBuilderSourceToBufferedGroup()
				o.outputReady = true
			}
			o.state = mjBuild
		case mjFinishBufferedGroup:
			o.finishProbe(ctx)
			o.setBuilderSourceToBufferedGroup()
			o.state = mjBuild
		case mjProbe:
			o.probe()
			o.setBuilderSourceToBatch()
			o.state = mjBuild
		case mjBuild:
			o.build()

			if o.builderState.outFinished {
				o.state = mjEntry
				o.builderState.outFinished = false
			}

			if o.outputReady || o.builderState.outCount == o.outputBatchSize {
				o.output.SetSelection(false)
				o.output.SetLength(o.builderState.outCount)
				// Reset builder out count.
				o.builderState.outCount = uint16(0)
				o.needToResetOutput = true
				o.outputReady = false
				return o.output
			}
		default:
			panic(fmt.Sprintf("unexpected merge joiner state in Next: %v", o.state))
		}
	}
}
