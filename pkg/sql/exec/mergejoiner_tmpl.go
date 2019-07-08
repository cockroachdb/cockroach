// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for mergejoiner.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the the second input == the third input.
func _ASSIGN_EQ(_, _, _ interface{}) uint64 {
	panic("")
}

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the the second input < the third input.
func _ASSIGN_LT(_, _, _ interface{}) uint64 {
	panic("")
}

// _L_SEL_IND is the template type variable for the loop variable that
// is either curLIdx or lSel[curLIdx] depending on whether we're in a
// selection or not.
const _L_SEL_IND = 0

// _R_SEL_IND is the template type variable for the loop variable that
// is either curRIdx or rSel[curRIdx] depending on whether we're in a
// selection or not.

const _R_SEL_IND = 0

// _SEL_ARG is used in place of the string "$sel", since that isn't valid go
// code.
const _SEL_ARG = 0

// _JOIN_TYPE is used in place of the string "$joinType", since that isn't
// valid go code.
const _JOIN_TYPE = 0

// _MJ_OVERLOAD is used in place of the string "$mjOverload", since that isn't
// valid go code.
const _MJ_OVERLOAD = 0

// */}}

// {{ range $joinType := .JoinTypes }}
type mergeJoin_JOIN_TYPE_STRINGOp struct {
	*mergeJoinBase
}

// {{ end }}

// {{/*
// This code snippet is the "meat" of the probing phase.
func _PROBE_SWITCH(
	joinType joinTypeInfo, sel selPermutation, lHasNulls bool, rHasNulls bool, asc bool,
) { // */}}
	// {{define "probeSwitch"}}
	// {{ $sel := $.Sel }}
	// {{ $joinType := $.JoinType }}
	// {{ $mjOverloads := $.Global.MJOverloads }}
	switch colType {
	// {{range $mjOverload := $.Global.MJOverloads }}
	case _TYPES_T:
		lKeys := lVec._TemplateType()
		rKeys := rVec._TemplateType()
		var lGroup, rGroup group
		for o.groups.nextGroupInCol(&lGroup, &rGroup) {
			curLIdx := lGroup.rowStartIdx
			curRIdx := rGroup.rowStartIdx
			curLLength := lGroup.rowEndIdx
			curRLength := rGroup.rowEndIdx
			areGroupsProcessed := false
			_LEFT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE)
			// Expand or filter each group based on the current equality column.
			for curLIdx < curLLength && curRIdx < curRLength && !areGroupsProcessed {
				// TODO(georgeutsin): change null check logic for non INNER joins.
				// {{ if $.LNull }}
				if lVec.Nulls().NullAt64(uint64(_L_SEL_IND)) {
					_NULL_FROM_LEFT_SWITCH(_JOIN_TYPE)
					curLIdx++
					continue
				}
				// {{ end }}
				// {{ if $.RNull }}
				if rVec.Nulls().NullAt64(uint64(_R_SEL_IND)) {
					curRIdx++
					continue
				}
				// {{ end }}

				lVal := lKeys[_L_SEL_IND]
				rVal := rKeys[_R_SEL_IND]

				var match bool
				_ASSIGN_EQ("match", "lVal", "rVal")
				if match {
					// Find the length of the groups on each side.
					lGroupLength, rGroupLength := 1, 1
					lComplete, rComplete := false, false
					beginLIdx, beginRIdx := curLIdx, curRIdx

					// Find the length of the group on the left.
					if curLLength == 0 {
						lGroupLength, lComplete = 0, true
					} else {
						curLIdx++
						for curLIdx < curLLength {
							// TODO(georgeutsin): change null check logic for non INNER joins.
							// {{ if $.LNull }}
							if lVec.Nulls().NullAt64(uint64(_L_SEL_IND)) {
								lComplete = true
								break
							}
							// {{ end }}
							newLVal := lKeys[_L_SEL_IND]
							_ASSIGN_EQ("match", "newLVal", "lVal")
							if !match {
								lComplete = true
								break
							}
							lGroupLength++
							curLIdx++
						}
					}

					// Find the length of the group on the right.
					if curRLength == 0 {
						rGroupLength, rComplete = 0, true
					} else {
						curRIdx++
						for curRIdx < curRLength {
							// TODO(georgeutsin): change null check logic for non INNER joins.
							// {{ if $.RNull }}
							if rVec.Nulls().NullAt64(uint64(_R_SEL_IND)) {
								rComplete = true
								break
							}
							// {{ end }}
							newRVal := rKeys[_R_SEL_IND]
							_ASSIGN_EQ("match", "newRVal", "rVal")
							if !match {
								rComplete = true
								break
							}
							rGroupLength++
							curRIdx++
						}
					}

					// Last equality column and either group is incomplete. Save state
					// and have it handled in the next iteration.
					if eqColIdx == len(o.left.eqCols)-1 && (!lComplete || !rComplete) {
						o.appendToBufferedGroup(&o.left, o.proberState.lBatch, lSel, beginLIdx, lGroupLength)
						o.proberState.lIdx = lGroupLength + beginLIdx
						o.appendToBufferedGroup(&o.right, o.proberState.rBatch, rSel, beginRIdx, rGroupLength)
						o.proberState.rIdx = rGroupLength + beginRIdx

						o.groups.finishedCol()
						break EqLoop
					}

					// Neither group ends with the batch, so add the group to the
					// circular buffer.
					o.groups.addGroupsToNextCol(beginLIdx, lGroupLength, beginRIdx, rGroupLength)
				} else { // mismatch
					var incrementLeft bool
					// {{ if $.Asc }}
					_ASSIGN_LT("incrementLeft", "lVal", "rVal")
					// {{ else }}
					_ASSIGN_GT("incrementLeft", "lVal", "rVal")
					// {{ end }}
					if incrementLeft {
						curLIdx++
						// {{ if $.LNull }}
						_INCREMENT_LEFT_SWITCH(_JOIN_TYPE, _SEL_ARG, _MJ_OVERLOAD, true)
						// {{ else }}
						_INCREMENT_LEFT_SWITCH(_JOIN_TYPE, _SEL_ARG, _MJ_OVERLOAD, false)
						// {{ end }}
					} else {
						curRIdx++
					}

				}
			}
			_PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH(_JOIN_TYPE)
			// Both o.proberState.lIdx and o.proberState.rIdx should point to the
			// last elements processed in their respective batches.
			o.proberState.lIdx = curLIdx
			o.proberState.rIdx = curRIdx
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes an unmatched group from the left.
func _LEFT_UNMATCHED_GROUP_SWITCH(joinType joinTypeInfo) { // */}}
	// {{define "leftUnmatchedGroupSwitch"}}
	// {{ if $.JoinType.IsInner }}
	// {{/*
	// Unmatched groups are not possible with INNER JOIN, so there is nothing to
	// do here.
	// */}}
	// {{ else if $.JoinType.IsLeftOuter }}
	if lGroup.unmatched {
		if curLIdx+1 != curLLength {
			panic("unexpectedly length of the left unmatched group is not 1")
		}
		// The row already does not have a match, so we don't need to do any
		// additional processing.
		o.groups.addLeftOuterGroup(curLIdx, curRIdx)
		curLIdx++
		areGroupsProcessed = true
	}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do if we encounter null in the equality
// column from the left input.
func _NULL_FROM_LEFT_SWITCH(joinType joinTypeInfo) { // */}}
	// {{define "nullFromLeftSwitch"}}
	// {{ if $.JoinType.IsInner }}
	// {{/*
	// Nulls coming from the left input are ignored in INNER JOIN.
	// */}}
	// {{ else if $.JoinType.IsLeftOuter }}
	o.groups.addLeftOuterGroup(curLIdx, curRIdx)
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do when - while looking for a match
// between two inputs - we need to advance the left side, i.e. it decides how
// to handle an unmatched tuple from the left.
func _INCREMENT_LEFT_SWITCH(
	joinType joinTypeInfo, sel selPermutation, overload mjOverload, lHasNulls bool,
) { // */}}
	// {{define "incrementLeftSwitch"}}
	// {{ $sel := $.Sel }}
	// {{ $mjOverload := $.MJOverload }}
	// {{ if $.JoinType.IsInner }}
	// {{/*
	// Unmatched tuple from the left source is not outputted in INNER JOIN.
	// */}}
	// {{ else if $.JoinType.IsLeftOuter }}
	// All the rows on the left within the current group will not get
	// a match on the right, so we're adding each of them as a left
	// outer group.
	o.groups.addLeftOuterGroup(curLIdx-1, curRIdx)
	for curLIdx < curLLength {
		// {{ if $.LNull }}
		if lVec.Nulls().NullAt64(uint64(_L_SEL_IND)) {
			break
		}
		// {{ end }}
		newLVal := lKeys[_L_SEL_IND]
		// {{with $mjOverload}}
		_ASSIGN_EQ("match", "newLVal", "lVal")
		// {{end}}
		if !match {
			break
		}
		o.groups.addLeftOuterGroup(curLIdx, curRIdx)
		curLIdx++
	}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes all but last groups in a column after we have
// reached the end of either the left or right group.
func _PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH(joinType joinTypeInfo) { // */}}
	// {{define "processNotLastGroupInColumnSwitch"}}
	// {{ if $.JoinType.IsInner }}
	// {{/*
	// Nothing to do here since an unmatched tuple is omitted.
	// */}}
	// {{ else if $.JoinType.IsLeftOuter }}
	if !o.groups.isLastGroupInCol() && !areGroupsProcessed {
		// The current group is not the last one within the column, so it
		// cannot be extended into the next batch, and we need to process it
		// right now.
		// Any unprocessed row in the left group will not get a match, so
		// each one of them becomes a new unmatched group with a
		// corresponding null group.
		for curLIdx < curLLength {
			o.groups.addLeftOuterGroup(curLIdx, curRIdx)
			curLIdx++
		}
	}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{ range $joinType := $.JoinTypes }}
// {{ range $sel := $.SelPermutations }}
func (o *mergeJoin_JOIN_TYPE_STRINGOp) probeBodyLSel_IS_L_SELRSel_IS_R_SEL() {
	lSel := o.proberState.lBatch.Selection()
	rSel := o.proberState.rBatch.Selection()
EqLoop:
	for eqColIdx := 0; eqColIdx < len(o.left.eqCols); eqColIdx++ {
		lVec := o.proberState.lBatch.ColVec(int(o.left.eqCols[eqColIdx]))
		rVec := o.proberState.rBatch.ColVec(int(o.right.eqCols[eqColIdx]))
		colType := o.left.sourceTypes[int(o.left.eqCols[eqColIdx])]
		if lVec.HasNulls() {
			if rVec.HasNulls() {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, true, true, true)
				} else {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, true, true, false)
				}
			} else {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, true, false, true)
				} else {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, true, false, false)
				}
			}
		} else {
			if rVec.HasNulls() {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, false, true, true)
				} else {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, false, true, false)
				}
			} else {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, false, false, true)
				} else {
					_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, false, false, false)
				}
			}
		}
		// Look at the groups associated with the next equality column by moving
		// the circular buffer pointer up.
		o.groups.finishedCol()
	}
}

// {{end}}
// {{end}}

// {{/*
// This code snippet builds the output corresponding to the left side (i.e. is
// the main body of buildLeftGroups()).
func _LEFT_SWITCH(isSel bool, hasNulls bool) { // */}}
	// {{define "leftSwitch"}}

	switch colType {
	// {{range $.Global.MJOverloads }}
	case _TYPES_T:
		srcCol := src._TemplateType()
		outCol := out._TemplateType()
		var val _GOTYPE
		var srcStartIdx int

		// Loop over every group.
		for ; o.builderState.left.groupsIdx < len(leftGroups); o.builderState.left.groupsIdx++ {
			leftGroup := &leftGroups[o.builderState.left.groupsIdx]
			// If curSrcStartIdx is uninitialized, start it at the group's start idx.
			// Otherwise continue where we left off.
			if o.builderState.left.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
				o.builderState.left.curSrcStartIdx = leftGroup.rowStartIdx
			}
			// Loop over every row in the group.
			for ; o.builderState.left.curSrcStartIdx < leftGroup.rowEndIdx; o.builderState.left.curSrcStartIdx++ {
				// Repeat each row numRepeats times.
				srcStartIdx = o.builderState.left.curSrcStartIdx
				// {{ if $.IsSel }}
				srcStartIdx = int(sel[srcStartIdx])
				// {{ end }}

				repeatsLeft := leftGroup.numRepeats - o.builderState.left.numRepeatsIdx
				toAppend := repeatsLeft
				if outStartIdx+toAppend > outputBatchSize {
					toAppend = outputBatchSize - outStartIdx
				}

				var isNull bool
				// {{ if $.HasNulls }}
				isNull = src.Nulls().NullAt64(uint64(srcStartIdx))
				if isNull {
					out.Nulls().SetNullRange(uint64(outStartIdx), uint64(outStartIdx+toAppend))
					outStartIdx += toAppend
				}
				// {{ end }}

				if !isNull {
					val = srcCol[srcStartIdx]
					for i := 0; i < toAppend; i++ {
						outCol[outStartIdx] = val
						outStartIdx++
					}
				}

				if toAppend < repeatsLeft {
					// We didn't materialize all the rows in the group so save state and
					// move to the next column.
					o.builderState.left.numRepeatsIdx += toAppend
					if o.builderState.left.colIdx == len(input.outCols)-1 {
						o.builderState.left.colIdx = zeroMJCPcolIdx
						return
					}
					o.builderState.left.setBuilderColumnState(initialBuilderState)
					continue LeftColLoop
				}

				o.builderState.left.numRepeatsIdx = zeroMJCPnumRepeatsIdx
			}
			o.builderState.left.curSrcStartIdx = zeroMJCPcurSrcStartIdx
		}
		o.builderState.left.groupsIdx = zeroMJCPgroupsIdx
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// buildLeftGroups takes a []group and expands each group into the output by
// repeating each row in the group numRepeats times. For example, given an
// input table:
//  L1 |  L2
//  --------
//  1  |  a
//  1  |  b
// and leftGroups = [{startIdx: 0, endIdx: 2, numRepeats: 3}]
// then buildLeftGroups expands this to
//  L1 |  L2
//  --------
//  1  |  a
//  1  |  a
//  1  |  a
//  1  |  b
//  1  |  b
//  1  |  b
// Note: this is different from buildRightGroups in that each row of group is
// repeated numRepeats times, instead of a simple copy of the group as a whole.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoinBase) buildLeftGroups(
	leftGroups []group,
	colOffset int,
	input *mergeJoinInput,
	batch coldata.Batch,
	destStartIdx uint16,
) {
	sel := batch.Selection()
	initialBuilderState := o.builderState.left
	outputBatchSize := int(o.outputBatchSize)
	// Loop over every column.
LeftColLoop:
	for ; o.builderState.left.colIdx < len(input.outCols); o.builderState.left.colIdx++ {
		colIdx := input.outCols[o.builderState.left.colIdx]
		outStartIdx := int(destStartIdx)
		out := o.output.ColVec(int(colIdx))
		src := batch.ColVec(int(colIdx))
		colType := input.sourceTypes[colIdx]

		if sel != nil {
			if src.HasNulls() {
				_LEFT_SWITCH(true, true)
			} else {
				_LEFT_SWITCH(true, false)
			}
		} else {
			if src.HasNulls() {
				_LEFT_SWITCH(false, true)
			} else {
				_LEFT_SWITCH(false, false)
			}
		}
		o.builderState.left.setBuilderColumnState(initialBuilderState)
	}
	o.builderState.left.reset()
}

// {{/*
// This code snippet builds the output corresponding to the right side (i.e. is
// the main body of buildRightGroups()).
func _RIGHT_SWITCH(isSel bool, hasNulls bool) { // */}}
	// {{define "rightSwitch"}}

	switch colType {
	// {{range $.Global.MJOverloads }}
	case _TYPES_T:
		srcCol := src._TemplateType()
		outCol := out._TemplateType()

		// Loop over every group.
		for ; o.builderState.right.groupsIdx < len(rightGroups); o.builderState.right.groupsIdx++ {
			rightGroup := &rightGroups[o.builderState.right.groupsIdx]
			// Repeat every group numRepeats times.
			for ; o.builderState.right.numRepeatsIdx < rightGroup.numRepeats; o.builderState.right.numRepeatsIdx++ {
				if o.builderState.right.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
					o.builderState.right.curSrcStartIdx = rightGroup.rowStartIdx
				}
				toAppend := rightGroup.rowEndIdx - o.builderState.right.curSrcStartIdx
				if outStartIdx+toAppend > outputBatchSize {
					toAppend = outputBatchSize - outStartIdx
				}

				if rightGroup.nullGroup {
					out.Nulls().SetNullRange(uint64(outStartIdx), uint64(outStartIdx+toAppend))
				} else {
					// {{ if $.HasNulls }}
					// {{ if $.IsSel }}
					out.Nulls().ExtendWithSel(src.Nulls(), uint64(outStartIdx), uint16(o.builderState.right.curSrcStartIdx), uint16(toAppend), sel)
					// {{ else }}
					out.Nulls().Extend(src.Nulls(), uint64(outStartIdx), uint16(o.builderState.right.curSrcStartIdx), uint16(toAppend))
					// {{ end }}
					// {{ end }}

					// Optimization in the case that group length is 1, use assign
					// instead of copy.
					if toAppend == 1 {
						// {{ if $.IsSel }}
						outCol[outStartIdx] = srcCol[sel[o.builderState.right.curSrcStartIdx]]
						// {{ else }}
						outCol[outStartIdx] = srcCol[o.builderState.right.curSrcStartIdx]
						// {{ end }}
					} else {
						// {{ if $.IsSel }}
						for i := 0; i < toAppend; i++ {
							outCol[i+outStartIdx] = srcCol[sel[i+o.builderState.right.curSrcStartIdx]]
						}
						// {{ else }}
						copy(outCol[outStartIdx:], srcCol[o.builderState.right.curSrcStartIdx:o.builderState.right.curSrcStartIdx+toAppend])
						// {{ end }}
					}
				}

				outStartIdx += toAppend

				// If we haven't materialized all the rows from the group, then we are
				// done with the current column.
				if toAppend < rightGroup.rowEndIdx-o.builderState.right.curSrcStartIdx {
					// If it's the last column, save state and return.
					if o.builderState.right.colIdx == len(input.outCols)-1 {
						o.builderState.right.curSrcStartIdx = o.builderState.right.curSrcStartIdx + toAppend
						o.builderState.right.colIdx = zeroMJCPcolIdx
						return
					}
					// Otherwise, reset to the initial state and begin the next column.
					o.builderState.right.setBuilderColumnState(initialBuilderState)
					continue RightColLoop
				}
				o.builderState.right.curSrcStartIdx = zeroMJCPcurSrcStartIdx
			}
			o.builderState.right.numRepeatsIdx = zeroMJCPnumRepeatsIdx
		}
		o.builderState.right.groupsIdx = zeroMJCPgroupsIdx
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// buildRightGroups takes a []group and repeats each group numRepeats times.
// For example, given an input table:
//  R1 |  R2
//  --------
//  1  |  a
//  1  |  b
// and rightGroups = [{startIdx: 0, endIdx: 2, numRepeats: 3}]
// then buildRightGroups expands this to
//  R1 |  R2
//  --------
//  1  |  a
//  1  |  b
//  1  |  a
//  1  |  b
//  1  |  a
//  1  |  b
// Note: this is different from buildLeftGroups in that each group is not
// expanded but directly copied numRepeats times.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoinBase) buildRightGroups(
	rightGroups []group,
	colOffset int,
	input *mergeJoinInput,
	batch coldata.Batch,
	destStartIdx uint16,
) {
	initialBuilderState := o.builderState.right
	sel := batch.Selection()
	outputBatchSize := int(o.outputBatchSize)

	// Loop over every column.
RightColLoop:
	for ; o.builderState.right.colIdx < len(input.outCols); o.builderState.right.colIdx++ {
		colIdx := input.outCols[o.builderState.right.colIdx]
		outStartIdx := int(destStartIdx)
		out := o.output.ColVec(int(colIdx) + colOffset)
		src := batch.ColVec(int(colIdx))
		colType := input.sourceTypes[colIdx]

		if sel != nil {
			if src.HasNulls() {
				_RIGHT_SWITCH(true, true)
			} else {
				_RIGHT_SWITCH(true, false)
			}
		} else {
			if src.HasNulls() {
				_RIGHT_SWITCH(false, true)
			} else {
				_RIGHT_SWITCH(false, false)
			}
		}

		o.builderState.right.setBuilderColumnState(initialBuilderState)
	}
	o.builderState.right.reset()
}

// isBufferedGroupFinished checks to see whether or not the buffered group
// corresponding to input continues in batch.
func (o *mergeJoinBase) isBufferedGroupFinished(
	input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) bool {
	if batch.Length() == 0 {
		return true
	}
	sel := batch.Selection()
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
	}
	lastBufferedTupleIdx := bufferedGroup.length - 1

	// Check all equality columns in the first row of batch to make sure we're in
	// the same group.
	for _, colIdx := range input.eqCols[:len(input.eqCols)] {
		colTyp := input.sourceTypes[colIdx]

		switch colTyp {
		// {{ range $.MJOverloads }}
		case _TYPES_T:
			switch o.joinType {
			case sqlbase.JoinType_INNER:
				// {{/*
				// Nulls cannot be in equality columns in the buffered group in INNER JOIN
				// because such tuple would not get a match, so they are never buffered.
				// */}}
			case sqlbase.JoinType_LEFT_OUTER:
				if input == &o.left {
					// Nulls only from the left input can be saved in the buffer, so we do
					// not perform this check on the right input.
					// TODO(yuzefovich): update this when new join types are supported.
					if bufferedGroup.ColVec(int(colIdx)).Nulls().NullAt64(uint64(lastBufferedTupleIdx)) {
						return true
					}
				}
			default:
				panic("")
			}
			prevVal := bufferedGroup.ColVec(int(colIdx))._TemplateType()[lastBufferedTupleIdx]
			var curVal _GOTYPE
			if sel != nil {
				// TODO (georgeutsin): Potentially update this logic for non INNER joins.
				if batch.ColVec(int(colIdx)).HasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt64(uint64(sel[rowIdx])) {
					return true
				}
				curVal = batch.ColVec(int(colIdx))._TemplateType()[sel[rowIdx]]
			} else {
				// TODO (georgeutsin): Potentially update this logic for non INNER joins.
				if batch.ColVec(int(colIdx)).HasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt64(uint64(rowIdx)) {
					return true
				}
				curVal = batch.ColVec(int(colIdx))._TemplateType()[rowIdx]
			}
			var match bool
			_ASSIGN_EQ("match", "prevVal", "curVal")
			if !match {
				return true
			}
		// {{end}}
		default:
			panic(fmt.Sprintf("unhandled type %d", colTyp))
		}
	}
	return false
}

// {{ range $joinType := .JoinTypes }}

// probe is where we generate the groups slices that are used in the build
// phase. We do this by first assuming that every row in both batches
// contributes to the cross product. Then, with every equality column, we
// filter out the rows that don't contribute to the cross product (i.e. they
// don't have a matching row on the other side in the case of an inner join),
// and set the correct cardinality.
// Note that in this phase, we do this for every group, except the last group
// in the batch.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) probe() {
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

// {{ end }}

// {{/*
// This code snippet is executed when at least one of the input sources has
// been exhausted. It processes any remaining tuples and then sets up the
// builder.
func _SOURCE_FINISHED_SWITCH(joinType joinTypeInfo) { // */}}
	// {{define "sourceFinishedSwitch"}}
	// {{ if $.JoinType.IsInner }}
	o.setBuilderSourceToBufferedGroup()
	o.outputReady = true
	// {{ else if $.JoinType.IsLeftOuter }}
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
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{ range $joinType := .JoinTypes }}
func (o *mergeJoin_JOIN_TYPE_STRINGOp) Next(ctx context.Context) coldata.Batch {
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
			_SOURCE_FINISHED_SWITCH(_JOIN_TYPE)
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

// {{ end }}
