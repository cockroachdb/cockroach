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
	"math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the the second input == the third input.
func _ASSIGN_EQ(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the the second input < the third input.
func _ASSIGN_LT(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
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

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// {{ range $joinType := .JoinTypes }}
type mergeJoin_JOIN_TYPE_STRINGOp struct {
	mergeJoinBase
}

var _ StaticMemoryOperator = &mergeJoin_JOIN_TYPE_STRINGOp{}

// {{ end }}

// {{/*
// This code snippet is the "meat" of the probing phase.
func _PROBE_SWITCH(
	_JOIN_TYPE joinTypeInfo,
	_SEL_PERMUTATION selPermutation,
	_L_HAS_NULLS bool,
	_R_HAS_NULLS bool,
	_ASC_DIRECTION bool,
) { // */}}
	// {{define "probeSwitch"}}
	// {{ $sel := $.SelPermutation }}
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
			_RIGHT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE)
			// Expand or filter each group based on the current equality column.
			for curLIdx < curLLength && curRIdx < curRLength && !areGroupsProcessed {
				// {{ if _L_HAS_NULLS }}
				if lVec.Nulls().NullAt64(uint64(_L_SEL_IND)) {
					_NULL_FROM_LEFT_SWITCH(_JOIN_TYPE)
					curLIdx++
					continue
				}
				// {{ end }}
				// {{ if _R_HAS_NULLS }}
				if rVec.Nulls().NullAt64(uint64(_R_SEL_IND)) {
					_NULL_FROM_RIGHT_SWITCH(_JOIN_TYPE)
					curRIdx++
					continue
				}
				// {{ end }}

				lSelIdx := _L_SEL_IND
				lVal := execgen.UNSAFEGET(lKeys, int(lSelIdx))
				rSelIdx := _R_SEL_IND
				rVal := execgen.UNSAFEGET(rKeys, int(rSelIdx))

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
							// {{ if _L_HAS_NULLS }}
							if lVec.Nulls().NullAt64(uint64(_L_SEL_IND)) {
								lComplete = true
								break
							}
							// {{ end }}
							lSelIdx := _L_SEL_IND
							newLVal := execgen.UNSAFEGET(lKeys, int(lSelIdx))
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
							// {{ if _R_HAS_NULLS }}
							if rVec.Nulls().NullAt64(uint64(_R_SEL_IND)) {
								rComplete = true
								break
							}
							// {{ end }}
							rSelIdx := _R_SEL_IND
							newRVal := execgen.UNSAFEGET(rKeys, int(rSelIdx))
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

					// {{ if _JOIN_TYPE.IsLeftSemi }}
					if eqColIdx < len(o.left.eqCols)-1 {
						o.groups.addGroupsToNextCol(beginLIdx, lGroupLength, beginRIdx, rGroupLength)
					} else {
						o.groups.addLeftSemiGroup(beginLIdx, lGroupLength)
					}
					// {{ else }}
					// Neither group ends with the batch, so add the group to the
					// circular buffer.
					o.groups.addGroupsToNextCol(beginLIdx, lGroupLength, beginRIdx, rGroupLength)
					// {{ end }}
				} else { // mismatch
					var incrementLeft bool
					// {{ if _ASC_DIRECTION }}
					_ASSIGN_LT("incrementLeft", "lVal", "rVal")
					// {{ else }}
					_ASSIGN_GT("incrementLeft", "lVal", "rVal")
					// {{ end }}
					if incrementLeft {
						curLIdx++
						// {{ if _L_HAS_NULLS }}
						_INCREMENT_LEFT_SWITCH(_JOIN_TYPE, _SEL_ARG, _MJ_OVERLOAD, true)
						// {{ else }}
						_INCREMENT_LEFT_SWITCH(_JOIN_TYPE, _SEL_ARG, _MJ_OVERLOAD, false)
						// {{ end }}
					} else {
						curRIdx++
						// {{ if _R_HAS_NULLS }}
						_INCREMENT_RIGHT_SWITCH(_JOIN_TYPE, _SEL_ARG, _MJ_OVERLOAD, true)
						// {{ else }}
						_INCREMENT_RIGHT_SWITCH(_JOIN_TYPE, _SEL_ARG, _MJ_OVERLOAD, false)
						// {{ end }}
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
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes an unmatched group from the left.
func _LEFT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "leftUnmatchedGroupSwitch"}}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Unmatched groups are not possible with INNER JOIN and LEFT SEMI JOIN, so
	// there is nothing to do here.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	if lGroup.unmatched {
		if curLIdx+1 != curLLength {
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpectedly length %d of the left unmatched group is not 1", curLLength-curLIdx))
		}
		// The row already does not have a match, so we don't need to do any
		// additional processing.
		o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
		curLIdx++
		areGroupsProcessed = true
	}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	// {{/*
	// Unmatched groups from the left are not possible with RIGHT OUTER JOIN, so
	// there is nothing to do here.
	// */}}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes an unmatched group from the right.
func _RIGHT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "rightUnmatchedGroupSwitch"}}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Unmatched groups are not possible with INNER JOIN and LEFT SEMI JOIN, so
	// there is nothing to do here.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	// {{/*
	// Unmatched groups from the right are not possible with LEFT OUTER JOIN and
	// LEFT ANTI JOIN, so there is nothing to do here.
	// */}}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	if rGroup.unmatched {
		if curRIdx+1 != curRLength {
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpectedly length %d of the right unmatched group is not 1", curRLength-curRIdx))
		}
		// The row already does not have a match, so we don't need to do any
		// additional processing.
		o.groups.addRightOuterGroup(curLIdx, curRIdx)
		curRIdx++
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
func _NULL_FROM_LEFT_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "nullFromLeftSwitch"}}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Nulls coming from the left input are ignored in INNER JOIN and LEFT SEMI
	// JOIN.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	// {{/*
	// Nulls coming from the left input are ignored in RIGHT OUTER JOIN.
	// */}}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do if we encounter null in the equality
// column from the right input.
func _NULL_FROM_RIGHT_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "nullFromRightSwitch"}}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Nulls coming from the right input are ignored in INNER JOIN and LEFT SEMI
	// JOIN.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	// {{/*
	// Nulls coming from the right input are ignored in LEFT OUTER JOIN and LEFT
	// ANTI JOIN.
	// */}}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	o.groups.addRightOuterGroup(curLIdx, curRIdx)
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
	_JOIN_TYPE joinTypeInfo,
	_SEL_PERMUTATION selPermutation,
	_MJ_OVERLOAD mjOverload,
	_L_HAS_NULLS bool,
) { // */}}
	// {{define "incrementLeftSwitch"}}
	// {{ $sel := $.SelPermutation }}
	// {{ $mjOverload := $.MJOverload }}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Unmatched tuple from the left source is not outputted in INNER JOIN and
	// LEFT SEMI JOIN.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	// All the rows on the left within the current group will not get
	// a match on the right, so we're adding each of them as a left
	// outer group.
	o.groups.addLeftUnmatchedGroup(curLIdx-1, curRIdx)
	for curLIdx < curLLength {
		// {{ if _L_HAS_NULLS }}
		if lVec.Nulls().NullAt64(uint64(_L_SEL_IND)) {
			break
		}
		// {{ end }}
		lSelIdx = _L_SEL_IND
		newLVal := execgen.UNSAFEGET(lKeys, int(lSelIdx))
		// {{with _MJ_OVERLOAD}}
		_ASSIGN_EQ("match", "newLVal", "lVal")
		// {{end}}
		if !match {
			break
		}
		o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
		curLIdx++
	}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	// {{/*
	// Unmatched tuple from the left source is not outputted in RIGHT OUTER JOIN.
	// */}}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do when - while looking for a match
// between two inputs - we need to advance the right side, i.e. it decides how
// to handle an unmatched tuple from the right.
func _INCREMENT_RIGHT_SWITCH(
	_JOIN_TYPE joinTypeInfo,
	_SEL_PERMUTATION selPermutation,
	_MJ_OVERLOAD mjOverload,
	_R_HAS_NULLS bool,
) { // */}}
	// {{define "incrementRightSwitch"}}
	// {{ $sel := $.SelPermutation }}
	// {{ $mjOverload := $.MJOverload }}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Unmatched tuple from the right source is not outputted in INNER JOIN and
	// LEFT SEMI JOIN.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	// {{/*
	// Unmatched tuple from the right source is not outputted in LEFT OUTER JOIN
	// and LEFT ANTI JOIN.
	// */}}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	// All the rows on the right within the current group will not get a match on
	// the left, so we're adding each of them as a right outer group.
	o.groups.addRightOuterGroup(curLIdx, curRIdx-1)
	for curRIdx < curRLength {
		// {{ if _R_HAS_NULLS }}
		if rVec.Nulls().NullAt64(uint64(_R_SEL_IND)) {
			break
		}
		// {{ end }}
		rSelIdx = _R_SEL_IND
		newRVal := execgen.UNSAFEGET(rKeys, int(rSelIdx))
		// {{with _MJ_OVERLOAD}}
		_ASSIGN_EQ("match", "newRVal", "rVal")
		// {{end}}
		if !match {
			break
		}
		o.groups.addRightOuterGroup(curLIdx, curRIdx)
		curRIdx++
	}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes all but last groups in a column after we have
// reached the end of either the left or right group.
func _PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "processNotLastGroupInColumnSwitch"}}
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	// {{/*
	// Nothing to do here since an unmatched tuple is omitted.
	// */}}
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	if !o.groups.isLastGroupInCol() && !areGroupsProcessed {
		// The current group is not the last one within the column, so it
		// cannot be extended into the next batch, and we need to process it
		// right now.
		// Any unprocessed row in the left group will not get a match, so
		// each one of them becomes a new unmatched group with a
		// corresponding null group.
		for curLIdx < curLLength {
			o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
			curLIdx++
		}
	}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	if !o.groups.isLastGroupInCol() && !areGroupsProcessed {
		// The current group is not the last one within the column, so it
		// cannot be extended into the next batch, and we need to process it
		// right now.
		// Any unprocessed row in the right group will not get a match, so
		// each one of them becomes a new unmatched group with a
		// corresponding null group.
		for curRIdx < curRLength {
			o.groups.addRightOuterGroup(curLIdx, curRIdx)
			curRIdx++
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
		if lVec.MaybeHasNulls() {
			if rVec.MaybeHasNulls() {
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
			if rVec.MaybeHasNulls() {
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

// {{ end }}
// {{ end }}

// {{/*
// This code snippet builds the output corresponding to the left side (i.e. is
// the main body of buildLeftGroups()).
func _LEFT_SWITCH(_JOIN_TYPE joinTypeInfo, _HAS_SELECTION bool, _HAS_NULLS bool) { // */}}
	// {{define "leftSwitch"}}
	// {{ $joinType := .JoinType }}
	switch colType {
	// {{ range $.Global.MJOverloads }}
	case _TYPES_T:
		srcCol := src._TemplateType()
		outCol := out._TemplateType()
		var val _GOTYPE
		var srcStartIdx int

		// Loop over every group.
		for ; o.builderState.left.groupsIdx < len(leftGroups); o.builderState.left.groupsIdx++ {
			leftGroup := &leftGroups[o.builderState.left.groupsIdx]
			// {{ if _JOIN_TYPE.IsLeftAnti }}
			// {{/*
			// With LEFT ANTI JOIN we want to emit output corresponding only to
			// unmatched tuples, so we're skipping all "matched" groups.
			// */}}
			if !leftGroup.unmatched {
				continue
			}
			// {{ end }}
			// If curSrcStartIdx is uninitialized, start it at the group's start idx.
			// Otherwise continue where we left off.
			if o.builderState.left.curSrcStartIdx == zeroMJCPCurSrcStartIdx {
				o.builderState.left.curSrcStartIdx = leftGroup.rowStartIdx
			}
			// Loop over every row in the group.
			for ; o.builderState.left.curSrcStartIdx < leftGroup.rowEndIdx; o.builderState.left.curSrcStartIdx++ {
				// Repeat each row numRepeats times.
				srcStartIdx = o.builderState.left.curSrcStartIdx
				// {{ if _HAS_SELECTION }}
				srcStartIdx = int(sel[srcStartIdx])
				// {{ end }}

				repeatsLeft := leftGroup.numRepeats - o.builderState.left.numRepeatsIdx
				toAppend := repeatsLeft
				if outStartIdx+toAppend > outputBatchSize {
					toAppend = outputBatchSize - outStartIdx
				}

				// {{ if _JOIN_TYPE.IsRightOuter }}
				// {{/*
				// Null groups on the left can only occur with RIGHT OUTER and FULL
				// OUTER joins for both of which IsRightOuter is true. For other joins,
				// we're omitting this check.
				// */}}
				if leftGroup.nullGroup {
					out.Nulls().SetNullRange(uint64(outStartIdx), uint64(outStartIdx+toAppend))
					outStartIdx += toAppend
				} else
				// {{ end }}
				{
					var isNull bool
					// {{ if _HAS_NULLS }}
					isNull = src.Nulls().NullAt64(uint64(srcStartIdx))
					if isNull {
						out.Nulls().SetNullRange(uint64(outStartIdx), uint64(outStartIdx+toAppend))
						outStartIdx += toAppend
					}
					// {{ end }}

					if !isNull {
						val = execgen.UNSAFEGET(srcCol, srcStartIdx)
						for i := 0; i < toAppend; i++ {
							execgen.SET(outCol, outStartIdx, val)
							outStartIdx++
						}
					}
				}

				if toAppend < repeatsLeft {
					// We didn't materialize all the rows in the group so save state and
					// move to the next column.
					o.builderState.left.numRepeatsIdx += toAppend
					if o.builderState.left.colIdx == len(input.outCols)-1 {
						o.builderState.left.colIdx = zeroMJCPColIdx
						return
					}
					o.builderState.left.setBuilderColumnState(initialBuilderState)
					continue LeftColLoop
				}

				o.builderState.left.numRepeatsIdx = zeroMJCPNumRepeatsIdx
			}
			o.builderState.left.curSrcStartIdx = zeroMJCPCurSrcStartIdx
		}
		o.builderState.left.groupsIdx = zeroMJCPGroupsIdx
	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// {{ range $joinType := .JoinTypes }}

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
func (o *mergeJoin_JOIN_TYPE_STRINGOp) buildLeftGroups(
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
			if src.MaybeHasNulls() {
				_LEFT_SWITCH(_JOIN_TYPE, true, true)
			} else {
				_LEFT_SWITCH(_JOIN_TYPE, true, false)
			}
		} else {
			if src.MaybeHasNulls() {
				_LEFT_SWITCH(_JOIN_TYPE, false, true)
			} else {
				_LEFT_SWITCH(_JOIN_TYPE, false, false)
			}
		}
		o.builderState.left.setBuilderColumnState(initialBuilderState)
	}
	o.builderState.left.reset()
}

// {{ end }}

// {{/*
// This code snippet builds the output corresponding to the right side (i.e. is
// the main body of buildRightGroups()).
func _RIGHT_SWITCH(_JOIN_TYPE joinTypeInfo, _HAS_SELECTION bool, _HAS_NULLS bool) { // */}}
	// {{define "rightSwitch"}}
	// {{ $joinType := .JoinType }}

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
				if o.builderState.right.curSrcStartIdx == zeroMJCPCurSrcStartIdx {
					o.builderState.right.curSrcStartIdx = rightGroup.rowStartIdx
				}
				toAppend := rightGroup.rowEndIdx - o.builderState.right.curSrcStartIdx
				if outStartIdx+toAppend > outputBatchSize {
					toAppend = outputBatchSize - outStartIdx
				}

				// {{ if _JOIN_TYPE.IsLeftOuter }}
				// {{/*
				// Null groups on the right can only occur with LEFT OUTER and FULL
				// OUTER joins for both of which IsLeftOuter is true. For other joins,
				// we're omitting this check.
				// */}}
				if rightGroup.nullGroup {
					out.Nulls().SetNullRange(uint64(outStartIdx), uint64(outStartIdx+toAppend))
				} else
				// {{ end }}
				{
					// {{ if _HAS_NULLS }}
					// {{ if _HAS_SELECTION }}
					out.Nulls().ExtendWithSel(src.Nulls(), uint64(outStartIdx), uint16(o.builderState.right.curSrcStartIdx), uint16(toAppend), sel)
					// {{ else }}
					out.Nulls().Extend(src.Nulls(), uint64(outStartIdx), uint16(o.builderState.right.curSrcStartIdx), uint16(toAppend))
					// {{ end }}
					// {{ end }}

					// Optimization in the case that group length is 1, use assign
					// instead of copy.
					if toAppend == 1 {
						// {{ if _HAS_SELECTION }}
						v := execgen.UNSAFEGET(srcCol, int(sel[o.builderState.right.curSrcStartIdx]))
						execgen.SET(outCol, outStartIdx, v)
						// {{ else }}
						v := execgen.UNSAFEGET(srcCol, o.builderState.right.curSrcStartIdx)
						execgen.SET(outCol, outStartIdx, v)
						// {{ end }}
					} else {
						// {{ if _HAS_SELECTION }}
						for i := 0; i < toAppend; i++ {
							v := execgen.UNSAFEGET(srcCol, int(sel[i+o.builderState.right.curSrcStartIdx]))
							execgen.SET(outCol, i+outStartIdx, v)
						}
						// {{ else }}
						execgen.COPYSLICE(outCol, srcCol, outStartIdx, o.builderState.right.curSrcStartIdx, o.builderState.right.curSrcStartIdx+toAppend)
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
						o.builderState.right.colIdx = zeroMJCPColIdx
						return
					}
					// Otherwise, reset to the initial state and begin the next column.
					o.builderState.right.setBuilderColumnState(initialBuilderState)
					continue RightColLoop
				}
				o.builderState.right.curSrcStartIdx = zeroMJCPCurSrcStartIdx
			}
			o.builderState.right.numRepeatsIdx = zeroMJCPNumRepeatsIdx
		}
		o.builderState.right.groupsIdx = zeroMJCPGroupsIdx
	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// {{ range $joinType := .JoinTypes }}

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
func (o *mergeJoin_JOIN_TYPE_STRINGOp) buildRightGroups(
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
			if src.MaybeHasNulls() {
				_RIGHT_SWITCH(_JOIN_TYPE, true, true)
			} else {
				_RIGHT_SWITCH(_JOIN_TYPE, true, false)
			}
		} else {
			if src.MaybeHasNulls() {
				_RIGHT_SWITCH(_JOIN_TYPE, false, true)
			} else {
				_RIGHT_SWITCH(_JOIN_TYPE, false, false)
			}
		}

		o.builderState.right.setBuilderColumnState(initialBuilderState)
	}
	o.builderState.right.reset()
}

// {{ end }}

// isBufferedGroupFinished checks to see whether or not the buffered group
// corresponding to input continues in batch.
func (o *mergeJoinBase) isBufferedGroupFinished(
	input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) bool {
	if batch.Length() == 0 {
		return true
	}
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
	}
	lastBufferedTupleIdx := bufferedGroup.length - 1
	tupleToLookAtIdx := uint64(rowIdx)
	sel := batch.Selection()
	if sel != nil {
		tupleToLookAtIdx = uint64(sel[rowIdx])
	}

	// Check all equality columns in the first row of batch to make sure we're in
	// the same group.
	for _, colIdx := range input.eqCols[:len(input.eqCols)] {
		colTyp := input.sourceTypes[colIdx]

		switch colTyp {
		// {{ range $.MJOverloads }}
		case _TYPES_T:
			// We perform this null check on every equality column of the last
			// buffered tuple regardless of the join type since it is done only once
			// per batch. In some cases (like INNER JOIN, or LEFT OUTER JOIN with the
			// right side being an input) this check will always return false since
			// nulls couldn't be buffered up though.
			if bufferedGroup.ColVec(int(colIdx)).Nulls().NullAt64(uint64(lastBufferedTupleIdx)) {
				return true
			}
			bufferedCol := bufferedGroup.ColVec(int(colIdx))._TemplateType()
			prevVal := execgen.UNSAFEGET(bufferedCol, int(lastBufferedTupleIdx))
			var curVal _GOTYPE
			if batch.ColVec(int(colIdx)).MaybeHasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt64(tupleToLookAtIdx) {
				return true
			}
			col := batch.ColVec(int(colIdx))._TemplateType()
			curVal = execgen.UNSAFEGET(col, int(tupleToLookAtIdx))
			var match bool
			_ASSIGN_EQ("match", "prevVal", "curVal")
			if !match {
				return true
			}
		// {{end}}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colTyp))
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

// setBuilderSourceToBufferedGroup sets up the builder state to use the
// buffered group.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) setBuilderSourceToBufferedGroup() {
	lGroupEndIdx := int(o.proberState.lBufferedGroup.length)
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.rGroups = o.builderState.rGroups[:1]
	// {{ if not _JOIN_TYPE.IsLeftSemi }}
	rGroupEndIdx := int(o.proberState.rBufferedGroup.length)
	o.builderState.lGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   lGroupEndIdx,
		numRepeats:  rGroupEndIdx,
		toBuild:     lGroupEndIdx * rGroupEndIdx,
	}
	o.builderState.rGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   rGroupEndIdx,
		numRepeats:  lGroupEndIdx,
		toBuild:     rGroupEndIdx * lGroupEndIdx,
	}
	// {{ else }}
	o.builderState.lGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   lGroupEndIdx,
		numRepeats:  1,
		toBuild:     lGroupEndIdx,
	}
	// {{ end }}

	o.builderState.lBatch = o.proberState.lBufferedGroup
	o.builderState.rBatch = o.proberState.rBufferedGroup

	// We cannot yet reset the buffered groups because the builder will be taking
	// input from them. The actual reset will take place on the next call to
	// initProberState().
	o.proberState.lBufferedGroup.needToReset = true
	o.proberState.rBufferedGroup.needToReset = true
}

// exhaustLeftSource sets up the builder to process any remaining tuples from
// the left source. It should only be called when the right source has been
// exhausted.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) exhaustLeftSource() {
	// {{ if _JOIN_TYPE.IsInner }}
	// {{/*
	// Remaining tuples from the left source do not have a match, so they are
	// ignored in INNER JOIN.
	// */}}
	// {{ end }}
	// {{ if or _JOIN_TYPE.IsLeftOuter _JOIN_TYPE.IsLeftAnti }}
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

	o.proberState.lIdx = o.proberState.lLength
	// {{ end }}
	// {{ if _JOIN_TYPE.IsRightOuter }}
	// {{/*
	// Remaining tuples from the left source do not have a match, so they are
	// ignored in RIGHT OUTER JOIN.
	// */}}
	// {{ end }}
}

// exhaustRightSource sets up the builder to process any remaining tuples from
// the right source. It should only be called when the left source has been
// exhausted.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) exhaustRightSource() {
	// {{ if _JOIN_TYPE.IsInner }}
	// {{/*
	// Remaining tuples from the right source do not have a match, so they are
	// ignored in INNER JOIN.
	// */}}
	// {{ end }}
	// {{ if _JOIN_TYPE.IsLeftOuter }}
	// {{/*
	// Remaining tuples from the right source do not have a match, so they are
	// ignored in LEFT OUTER JOIN.
	// */}}
	// {{ end }}
	// {{ if _JOIN_TYPE.IsRightOuter }}
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.lGroups[0] = group{
		rowStartIdx: o.proberState.rIdx,
		rowEndIdx:   o.proberState.rLength,
		numRepeats:  1,
		toBuild:     o.proberState.rLength - o.proberState.rIdx,
		nullGroup:   true,
	}
	o.builderState.rGroups = o.builderState.rGroups[:1]
	o.builderState.rGroups[0] = group{
		rowStartIdx: o.proberState.rIdx,
		rowEndIdx:   o.proberState.rLength,
		numRepeats:  1,
		toBuild:     o.proberState.rLength - o.proberState.rIdx,
		unmatched:   true,
	}

	o.proberState.rIdx = o.proberState.rLength
	// {{ end }}
}

// build creates the cross product, and writes it to the output member.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) build() {
	if o.output.Width() != 0 {
		outStartIdx := o.builderState.outCount
		o.buildLeftGroups(o.builderState.lGroups, 0 /* colOffset */, &o.left, o.builderState.lBatch, outStartIdx)
		// {{ if not (or _JOIN_TYPE.IsLeftSemi _JOIN_TYPE.IsLeftAnti) }}
		o.buildRightGroups(o.builderState.rGroups, len(o.left.sourceTypes), &o.right, o.builderState.rBatch, outStartIdx)
		// {{ end }}
	}
	o.builderState.outCount = o.calculateOutputCount(o.builderState.lGroups)
}

// {{ end }}

// {{/*
// This code snippet is executed when at least one of the input sources has
// been exhausted. It processes any remaining tuples and then sets up the
// builder.
func _SOURCE_FINISHED_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "sourceFinishedSwitch"}}
	o.outputReady = true
	// {{ if or $.JoinType.IsInner $.JoinType.IsLeftSemi }}
	o.setBuilderSourceToBufferedGroup()
	// {{ else }}
	// First we make sure that batches of the builder state are always set. This
	// is needed because the batches are accessed outside of _LEFT_SWITCH and
	// _RIGHT_SWITCH (before the merge joiner figures out whether there are any
	// groups to be built).
	o.builderState.lBatch = o.proberState.lBatch
	o.builderState.rBatch = o.proberState.rBatch
	// Next, we need to make sure that builder state is set up for a case when
	// neither exhaustLeftSource nor exhaustRightSource is called below. In such
	// scenario the merge joiner is done, so it'll be outputting zero-length
	// batches from now on.
	o.builderState.lGroups = o.builderState.lGroups[:0]
	o.builderState.rGroups = o.builderState.rGroups[:0]
	// {{ end }}
	// {{ if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti }}
	// At least one of the sources is finished. If it was the right one,
	// then we need to emit remaining tuples from the left source with
	// nulls corresponding to the right one. But if the left source is
	// finished, then there is nothing left to do.
	if o.proberState.lIdx < o.proberState.lLength {
		o.exhaustLeftSource()
		// We unset o.outputReady here because we want to put as many unmatched
		// tuples from the left into the output batch. Once outCount reaches the
		// desired output batch size, the output will be returned.
		o.outputReady = false
	}
	// {{ end }}
	// {{ if $.JoinType.IsRightOuter }}
	// At least one of the sources is finished. If it was the left one,
	// then we need to emit remaining tuples from the right source with
	// nulls corresponding to the left one. But if the right source is
	// finished, then there is nothing left to do.
	if o.proberState.rIdx < o.proberState.rLength {
		o.exhaustRightSource()
		// We unset o.outputReady here because we want to put as many unmatched
		// tuples from the right into the output batch. Once outCount reaches the
		// desired output batch size, the output will be returned.
		o.outputReady = false
	}
	// {{ end }}
	// {{end}}
	// {{/*
}

// */}}

// {{ range $joinType := .JoinTypes }}

// calculateOutputCount uses the toBuild field of each group and the output
// batch size to determine the output count. Note that as soon as a group is
// materialized partially or fully to output, its toBuild field is updated
// accordingly.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) calculateOutputCount(groups []group) uint16 {
	count := int(o.builderState.outCount)

	for i := 0; i < len(groups); i++ {
		// {{ if _JOIN_TYPE.IsLeftAnti }}
		if !groups[i].unmatched {
			// "Matched" groups are not outputted in LEFT ANTI JOIN, so they do not
			// contribute to the output count.
			continue
		}
		// {{ end }}
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
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected merge joiner state in Next: %v", o.state))
		}
	}
}

// {{ end }}
