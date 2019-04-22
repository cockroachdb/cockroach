// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
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

const _L_SEL_IND = 0
const _R_SEL_IND = 0

// _SEL_ARG is used in place of the string "$sel", since that isn't valid go code.
const _SEL_ARG = 0

// */}}

// {{/*
func _PROBE_SWITCH(sel selPermutation, lHasNulls bool, rHasNulls bool, asc bool) { // */}}
	// {{define "probeSwitch"}}
	// {{ $sel := $.Sel }}
	switch colType {
	// {{range $.Global.MJOverloads }}
	case _TYPES_T:
		lKeys := lVec._TemplateType()
		rKeys := rVec._TemplateType()
		var lGroup, rGroup group
		for o.groups.nextGroupInCol(&lGroup, &rGroup) {
			curLIdx := lGroup.rowStartIdx
			curRIdx := rGroup.rowStartIdx
			curLLength := lGroup.rowEndIdx
			curRLength := rGroup.rowEndIdx
			// Expand or filter each group based on the current equality column.
			for curLIdx < curLLength && curRIdx < curRLength {
				// TODO(georgeutsin): change null check logic for non INNER joins.
				// {{ if $.LNull }}
				if lVec.NullAt64(uint64(_L_SEL_IND)) {
					curLIdx++
					continue
				}
				// {{ end }}
				// {{ if $.RNull }}
				if rVec.NullAt64(uint64(_R_SEL_IND)) {
					curRIdx++
					continue
				}
				// {{ end }}

				// _L_SEL_IND is the template type variable for the loop variable that's either
				// curLIdx or lSel[curLIdx] depending on whether we're in a selection or not.
				lVal := lKeys[_L_SEL_IND]
				// _R_SEL_IND is the template type variable for the loop variable that's either
				// curRIdx or rSel[curRIdx] depending on whether we're in a selection or not.
				rVal := rKeys[_R_SEL_IND]

				var match bool
				_ASSIGN_EQ("match", "lVal", "rVal")
				if match {
					// Find the length of the groups on each side.
					lGroupLength, rGroupLength := 0, 0
					lComplete, rComplete := false, false
					beginLIdx, beginRIdx := curLIdx, curRIdx

					// Find the length of the group on the left.
					if curLLength == 0 {
						lGroupLength, lComplete = 0, true
					} else {
						for curLIdx < curLLength {
							// TODO(georgeutsin): change null check logic for non INNER joins.
							// {{ if $.LNull }}
							if lVec.NullAt64(uint64(_L_SEL_IND)) {
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
						for curRIdx < curRLength {
							// TODO(georgeutsin): change null check logic for non INNER joins.
							// {{ if $.RNull }}
							if rVec.NullAt64(uint64(_R_SEL_IND)) {
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

					// Last equality column and either group is incomplete. Save state and have it handled in the next iteration.
					if eqColIdx == len(o.left.eqCols)-1 && (!lComplete || !rComplete) {
						o.saveGroupToState(beginLIdx, lGroupLength, o.proberState.lBatch, lSel, &o.left, o.proberState.lGroup, &o.proberState.lGroupEndIdx)
						o.proberState.lIdx = lGroupLength + beginLIdx
						o.saveGroupToState(beginRIdx, rGroupLength, o.proberState.rBatch, rSel, &o.right, o.proberState.rGroup, &o.proberState.rGroupEndIdx)
						o.proberState.rIdx = rGroupLength + beginRIdx

						o.groups.finishedCol()
						break EqLoop
					}

					// Neither group ends with the batch so add the group to the circular buffer and increment the indices.
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
					} else {
						curRIdx++
					}
				}
			}
			// Both o.proberState.lIdx and o.proberState.rIdx should point to the last elements processed in their respective batches.
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

// {{ range $sel := .SelPermutations }}
func (o *mergeJoinOp) probeBodyLSel_IS_L_SELRSel_IS_R_SEL() {
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
					_PROBE_SWITCH(_SEL_ARG, true, true, true)
				} else {
					_PROBE_SWITCH(_SEL_ARG, true, true, false)
				}
			} else {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_SEL_ARG, true, false, true)
				} else {
					_PROBE_SWITCH(_SEL_ARG, true, false, false)
				}
			}
		} else {
			if rVec.HasNulls() {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_SEL_ARG, false, true, true)
				} else {
					_PROBE_SWITCH(_SEL_ARG, false, true, false)
				}
			} else {
				if o.left.directions[eqColIdx] == distsqlpb.Ordering_Column_ASC {
					_PROBE_SWITCH(_SEL_ARG, false, false, true)
				} else {
					_PROBE_SWITCH(_SEL_ARG, false, false, false)
				}
			}
		}
		// Look at the groups associated with the next equality column by moving the circular buffer pointer up.
		o.groups.finishedCol()
	}
}

// {{end}}

// {{/*
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
		for ; o.builderState.left.groupsIdx < groupsLen; o.builderState.left.groupsIdx++ {
			leftGroup := &leftGroups[o.builderState.left.groupsIdx]
			// If curSrcStartIdx is uninitialized, start it at the group's start idx. Otherwise continue where we left off.
			if o.builderState.left.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
				o.builderState.left.curSrcStartIdx = leftGroup.rowStartIdx
			}
			// Loop over every row in the group.
			for ; o.builderState.left.curSrcStartIdx < leftGroup.rowEndIdx; o.builderState.left.curSrcStartIdx++ {
				// Repeat each row numRepeats times.
				srcStartIdx = o.builderState.left.curSrcStartIdx
				// {{ if $.IsSel }}
				val = srcCol[sel[srcStartIdx]]
				// {{ else }}
				val = srcCol[srcStartIdx]
				// {{ end }}

				repeatsLeft := leftGroup.numRepeats - o.builderState.left.numRepeatsIdx
				toAppend := repeatsLeft
				if outStartIdx+toAppend > outputBatchSize {
					toAppend = outputBatchSize - outStartIdx
				}

				// {{ if $.HasNulls }}
				if src.NullAt64(uint64(srcStartIdx)) {
					out.SetNullRange(uint64(outStartIdx), uint64(outStartIdx+toAppend))
				}
				// {{ end }}

				for i := 0; i < toAppend; i++ {
					outCol[outStartIdx] = val
					outStartIdx++
				}

				if toAppend < repeatsLeft {
					// We didn't materialize all the rows in the group so save state and move to next column.
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

// buildLeftGroups takes a []group and expands each group into the output by repeating
// each row in the group numRepeats times. For example, given an input table:
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
// Note: this is different from buildRightGroups in that each row of group is repeated
// numRepeats times, instead of a simple copy of the group as a whole.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoinOp) buildLeftGroups(
	leftGroups []group,
	groupsLen int,
	colOffset int,
	input *mergeJoinInput,
	bat coldata.Batch,
	destStartIdx uint16,
) {
	o.builderState.left.finished = false
	sel := bat.Selection()
	initialBuilderState := o.builderState.left
	outputBatchSize := int(o.outputBatchSize)
	// Loop over every column.
LeftColLoop:
	for ; o.builderState.left.colIdx < len(input.outCols); o.builderState.left.colIdx++ {
		colIdx := input.outCols[o.builderState.left.colIdx]
		outStartIdx := int(destStartIdx)
		out := o.output.ColVec(int(colIdx))
		src := bat.ColVec(int(colIdx))
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
func _RIGHT_SWITCH(isSel bool, hasNulls bool) { // */}}
	// {{define "rightSwitch"}}

	switch colType {
	// {{range $.Global.MJOverloads }}
	case _TYPES_T:
		srcCol := src._TemplateType()
		outCol := out._TemplateType()

		// Loop over every group.
		for ; o.builderState.right.groupsIdx < groupsLen; o.builderState.right.groupsIdx++ {
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

				// {{ if $.HasNulls }}
				out.ExtendNulls(src, uint64(outStartIdx), uint16(o.builderState.right.curSrcStartIdx), uint16(toAppend))
				// {{ end }}

				// Optimization in the case that group length is 1, use assign instead of copy.
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

				outStartIdx += toAppend

				// If we haven't materialized all the rows from the group, then we are done with the current column.
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
// Note: this is different from buildLeftGroups in that each group is not expanded,
// but directly copied numRepeats times.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoinOp) buildRightGroups(
	rightGroups []group,
	groupsLen int,
	colOffset int,
	input *mergeJoinInput,
	bat coldata.Batch,
	destStartIdx uint16,
) {
	o.builderState.right.finished = false
	initialBuilderState := o.builderState.right
	sel := bat.Selection()
	outputBatchSize := int(o.outputBatchSize)

	// Loop over every column.
RightColLoop:
	for ; o.builderState.right.colIdx < len(input.outCols); o.builderState.right.colIdx++ {
		colIdx := input.outCols[o.builderState.right.colIdx]
		outStartIdx := int(destStartIdx)
		out := o.output.ColVec(int(colIdx) + colOffset)
		src := bat.ColVec(int(colIdx))
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

// isGroupFinished checks to see whether or not the savedGroup continues in bat.
func (o *mergeJoinOp) isGroupFinished(
	input *mergeJoinInput,
	savedGroup coldata.Batch,
	savedGroupIdx int,
	bat coldata.Batch,
	rowIdx int,
	sel []uint16,
) bool {
	if bat.Length() == 0 {
		return true
	}

	// Check all equality columns in the first row of the bat to make sure we're in the same group.
	for _, colIdx := range input.eqCols[:len(input.eqCols)] {
		colTyp := input.sourceTypes[colIdx]

		switch colTyp {
		// {{ range .MJOverloads }}
		case _TYPES_T:
			prevVal := savedGroup.ColVec(int(colIdx))._TemplateType()[savedGroupIdx-1]
			var curVal _GOTYPE
			if sel != nil {
				// TODO (georgeutsin): Potentially update this logic for non INNER joins.
				if bat.ColVec(int(colIdx)).HasNulls() && bat.ColVec(int(colIdx)).NullAt64(uint64(sel[rowIdx])) {
					return true
				}
				curVal = bat.ColVec(int(colIdx))._TemplateType()[sel[rowIdx]]
			} else {
				// TODO (georgeutsin): Potentially update this logic for non INNER joins.
				if bat.ColVec(int(colIdx)).HasNulls() && bat.ColVec(int(colIdx)).NullAt64(uint64(rowIdx)) {
					return true
				}
				curVal = bat.ColVec(int(colIdx))._TemplateType()[rowIdx]
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
