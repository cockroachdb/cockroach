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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// {{/*
// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// */}}

// {{/*
func _COPY_WITH_SEL(
	t_dest coldata.Vec,
	t_destStartIdx int,
	t_src coldata.Vec,
	t_srcStartIdx int,
	t_srcEndIdx int,
	t_sel []uint16,
) { // */}}
	// {{define "copyWithSel"}}
	batchSize := t_srcEndIdx - t_srcStartIdx
	for i := 0; i < batchSize; i++ {
		t_dest._TemplateType()[i+t_destStartIdx] = t_src._TemplateType()[t_sel[i+t_srcStartIdx]]
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
// buildLeftGroups returns the first available index of the output buffer as outStartIdx.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoinOp) buildLeftGroups(
	leftGroups []group,
	groupsLen int,
	colOffset int,
	input *mergeJoinInput,
	bat coldata.Batch,
	destStartIdx uint16,
) (outStartIdx uint16) {
	o.builderState.left.finished = false
	sel := bat.Selection()
	outStartIdx = destStartIdx
	initialBuilderState := o.builderState.left
	// Loop over every column.
LeftColLoop:
	for ; o.builderState.left.colIdx < len(input.outCols); o.builderState.left.colIdx++ {
		colIdx := input.outCols[o.builderState.left.colIdx]
		outStartIdx = destStartIdx
		out := o.output.ColVec(int(colIdx))
		src := bat.ColVec(int(colIdx))
		colType := input.sourceTypes[colIdx]

		switch colType {
		// {{range .}}
		case _TYPES_T:
			srcCol := src._TemplateType()
			outCol := out._TemplateType()

			if sel != nil {
				// Loop over every group.
				for ; o.builderState.left.groupsIdx < groupsLen; o.builderState.left.groupsIdx++ {
					leftGroup := leftGroups[o.builderState.left.groupsIdx]
					// If curSrcStartIdx is uninitialized, start it at the group's start idx. Otherwise continue where we left off.
					if o.builderState.left.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
						o.builderState.left.curSrcStartIdx = leftGroup.rowStartIdx
					}
					// Loop over every row in the group.
					for ; o.builderState.left.curSrcStartIdx < leftGroup.rowEndIdx; o.builderState.left.curSrcStartIdx++ {
						// Repeat each row numRepeats times.
						for ; o.builderState.left.numRepeatsIdx < leftGroup.numRepeats; o.builderState.left.numRepeatsIdx++ {
							srcStartIdx := o.builderState.left.curSrcStartIdx
							srcEndIdx := srcStartIdx + 1
							if outStartIdx < o.outputBatchSize {

								// TODO (georgeutsin): update template language to automatically generate template function function parameter definitions from expressions passed in.
								t_dest := out
								t_destStartIdx := int(outStartIdx)
								t_src := src
								t_srcStartIdx := srcStartIdx
								t_srcEndIdx := srcEndIdx
								t_sel := sel
								_COPY_WITH_SEL(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx, t_sel)

								outStartIdx++
							} else {
								if o.builderState.left.colIdx == len(input.outCols)-1 {
									o.builderState.left.colIdx = zeroMJCPcolIdx
									return outStartIdx
								}
								o.builderState.left = initialBuilderState
								continue LeftColLoop

							}
						}
						o.builderState.left.numRepeatsIdx = zeroMJCPnumRepeatsIdx
					}
					o.builderState.left.curSrcStartIdx = zeroMJCPcurSrcStartIdx
				}
				o.builderState.left.groupsIdx = zeroMJCPgroupsIdx
			} else {
				// Loop over every group.
				for ; o.builderState.left.groupsIdx < groupsLen; o.builderState.left.groupsIdx++ {
					leftGroup := leftGroups[o.builderState.left.groupsIdx]
					// If curSrcStartIdx is uninitialized, start it at the group's start idx. Otherwise continue where we left off.
					if o.builderState.left.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
						o.builderState.left.curSrcStartIdx = leftGroup.rowStartIdx
					}
					// Loop over every row in the group.
					for ; o.builderState.left.curSrcStartIdx < leftGroup.rowEndIdx; o.builderState.left.curSrcStartIdx++ {
						// Repeat each row numRepeats times.
						for ; o.builderState.left.numRepeatsIdx < leftGroup.numRepeats; o.builderState.left.numRepeatsIdx++ {
							srcStartIdx := o.builderState.left.curSrcStartIdx
							srcEndIdx := srcStartIdx + 1
							if outStartIdx < o.outputBatchSize {

								copy(outCol[outStartIdx:], srcCol[srcStartIdx:srcEndIdx])

								outStartIdx++
							} else {
								if o.builderState.left.colIdx == len(input.outCols)-1 {
									o.builderState.left.colIdx = zeroMJCPcolIdx
									return outStartIdx
								}
								o.builderState.left = initialBuilderState

								continue LeftColLoop
							}
						}
						o.builderState.left.numRepeatsIdx = zeroMJCPnumRepeatsIdx
					}
					o.builderState.left.curSrcStartIdx = zeroMJCPcurSrcStartIdx
				}
				o.builderState.left.groupsIdx = zeroMJCPgroupsIdx
			}
		// {{end}}
		default:
			panic(fmt.Sprintf("unhandled type %d", colType))
		}
		o.builderState.left.setBuilderColumnState(initialBuilderState)
	}

	if len(input.outCols) == 0 {
		outStartIdx = o.calculateOutputCount(leftGroups, groupsLen, outStartIdx)
	}

	o.builderState.left.reset()
	return outStartIdx
}

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

	// Loop over every column.
RightColLoop:
	for ; o.builderState.right.colIdx < len(input.outCols); o.builderState.right.colIdx++ {
		colIdx := input.outCols[o.builderState.right.colIdx]
		outStartIdx := int(destStartIdx)
		out := o.output.ColVec(int(colIdx) + colOffset)
		src := bat.ColVec(int(colIdx))
		colType := input.sourceTypes[colIdx]

		switch colType {
		// {{range .}}
		case _TYPES_T:
			srcCol := src._TemplateType()
			outCol := out._TemplateType()

			if sel != nil {
				// Loop over every group.
				for ; o.builderState.right.groupsIdx < groupsLen; o.builderState.right.groupsIdx++ {
					rightGroup := rightGroups[o.builderState.right.groupsIdx]
					// Repeat every group numRepeats times.
					for ; o.builderState.right.numRepeatsIdx < rightGroup.numRepeats; o.builderState.right.numRepeatsIdx++ {
						if o.builderState.right.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
							o.builderState.right.curSrcStartIdx = rightGroup.rowStartIdx
						}
						toAppend := rightGroup.rowEndIdx - o.builderState.right.curSrcStartIdx
						if outStartIdx+toAppend > int(o.outputBatchSize) {
							toAppend = int(o.outputBatchSize) - outStartIdx
						}

						t_dest := out
						t_destStartIdx := outStartIdx
						t_src := src
						t_srcStartIdx := o.builderState.right.curSrcStartIdx
						t_srcEndIdx := o.builderState.right.curSrcStartIdx + toAppend
						t_sel := sel
						_COPY_WITH_SEL(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx, t_sel)

						outStartIdx += toAppend

						if toAppend < rightGroup.rowEndIdx-o.builderState.right.curSrcStartIdx {
							o.builderState.right.curSrcStartIdx = o.builderState.right.curSrcStartIdx + toAppend
							if o.builderState.right.colIdx == len(input.outCols)-1 {
								o.builderState.right.colIdx = zeroMJCPcolIdx
								return
							}
							o.builderState.right = initialBuilderState
							continue RightColLoop
						}
						o.builderState.right.curSrcStartIdx = zeroMJCPcurSrcStartIdx
					}
					o.builderState.right.numRepeatsIdx = zeroMJCPnumRepeatsIdx
				}
				o.builderState.right.groupsIdx = zeroMJCPgroupsIdx
			} else {
				// Loop over every group.
				for ; o.builderState.right.groupsIdx < groupsLen; o.builderState.right.groupsIdx++ {
					rightGroup := rightGroups[o.builderState.right.groupsIdx]
					// Repeat every group numRepeats times.
					for ; o.builderState.right.numRepeatsIdx < rightGroup.numRepeats; o.builderState.right.numRepeatsIdx++ {
						if o.builderState.right.curSrcStartIdx == zeroMJCPcurSrcStartIdx {
							o.builderState.right.curSrcStartIdx = rightGroup.rowStartIdx
						}
						toAppend := rightGroup.rowEndIdx - o.builderState.right.curSrcStartIdx
						if outStartIdx+toAppend > int(o.outputBatchSize) {
							toAppend = int(o.outputBatchSize) - outStartIdx
						}

						copy(outCol[outStartIdx:], srcCol[o.builderState.right.curSrcStartIdx:o.builderState.right.curSrcStartIdx+toAppend])

						outStartIdx += toAppend

						if toAppend < rightGroup.rowEndIdx-o.builderState.right.curSrcStartIdx {
							o.builderState.right.curSrcStartIdx = o.builderState.right.curSrcStartIdx + toAppend
							if o.builderState.right.colIdx == len(input.outCols)-1 {
								o.builderState.right.colIdx = zeroMJCPcolIdx
								return
							}
							o.builderState.right = initialBuilderState
							continue RightColLoop
						}
						o.builderState.right.curSrcStartIdx = zeroMJCPcurSrcStartIdx
					}
					o.builderState.right.numRepeatsIdx = zeroMJCPnumRepeatsIdx
				}
				o.builderState.right.groupsIdx = zeroMJCPgroupsIdx
			}
		// {{end}}
		default:
			panic(fmt.Sprintf("unhandled type %d", colType))
		}
		o.builderState.right.setBuilderColumnState(initialBuilderState)
	}

	o.builderState.right.reset()
}
