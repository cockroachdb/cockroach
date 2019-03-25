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

	"github.com/cockroachdb/apd"
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
func _ADD_SLICE_TO_COLVEC_WITH_SEL(
	t_dest coldata.Vec,
	t_destStartIdx int,
	t_src coldata.Vec,
	t_srcStartIdx int,
	t_srcEndIdx int,
	t_sel []uint16,
) { // */}}
	// {{define "addSliceToColVecWithSel"}}
	batchSize := t_srcEndIdx - t_srcStartIdx

	toCol := append(t_dest._TemplateType()[:t_destStartIdx], make([]_GOTYPE, batchSize)...)
	fromCol := t_src._TemplateType()

	for i := 0; i < batchSize; i++ {
		toCol[i+t_destStartIdx] = fromCol[t_sel[i+t_srcStartIdx]]
	}

	savedOut.SetCol(toCol)

	if batchSize > 0 {
		savedOut.ExtendNullsWithSel(t_src, uint64(t_destStartIdx), uint16(t_srcStartIdx), uint16(batchSize), t_sel)
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
func _ADD_SLICE_TO_COLVEC(
	t_dest coldata.Vec, t_destStartIdx int, t_src coldata.Vec, t_srcStartIdx int, t_srcEndIdx int,
) { // */}}
	// {{define "addSliceToColVec"}}
	batchSize := t_srcEndIdx - t_srcStartIdx
	outputLen := t_destStartIdx + batchSize

	if outputLen > (len(savedOut._TemplateType())) {
		t_dest.SetCol(append(t_dest._TemplateType()[:t_destStartIdx], t_src._TemplateType()[t_srcStartIdx:t_srcEndIdx]...))
	} else {
		copy(t_dest._TemplateType()[t_destStartIdx:], t_src._TemplateType()[t_srcStartIdx:t_srcEndIdx])
	}

	if batchSize > 0 {
		t_dest.ExtendNulls(src, uint64(t_destStartIdx), uint16(t_srcStartIdx), uint16(batchSize))
	}
	// {{end}}
	// {{/*
}

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
// buildLeftGroups returns the first available index of the output buffer as outStartIdx
// and the number of elements that were saved into state as savedOutCount.
// SIDE EFFECTS: writes into o.output (and o.savedOutput if applicable).
func (o *mergeJoinOp) buildLeftGroups(
	leftGroups []group,
	groupsLen int,
	colOffset int,
	input *mergeJoinInput,
	bat coldata.Batch,
	destStartIdx uint16,
) (outStartIdx uint16, savedOutCount int) {
	sel := bat.Selection()
	savedOutCount = 0
	outStartIdx = destStartIdx
	// Loop over every column.
	for _, colIdx := range input.outCols {
		savedOutCount = 0
		outStartIdx = destStartIdx
		out := o.output.ColVec(int(colIdx))
		savedOut := o.savedOutput.ColVec(int(colIdx))
		src := bat.ColVec(int(colIdx))
		colType := input.sourceTypes[colIdx]

		switch colType {
		// {{range .}}
		case _TYPES_T:
			srcCol := src._TemplateType()
			outCol := out._TemplateType()

			if sel != nil {
				// Loop over every group.
				for i := 0; i < groupsLen; i++ {
					leftGroup := leftGroups[i]
					// Loop over every row in the group.
					for curSrcStartIdx := leftGroup.rowStartIdx; curSrcStartIdx < leftGroup.rowEndIdx; curSrcStartIdx++ {
						// Repeat each row numRepeats times.
						for k := 0; k < leftGroup.numRepeats; k++ {
							srcStartIdx := curSrcStartIdx
							srcEndIdx := curSrcStartIdx + 1
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
								t_dest := savedOut
								t_destStartIdx := o.savedOutputEndIdx + savedOutCount
								t_src := src
								t_srcStartIdx := srcStartIdx
								t_srcEndIdx := srcEndIdx
								t_sel := sel
								_ADD_SLICE_TO_COLVEC_WITH_SEL(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx, t_sel)

								savedOutCount++
							}
						}
					}
				}
			} else {
				// Loop over every group.
				for i := 0; i < groupsLen; i++ {
					leftGroup := leftGroups[i]
					// Loop over every row in the group.
					for curSrcStartIdx := leftGroup.rowStartIdx; curSrcStartIdx < leftGroup.rowEndIdx; curSrcStartIdx++ {
						// Repeat each row numRepeats times.
						for k := 0; k < leftGroup.numRepeats; k++ {
							srcStartIdx := curSrcStartIdx
							srcEndIdx := curSrcStartIdx + 1
							if outStartIdx < o.outputBatchSize {

								copy(outCol[outStartIdx:], srcCol[srcStartIdx:srcEndIdx])

								outStartIdx++
							} else {
								t_dest := savedOut
								t_destStartIdx := o.savedOutputEndIdx + savedOutCount
								t_src := src
								t_srcStartIdx := srcStartIdx
								t_srcEndIdx := srcEndIdx
								_ADD_SLICE_TO_COLVEC(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx)

								savedOutCount++
							}
						}
					}
				}
			}
		// {{end}}
		default:
			panic(fmt.Sprintf("unhandled type %d", colType))
		}
	}

	if len(input.outCols) == 0 {
		outStartIdx = o.calculateOutputCount(leftGroups, groupsLen, outStartIdx)
	}

	return outStartIdx, savedOutCount
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
// SIDE EFFECTS: writes into o.output (and o.savedOutput if applicable).
func (o *mergeJoinOp) buildRightGroups(
	rightGroups []group,
	groupsLen int,
	colOffset int,
	input *mergeJoinInput,
	bat coldata.Batch,
	destStartIdx uint16,
) {
	sel := bat.Selection()
	savedOutputCount := 0
	// Loop over every column.
	for _, colIdx := range input.outCols {
		savedOutputCount = 0
		outStartIdx := int(destStartIdx)
		out := o.output.ColVec(int(colIdx) + colOffset)
		savedOut := o.savedOutput.ColVec(int(colIdx) + colOffset)
		src := bat.ColVec(int(colIdx))
		colType := input.sourceTypes[colIdx]

		switch colType {
		// {{range .}}
		case _TYPES_T:
			srcCol := src._TemplateType()
			outCol := out._TemplateType()

			if sel != nil {
				// Loop over every group.
				for i := 0; i < groupsLen; i++ {
					rightGroup := rightGroups[i]
					// Repeat every group numRepeats times.
					for k := 0; k < rightGroup.numRepeats; k++ {
						toAppend := rightGroup.rowEndIdx - rightGroup.rowStartIdx
						if outStartIdx+toAppend > int(o.outputBatchSize) {
							toAppend = int(o.outputBatchSize) - outStartIdx
						}

						t_dest := out
						t_destStartIdx := outStartIdx
						t_src := src
						t_srcStartIdx := rightGroup.rowStartIdx
						t_srcEndIdx := rightGroup.rowStartIdx + toAppend
						t_sel := sel
						_COPY_WITH_SEL(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx, t_sel)

						if toAppend < rightGroup.rowEndIdx-rightGroup.rowStartIdx {
							t_dest := savedOut
							t_destStartIdx := o.savedOutputEndIdx + savedOutputCount
							t_src := src
							t_srcStartIdx := (rightGroup.rowStartIdx) + toAppend
							t_srcEndIdx := rightGroup.rowEndIdx
							t_sel := sel
							_ADD_SLICE_TO_COLVEC_WITH_SEL(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx, t_sel)
						}

						outStartIdx += toAppend
						savedOutputCount += (rightGroup.rowEndIdx - rightGroup.rowStartIdx) - toAppend
					}
				}
			} else {
				// Loop over every group.
				for i := 0; i < groupsLen; i++ {
					rightGroup := rightGroups[i]
					// Repeat every group numRepeats times.
					for k := 0; k < rightGroup.numRepeats; k++ {
						toAppend := rightGroup.rowEndIdx - rightGroup.rowStartIdx
						if outStartIdx+toAppend > int(o.outputBatchSize) {
							toAppend = int(o.outputBatchSize) - outStartIdx
						}

						copy(outCol[outStartIdx:], srcCol[rightGroup.rowStartIdx:rightGroup.rowStartIdx+toAppend])

						if toAppend < rightGroup.rowEndIdx-rightGroup.rowStartIdx {
							t_dest := savedOut
							t_destStartIdx := o.savedOutputEndIdx + savedOutputCount
							t_src := src
							t_srcStartIdx := (rightGroup.rowStartIdx) + toAppend
							t_srcEndIdx := rightGroup.rowEndIdx
							_ADD_SLICE_TO_COLVEC(t_dest, t_destStartIdx, t_src, t_srcStartIdx, t_srcEndIdx)
						}

						outStartIdx += toAppend
						savedOutputCount += (rightGroup.rowEndIdx - rightGroup.rowStartIdx) - toAppend
					}
				}
			}
		// {{end}}
		default:
			panic(fmt.Sprintf("unhandled type %d", colType))
		}
	}
}
