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

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// {{/*
// Declarations to make the template compile properly.

// _GOTYPE is the template variable.
type _GOTYPE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the second input == the third input.
func _ASSIGN_EQ(_, _, _, _, _, _ interface{}) int {
	colexecerror.InternalError("")
}

// _ASSIGN_CMP is the template equality function for assigning the first input
// to the result of comparing the second input to the third input which returns
// an integer. That integer is:
// - negative if left < right
// - zero if left == right
// - positive if left > right.
func _ASSIGN_CMP(_, _, _, _, _ interface{}) int {
	colexecerror.VectorizedInternalPanic("")
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

// _JOIN_TYPE is used in place of the string "$.JoinType", since that isn't
// valid go code.
const _JOIN_TYPE = 0

// */}}

type mergeJoin_JOIN_TYPE_STRINGOp struct {
	*mergeJoinBase
}

var _ InternalMemoryOperator = &mergeJoin_JOIN_TYPE_STRINGOp{}

// {{/*
// This code snippet is the "meat" of the probing phase.
func _PROBE_SWITCH(
	_JOIN_TYPE joinTypeInfo, _SEL_PERMUTATION selPermutation, _L_HAS_NULLS bool, _R_HAS_NULLS bool,
) { // */}}
	// {{define "probeSwitch"}}
	// {{$sel := $.SelPermutation}}
	switch lVec.CanonicalTypeFamily() {
	// {{range $overload := $.Global.Overloads}}
	case _CANONICAL_TYPE_FAMILY:
		switch colType.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			lKeys := lVec.TemplateType()
			rKeys := rVec.TemplateType()
			var (
				lGroup, rGroup   group
				cmp              int
				match            bool
				lVal, rVal       _GOTYPE
				lSelIdx, rSelIdx int
			)

			for o.groups.nextGroupInCol(&lGroup, &rGroup) {
				curLIdx := lGroup.rowStartIdx
				curRIdx := rGroup.rowStartIdx
				curLEndIdx := lGroup.rowEndIdx
				curREndIdx := rGroup.rowEndIdx
				areGroupsProcessed := false
				_LEFT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE)
				_RIGHT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE)
				// Expand or filter each group based on the current equality column.
				for curLIdx < curLEndIdx && curRIdx < curREndIdx && !areGroupsProcessed {
					cmp = 0
					// {{if _L_HAS_NULLS}}
					lNull := lVec.Nulls().NullAt(_L_SEL_IND)
					// {{end}}
					// {{if _R_HAS_NULLS}}
					rNull := rVec.Nulls().NullAt(_R_SEL_IND)
					// {{end}}

					// {{if _JOIN_TYPE.IsSetOp}}
					// {{/*
					//     Set operations allow null equality, so we handle
					//     NULLs first.
					// */}}
					// {{if _L_HAS_NULLS}}
					if lNull {
						// {{/* If we have NULL on the left, then it is smaller than the right value. */}}
						cmp = -1
					}
					// {{end}}
					// {{if _R_HAS_NULLS}}
					if rNull {
						// {{/* If we have NULL on the right, then it is smaller than the left value. */}}
						cmp = 1
					}
					// {{end}}
					// {{if _L_HAS_NULLS}}
					var nullMatch bool
					// {{/* Remove unused warning for some code paths of INTERSECT ALL join. */}}
					_ = nullMatch
					// {{if _R_HAS_NULLS}}
					// {{/* Both vectors might have nulls. */}}
					// If we have a NULL match, it will take precedence over
					// cmp value set above.
					nullMatch = lNull && rNull
					// {{end}}
					// {{end}}
					// {{else}}
					// {{/*
					//     Non-set operation joins do not allow null equality,
					//     so if either value is NULL, the tuples are not
					//     matches.
					// */}}
					// TODO(yuzefovich): we can advance both sides if both are
					// NULL.
					// {{if _L_HAS_NULLS}}
					if lNull {
						_NULL_FROM_LEFT_SWITCH(_JOIN_TYPE)
						curLIdx++
						continue
					}
					// {{end}}
					// {{if _R_HAS_NULLS}}
					if rNull {
						_NULL_FROM_RIGHT_SWITCH(_JOIN_TYPE)
						curRIdx++
						continue
					}
					// {{end}}
					// {{end}}

					// {{if _JOIN_TYPE.IsSetOp}}
					// {{if and _L_HAS_NULLS _R_HAS_NULLS}}
					if nullMatch {
						// We have a null match, so two values are equal.
						cmp = 0
					} else
					// {{end}}
					// {{end}}
					{
						// {{if _JOIN_TYPE.IsSetOp}}
						// {{if or _L_HAS_NULLS _R_HAS_NULLS}}
						// {{/*
						//     For set operation joins we might have set 'cmp'
						//     to non-zero value above if we had a null mismatch.
						//     In such scenario we already know that the values
						//     are different and we know which side to advance.
						// */}}
						if cmp == 0 {
							// {{end}}
							// {{end}}

							lSelIdx = _L_SEL_IND
							lVal = execgen.UNSAFEGET(lKeys, lSelIdx)
							rSelIdx = _R_SEL_IND
							rVal = execgen.UNSAFEGET(rKeys, rSelIdx)
							_ASSIGN_CMP(cmp, lVal, rVal, lKeys, rKeys)

							// {{if _JOIN_TYPE.IsSetOp}}
							// {{if or _L_HAS_NULLS _R_HAS_NULLS}}
						}
						// {{end}}
						// {{end}}
					}

					if cmp == 0 {
						// Find the length of the groups on each side.
						lGroupLength, rGroupLength := 1, 1
						// If a group ends before the end of the probing batch,
						// then we know it is complete.
						lComplete := curLEndIdx < o.proberState.lLength
						rComplete := curREndIdx < o.proberState.rLength
						beginLIdx, beginRIdx := curLIdx, curRIdx
						curLIdx++
						curRIdx++

						// Find the length of the group on the left.
						for curLIdx < curLEndIdx {
							// {{if _JOIN_TYPE.IsSetOp}}
							// {{if and _L_HAS_NULLS _R_HAS_NULLS}}
							if nullMatch {
								// {{/*
								//     We have a NULL match, so we only
								//     extend the left group if we have a
								//     NULL element.
								// */}}
								if !lVec.Nulls().NullAt(_L_SEL_IND) {
									lComplete = true
									break
								}
							} else
							// {{end}}
							// {{end}}
							{
								// {{if _L_HAS_NULLS}}
								if lVec.Nulls().NullAt(_L_SEL_IND) {
									lComplete = true
									break
								}
								// {{end}}
								lSelIdx = _L_SEL_IND
								newLVal := execgen.UNSAFEGET(lKeys, lSelIdx)
								_ASSIGN_EQ(match, newLVal, lVal, _, lKeys, lKeys)
								if !match {
									lComplete = true
									break
								}
							}
							lGroupLength++
							curLIdx++
						}

						// Find the length of the group on the right.
						for curRIdx < curREndIdx {
							// {{if _JOIN_TYPE.IsSetOp}}
							// {{if and _L_HAS_NULLS _R_HAS_NULLS}}
							if nullMatch {
								// {{/*
								//     We have a NULL match, so we only
								//     extend the right group if we have a
								//     NULL element.
								// */}}
								if !rVec.Nulls().NullAt(_R_SEL_IND) {
									rComplete = true
									break
								}
							} else
							// {{end}}
							// {{end}}
							{
								// {{if _R_HAS_NULLS}}
								if rVec.Nulls().NullAt(_R_SEL_IND) {
									rComplete = true
									break
								}
								// {{end}}
								rSelIdx = _R_SEL_IND
								newRVal := execgen.UNSAFEGET(rKeys, rSelIdx)
								_ASSIGN_EQ(match, newRVal, rVal, _, rKeys, rKeys)
								if !match {
									rComplete = true
									break
								}
							}
							rGroupLength++
							curRIdx++
						}

						// Last equality column and either group is incomplete. Save state
						// and have it handled in the next iteration.
						if eqColIdx == len(o.left.eqCols)-1 && (!lComplete || !rComplete) {
							o.appendToBufferedGroup(ctx, &o.left, o.proberState.lBatch, lSel, beginLIdx, lGroupLength)
							o.proberState.lIdx = lGroupLength + beginLIdx
							o.appendToBufferedGroup(ctx, &o.right, o.proberState.rBatch, rSel, beginRIdx, rGroupLength)
							o.proberState.rIdx = rGroupLength + beginRIdx

							o.groups.finishedCol()
							break EqLoop
						}

						if eqColIdx < len(o.left.eqCols)-1 {
							o.groups.addGroupsToNextCol(beginLIdx, lGroupLength, beginRIdx, rGroupLength)
						} else {
							// {{if _JOIN_TYPE.IsLeftSemi}}
							leftSemiGroupLength := lGroupLength
							// {{if _JOIN_TYPE.IsSetOp}}
							// For INTERSECT ALL join we add a left semi group
							// of length min(lGroupLength, rGroupLength).
							if rGroupLength < lGroupLength {
								leftSemiGroupLength = rGroupLength
							}
							// {{end}}
							o.groups.addLeftSemiGroup(beginLIdx, leftSemiGroupLength)
							// {{else if _JOIN_TYPE.IsLeftAnti}}
							// {{if _JOIN_TYPE.IsSetOp}}
							// For EXCEPT ALL join we add (lGroupLength - rGroupLength) number
							// (if positive) of unmatched left groups.
							for leftUnmatchedTupleIdx := beginLIdx + rGroupLength; leftUnmatchedTupleIdx < beginLIdx+lGroupLength; leftUnmatchedTupleIdx++ {
								// Right index here doesn't matter.
								o.groups.addLeftUnmatchedGroup(leftUnmatchedTupleIdx, beginRIdx)
							}
							// {{else}}
							// With LEFT ANTI join, we are only interested in unmatched tuples
							// from the left, and all tuples in the current group have a match.
							// {{end}}
							// {{else}}
							// Neither group ends with the batch, so add the group to the
							// circular buffer.
							o.groups.addGroupsToNextCol(beginLIdx, lGroupLength, beginRIdx, rGroupLength)
							// {{end}}
						}
					} else { // mismatch
						// The line below is a compact form of the following:
						//   incrementLeft :=
						//    (cmp < 0 && o.left.directions[eqColIdx] == execinfrapb.Ordering_Column_ASC) ||
						//	  (cmp > 0 && o.left.directions[eqColIdx] == execinfrapb.Ordering_Column_DESC).
						incrementLeft := cmp < 0 == (o.left.directions[eqColIdx] == execinfrapb.Ordering_Column_ASC)
						if incrementLeft {
							curLIdx++
							// {{if _L_HAS_NULLS}}
							_INCREMENT_LEFT_SWITCH(_JOIN_TYPE, _SEL_ARG, true)
							// {{else}}
							_INCREMENT_LEFT_SWITCH(_JOIN_TYPE, _SEL_ARG, false)
							// {{end}}
						} else {
							curRIdx++
							// {{if _R_HAS_NULLS}}
							_INCREMENT_RIGHT_SWITCH(_JOIN_TYPE, _SEL_ARG, true)
							// {{else}}
							_INCREMENT_RIGHT_SWITCH(_JOIN_TYPE, _SEL_ARG, false)
							// {{end}}
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
		}
		// {{end}}
	default:
		colexecerror.InternalError(fmt.Sprintf("unhandled type %s", colType))
	}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes an unmatched group from the left.
func _LEFT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "leftUnmatchedGroupSwitch"}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Unmatched groups are not possible with INNER, LEFT SEMI, and INTERSECT
	// ALL joins (the latter has IsLeftSemi == true), so there is nothing to do
	// here.
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	if lGroup.unmatched {
		if curLIdx+1 != curLEndIdx {
			colexecerror.InternalError(fmt.Sprintf("unexpectedly length %d of the left unmatched group is not 1", curLEndIdx-curLIdx))
		}
		// The row already does not have a match, so we don't need to do any
		// additional processing.
		o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
		curLIdx++
		areGroupsProcessed = true
	}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	// {{/*
	// Unmatched groups from the left are not possible with RIGHT OUTER join, so
	// there is nothing to do here.
	// */}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes an unmatched group from the right.
func _RIGHT_UNMATCHED_GROUP_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "rightUnmatchedGroupSwitch"}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Unmatched groups are not possible with INNER, LEFT SEMI, and INTERSECT
	// ALL joins (the latter has IsLeftSemi == true), so there is nothing to do
	// here.
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	// {{/*
	// Unmatched groups from the right are not possible with LEFT OUTER, LEFT
	// ANTI, and EXCEPT ALL joins (the latter has IsLeftAnti == true), so there
	// is nothing to do here.
	// */}}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	if rGroup.unmatched {
		if curRIdx+1 != curREndIdx {
			colexecerror.InternalError(fmt.Sprintf("unexpectedly length %d of the right unmatched group is not 1", curREndIdx-curRIdx))
		}
		// The row already does not have a match, so we don't need to do any
		// additional processing.
		o.groups.addRightOuterGroup(curLIdx, curRIdx)
		curRIdx++
		areGroupsProcessed = true
	}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do if we encounter null in the equality
// column from the left input. Note that the case of Null equality *must* be
// checked separately.
func _NULL_FROM_LEFT_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "nullFromLeftSwitch"}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Nulls coming from the left input are ignored in INNER and LEFT SEMI
	// joins.
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	// {{/*
	// Nulls coming from the left input are ignored in RIGHT OUTER join.
	// */}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do if we encounter null in the equality
// column from the right input. Note that the case of Null equality *must* be
// checked separately.
func _NULL_FROM_RIGHT_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "nullFromRightSwitch"}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Nulls coming from the right input are ignored in INNER and LEFT SEMI
	// joins.
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	// {{/*
	// Nulls coming from the right input are ignored in LEFT OUTER and LEFT
	// ANTI joins.
	// */}}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	o.groups.addRightOuterGroup(curLIdx, curRIdx)
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do when - while looking for a match
// between two inputs - we need to advance the left side, i.e. it decides how
// to handle an unmatched tuple from the left.
func _INCREMENT_LEFT_SWITCH(
	_JOIN_TYPE joinTypeInfo, _SEL_PERMUTATION selPermutation, _L_HAS_NULLS bool,
) { // */}}
	// {{define "incrementLeftSwitch"}}
	// {{$sel := $.SelPermutation}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Unmatched tuple from the left source is not outputted in INNER, LEFT
	// SEMI, and INTERSECT ALL joins (the latter has IsLeftSemi == true).
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	// All the rows on the left within the current group will not get a match on
	// the right, so we're adding each of them as a left unmatched group.
	o.groups.addLeftUnmatchedGroup(curLIdx-1, curRIdx)
	for curLIdx < curLEndIdx {
		// {{/*
		//     EXCEPT ALL join allows NULL equality, so we have special
		//     treatment of NULLs.
		// */}}
		// {{if _L_HAS_NULLS}}
		// {{if _JOIN_TYPE.IsSetOp}}
		newLValNull := lVec.Nulls().NullAt(_L_SEL_IND)
		if lNull != newLValNull {
			// We have a null mismatch, so we've reached the end of the current
			// group on the left.
			break
		} else if newLValNull && lNull {
			nullMatch = true
		} else {
			nullMatch = false
		}
		// {{else}}
		if lVec.Nulls().NullAt(_L_SEL_IND) {
			break
		}
		// {{end}}
		// {{end}}

		// {{if and _JOIN_TYPE.IsSetOp _L_HAS_NULLS}}
		// {{/*
		//     We have checked for null equality above and set nullMatch to the
		//     correct value. If it is true, then both the old and the new
		//     values are NULL, so there is no further comparison needed.
		// */}}
		if !nullMatch {
			// {{end}}
			lSelIdx = _L_SEL_IND
			// {{with .Global}}
			newLVal := execgen.UNSAFEGET(lKeys, lSelIdx)
			_ASSIGN_EQ(match, newLVal, lVal, _, lKeys, lKeys)
			// {{end}}
			if !match {
				break
			}
			// {{if and _JOIN_TYPE.IsSetOp _L_HAS_NULLS}}
		}
		// {{end}}
		o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
		curLIdx++
	}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	// {{/*
	// Unmatched tuple from the left source is not outputted in RIGHT OUTER join.
	// */}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet decides what to do when - while looking for a match
// between two inputs - we need to advance the right side, i.e. it decides how
// to handle an unmatched tuple from the right.
func _INCREMENT_RIGHT_SWITCH(
	_JOIN_TYPE joinTypeInfo, _SEL_PERMUTATION selPermutation, _R_HAS_NULLS bool,
) { // */}}
	// {{define "incrementRightSwitch"}}
	// {{$sel := $.SelPermutation}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Unmatched tuple from the right source is not outputted in INNER, LEFT
	// SEMI, and INTERSECT ALL joins (the latter has IsLeftSemi == true).
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	// {{/*
	// Unmatched tuple from the right source is not outputted in LEFT OUTER,
	// LEFT ANTI, and EXCEPT ALL joins (the latter has IsLeftAnti == true).
	// */}}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	// All the rows on the right within the current group will not get a match on
	// the left, so we're adding each of them as a right outer group.
	o.groups.addRightOuterGroup(curLIdx, curRIdx-1)
	for curRIdx < curREndIdx {
		// {{if _R_HAS_NULLS}}
		if rVec.Nulls().NullAt(_R_SEL_IND) {
			break
		}
		// {{end}}
		rSelIdx = _R_SEL_IND
		// {{with .Global}}
		newRVal := execgen.UNSAFEGET(rKeys, rSelIdx)
		_ASSIGN_EQ(match, newRVal, rVal, _, rKeys, rKeys)
		// {{end}}
		if !match {
			break
		}
		o.groups.addRightOuterGroup(curLIdx, curRIdx)
		curRIdx++
	}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// This code snippet processes all but last groups in a column after we have
// reached the end of either the left or right group.
func _PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "processNotLastGroupInColumnSwitch"}}
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	// {{/*
	// Nothing to do here since an unmatched tuple is omitted.
	// */}}
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	if !o.groups.isLastGroupInCol() && !areGroupsProcessed {
		// The current group is not the last one within the column, so it cannot be
		// extended into the next batch, and we need to process it right now. Any
		// unprocessed row in the left group will not get a match, so each one of
		// them becomes a new unmatched group with a corresponding null group.
		for curLIdx < curLEndIdx {
			o.groups.addLeftUnmatchedGroup(curLIdx, curRIdx)
			curLIdx++
		}
	}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
	if !o.groups.isLastGroupInCol() && !areGroupsProcessed {
		// The current group is not the last one within the column, so it cannot be
		// extended into the next batch, and we need to process it right now. Any
		// unprocessed row in the right group will not get a match, so each one of
		// them becomes a new unmatched group with a corresponding null group.
		for curRIdx < curREndIdx {
			o.groups.addRightOuterGroup(curLIdx, curRIdx)
			curRIdx++
		}
	}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{range $sel := $.SelPermutations}}
func (o *mergeJoin_JOIN_TYPE_STRINGOp) probeBodyLSel_IS_L_SELRSel_IS_R_SEL(ctx context.Context) {
	lSel := o.proberState.lBatch.Selection()
	rSel := o.proberState.rBatch.Selection()
EqLoop:
	for eqColIdx := 0; eqColIdx < len(o.left.eqCols); eqColIdx++ {
		leftColIdx := o.left.eqCols[eqColIdx]
		rightColIdx := o.right.eqCols[eqColIdx]
		lVec := o.proberState.lBatch.ColVec(int(leftColIdx))
		rVec := o.proberState.rBatch.ColVec(int(rightColIdx))
		leftType := o.left.sourceTypes[leftColIdx]
		rightType := o.right.sourceTypes[rightColIdx]
		colType := leftType
		// Merge joiner only supports the case when the physical types in the
		// equality columns in both inputs are the same. If that is not the case,
		// we need to cast one of the vectors to another's physical type putting
		// the result of the cast into a temporary vector that is used instead of
		// the original.
		leftCanonicalTypeFamily := o.left.canonicalTypeFamilies[leftColIdx]
		isNumeric := leftCanonicalTypeFamily == types.IntFamily ||
			leftCanonicalTypeFamily == types.FloatFamily ||
			leftCanonicalTypeFamily == types.DecimalFamily
		if isNumeric && !leftType.Identical(rightType) {
			castLeftToRight := false
			// There is a hierarchy of valid casts:
			//   Int16 -> Int32 -> Int64 -> Float64 -> Decimal
			// and the cast is valid if 'fromType' is mentioned before 'toType'
			// in this chain.
			switch leftCanonicalTypeFamily {
			case types.IntFamily:
				switch leftType.Width() {
				case 16:
					castLeftToRight = true
				case 32:
					castLeftToRight = !rightType.Identical(types.Int2)
				case 64:
					castLeftToRight = !rightType.Identical(types.Int2) && !rightType.Identical(types.Int4)
				}
			case types.FloatFamily:
				castLeftToRight = o.right.canonicalTypeFamilies[rightColIdx] == types.DecimalFamily
			}

			toType := leftType
			if castLeftToRight {
				toType = rightType
			}
			var tempVec coldata.Vec
			for _, vec := range o.scratch.tempVecs {
				if vec.Type().Identical(toType) {
					tempVec = vec
					break
				}
			}
			if tempVec == nil {
				tempVec = o.unlimitedAllocator.NewMemColumn(toType, coldata.BatchSize())
				o.scratch.tempVecs = append(o.scratch.tempVecs, tempVec)
			} else {
				tempVec.Nulls().UnsetNulls()
				if tempVec.CanonicalTypeFamily() == types.BytesFamily {
					tempVec.Bytes().Reset()
				}
			}
			if castLeftToRight {
				cast(lVec, tempVec, o.proberState.lBatch.Length(), lSel)
				lVec = tempVec
				colType = rightType
			} else {
				cast(rVec, tempVec, o.proberState.rBatch.Length(), rSel)
				rVec = tempVec
			}
		}
		if lVec.MaybeHasNulls() {
			if rVec.MaybeHasNulls() {
				_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, true, true)
			} else {
				_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, true, false)
			}
		} else {
			if rVec.MaybeHasNulls() {
				_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, false, true)
			} else {
				_PROBE_SWITCH(_JOIN_TYPE, _SEL_ARG, false, false)
			}
		}
		// Look at the groups associated with the next equality column by moving
		// the circular buffer pointer up.
		o.groups.finishedCol()
	}
}

// {{end}}

// {{/*
// This code snippet builds the output corresponding to the left side (i.e. is
// the main body of buildLeftGroupsFromBatch()).
func _LEFT_SWITCH(_JOIN_TYPE joinTypeInfo, _HAS_SELECTION bool, _HAS_NULLS bool) { // */}}
	// {{define "leftSwitch"}}
	switch input.canonicalTypeFamilies[colIdx] {
	// {{range $.Global.Overloads}}
	case _CANONICAL_TYPE_FAMILY:
		switch input.sourceTypes[colIdx].Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			var srcCol _GOTYPESLICE
			if src != nil {
				srcCol = src.TemplateType()
			}
			outCol := out.TemplateType()
			var val _GOTYPE
			var srcStartIdx int

			// Loop over every group.
			for ; o.builderState.left.groupsIdx < len(leftGroups); o.builderState.left.groupsIdx++ {
				leftGroup := &leftGroups[o.builderState.left.groupsIdx]
				// {{if _JOIN_TYPE.IsLeftAnti}}
				// {{/*
				// With LEFT ANTI and EXCEPT ALL joins (the latter has
				// IsLeftAnti == true) we want to emit output corresponding only to
				// unmatched tuples, so we're skipping all "matched" groups.
				// */}}
				if !leftGroup.unmatched {
					continue
				}
				// {{end}}
				// If curSrcStartIdx is uninitialized, start it at the group's start idx.
				// Otherwise continue where we left off.
				if o.builderState.left.curSrcStartIdx == zeroMJCPCurSrcStartIdx {
					o.builderState.left.curSrcStartIdx = leftGroup.rowStartIdx
				}
				// Loop over every row in the group.
				for ; o.builderState.left.curSrcStartIdx < leftGroup.rowEndIdx; o.builderState.left.curSrcStartIdx++ {
					// Repeat each row numRepeats times.
					srcStartIdx = o.builderState.left.curSrcStartIdx
					// {{if _HAS_SELECTION}}
					srcStartIdx = sel[srcStartIdx]
					// {{end}}

					repeatsLeft := leftGroup.numRepeats - o.builderState.left.numRepeatsIdx
					toAppend := repeatsLeft
					if outStartIdx+toAppend > outputBatchSize {
						toAppend = outputBatchSize - outStartIdx
					}

					// {{if _JOIN_TYPE.IsRightOuter}}
					// {{/*
					// Null groups on the left can only occur with RIGHT OUTER and FULL
					// OUTER joins for both of which IsRightOuter is true. For other joins,
					// we're omitting this check.
					// */}}
					if leftGroup.nullGroup {
						out.Nulls().SetNullRange(outStartIdx, outStartIdx+toAppend)
						outStartIdx += toAppend
					} else
					// {{end}}
					{
						// {{if _HAS_NULLS}}
						if src.Nulls().NullAt(srcStartIdx) {
							out.Nulls().SetNullRange(outStartIdx, outStartIdx+toAppend)
							outStartIdx += toAppend
						} else
						// {{end}}
						{
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
						if colIdx == len(input.sourceTypes)-1 {
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
		}
		// {{end}}
	default:
		colexecerror.InternalError(fmt.Sprintf("unhandled type %s", input.sourceTypes[colIdx].String()))
	}
	// {{end}}
	// {{/*
}

// */}}

// buildLeftGroupsFromBatch takes a []group and expands each group into the
// output by repeating each row in the group numRepeats times. For example,
// given an input table:
//  L1 |  L2
//  --------
//  1  |  a
//  1  |  b
// and leftGroups = [{startIdx: 0, endIdx: 2, numRepeats: 3}]
// then buildLeftGroupsFromBatch expands this to
//  L1 |  L2
//  --------
//  1  |  a
//  1  |  a
//  1  |  a
//  1  |  b
//  1  |  b
//  1  |  b
// Note: this is different from buildRightGroupsFromBatch in that each row of
// group is repeated numRepeats times, instead of a simple copy of the group as
// a whole.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) buildLeftGroupsFromBatch(
	leftGroups []group, input *mergeJoinInput, batch coldata.Batch, destStartIdx int,
) {
	sel := batch.Selection()
	initialBuilderState := o.builderState.left
	outputBatchSize := o.outputBatchSize
	o.unlimitedAllocator.PerformOperation(
		o.output.ColVecs()[:len(input.sourceTypes)],
		func() {
			// Loop over every column.
		LeftColLoop:
			for colIdx := range input.sourceTypes {
				outStartIdx := destStartIdx
				out := o.output.ColVec(colIdx)
				var src coldata.Vec
				if batch.Length() > 0 {
					src = batch.ColVec(colIdx)
				}

				if sel != nil {
					if src != nil && src.MaybeHasNulls() {
						_LEFT_SWITCH(_JOIN_TYPE, true, true)
					} else {
						_LEFT_SWITCH(_JOIN_TYPE, true, false)
					}
				} else {
					if src != nil && src.MaybeHasNulls() {
						_LEFT_SWITCH(_JOIN_TYPE, false, true)
					} else {
						_LEFT_SWITCH(_JOIN_TYPE, false, false)
					}
				}
				o.builderState.left.setBuilderColumnState(initialBuilderState)
			}
			o.builderState.left.reset()
		},
	)
}

// buildLeftBufferedGroup is similar to buildLeftGroupsFromBatch, but it
// builds the output columns corresponding to the left input based on the
// buffered group. The goal is to repeat each row from the left buffered group
// leftGroup.numRepeats times.
// Note that for non-set operation joins all other fields of leftGroup are
// ignored because, by definition, all rows in the buffered group are part of
// leftGroup (i.e. we don't need to look at rowStartIdx and rowEndIdx). Also,
// all rows in the buffered group do have a match, so the group can neither be
// "nullGroup" nor "unmatched".
// This function does pay attention to rowEndIdx field for set operation joins:
// only the first rowEndIdx will be output. For performance reasons we choose
// to output the first rows (for both INTERSECT ALL and EXCEPT ALL joins we
// need to output exactly rowEndIdx different rows from the left, but the
// choice of rows can be arbitrary).
func (o *mergeJoin_JOIN_TYPE_STRINGOp) buildLeftBufferedGroup(
	ctx context.Context,
	leftGroup group,
	input *mergeJoinInput,
	bufferedGroup mjBufferedGroup,
	destStartIdx int,
) {
	var err error
	currentBatch := o.builderState.lBufferedGroupBatch
	if currentBatch == nil {
		currentBatch, err = bufferedGroup.dequeue(ctx)
		if err != nil {
			colexecerror.InternalError(err)
		}
		o.builderState.lBufferedGroupBatch = currentBatch
		o.builderState.left.curSrcStartIdx = 0
		o.builderState.left.numRepeatsIdx = 0
	}
	initialBuilderState := o.builderState.left
	o.unlimitedAllocator.PerformOperation(
		o.output.ColVecs()[:len(input.sourceTypes)],
		func() {
			batchLength := currentBatch.Length()
			for batchLength > 0 {
				// Loop over every column.
			LeftColLoop:
				for colIdx := range input.sourceTypes {
					outStartIdx := destStartIdx
					src := currentBatch.ColVec(colIdx)
					out := o.output.ColVec(colIdx)
					switch input.canonicalTypeFamilies[colIdx] {
					// {{range $.Overloads}}
					case _CANONICAL_TYPE_FAMILY:
						switch input.sourceTypes[colIdx].Width() {
						// {{range .WidthOverloads}}
						case _TYPE_WIDTH:
							srcCol := src.TemplateType()
							outCol := out.TemplateType()
							var val _GOTYPE
							// {{if _JOIN_TYPE.IsSetOp}}
							// Loop over every row in the group until we hit
							// rowEndIdx number of rows.
							// {{else}}
							// Loop over every row in the group.
							// {{end}}
							for ; o.builderState.left.curSrcStartIdx < batchLength; o.builderState.left.curSrcStartIdx++ {
								// Repeat each row numRepeats times.
								// {{/*
								// TODO(yuzefovich): we can optimize this code for
								// LEFT SEMI, INTERSECT ALL, and EXCEPT ALL joins
								// because in that case numRepeats is always 1.
								// */}}
								srcStartIdx := o.builderState.left.curSrcStartIdx
								repeatsLeft := leftGroup.numRepeats - o.builderState.left.numRepeatsIdx
								toAppend := repeatsLeft
								if outStartIdx+toAppend > o.outputBatchSize {
									toAppend = o.outputBatchSize - outStartIdx
								}

								// {{if _JOIN_TYPE.IsSetOp}}
								if o.builderState.left.setOpLeftSrcIdx == leftGroup.rowEndIdx {
									// We have fully materialized first rowEndIdx
									// rows in the current column, so we need to
									// either transition to the next column or exit.
									// We can accomplish this by setting toAppend
									// to 0.
									toAppend = 0
								}
								o.builderState.left.setOpLeftSrcIdx += toAppend
								// {{end}}

								// {{/*
								// TODO(yuzefovich): check whether it is beneficial
								// to have 'if toAppend > 0' check here.
								// */}}
								if src.Nulls().NullAt(srcStartIdx) {
									out.Nulls().SetNullRange(outStartIdx, outStartIdx+toAppend)
									outStartIdx += toAppend
								} else {
									val = execgen.UNSAFEGET(srcCol, srcStartIdx)
									for i := 0; i < toAppend; i++ {
										execgen.SET(outCol, outStartIdx, val)
										outStartIdx++
									}
								}

								if toAppend < repeatsLeft {
									// We didn't materialize all the rows in the current batch, so
									// we move to the next column.
									if colIdx == len(input.sourceTypes)-1 {
										// This is the last column, so we update the builder state
										// and exit.
										o.builderState.left.numRepeatsIdx += toAppend
										// {{if _JOIN_TYPE.IsSetOp}}
										// {{/*
										//     For non-set operation join the builder state is reset once the
										//     buffered group has been fully built (at the very bottom of this
										//     function), but we might be short-circuiting right now because
										//     set operation joins can have partially-built groups.
										// */}}
										if o.builderState.left.setOpLeftSrcIdx == leftGroup.rowEndIdx {
											o.builderState.lBufferedGroupBatch = nil
											o.builderState.left.reset()
										}
										// {{end}}
										return
									}
									// We need to start building the next column
									// with the same initial builder state as the
									// current column.
									o.builderState.left.setBuilderColumnState(initialBuilderState)
									continue LeftColLoop
								}
								// We fully processed the current row, and before moving on to the
								// next one, we need to reset numRepeatsIdx (so that the next row
								// would be repeated leftGroup.numRepeats times).
								o.builderState.left.numRepeatsIdx = 0
							}
							// {{end}}
						}
					// {{end}}
					default:
						colexecerror.InternalError(fmt.Sprintf("unhandled type %s", input.sourceTypes[colIdx].String()))
					}
					if colIdx == len(input.sourceTypes)-1 {
						// We have appended some tuples into the output batch from the current
						// batch (the latter is now fully processed), so we need to adjust
						// destStartIdx accordingly for the next batch.
						destStartIdx = outStartIdx
					} else {
						o.builderState.left.setBuilderColumnState(initialBuilderState)
					}
				}
				// We have processed all tuples in the current batch from the
				// buffered group, so we need to dequeue the next one.
				o.unlimitedAllocator.ReleaseBatch(currentBatch)
				currentBatch, err = bufferedGroup.dequeue(ctx)
				if err != nil {
					colexecerror.InternalError(err)
				}
				o.builderState.lBufferedGroupBatch = currentBatch
				batchLength = currentBatch.Length()
				// We have transitioned to building from a new batch, so we
				// need to update the builder state to build from the beginning
				// of the new batch.
				o.builderState.left.curSrcStartIdx = 0
				o.builderState.left.numRepeatsIdx = 0
				// We also need to update 'initialBuilderState' so that the
				// builder state gets reset correctly in-between different
				// columns in the loop above.
				initialBuilderState = o.builderState.left
			}
			o.builderState.lBufferedGroupBatch = nil
			o.builderState.left.reset()
		},
	)
}

// {{/*
// This code snippet builds the output corresponding to the right side (i.e. is
// the main body of buildRightGroupsFromBatch()).
func _RIGHT_SWITCH(_JOIN_TYPE joinTypeInfo, _HAS_SELECTION bool, _HAS_NULLS bool) { // */}}
	// {{define "rightSwitch"}}

	switch input.canonicalTypeFamilies[colIdx] {
	// {{range $.Global.Overloads}}
	case _CANONICAL_TYPE_FAMILY:
		switch input.sourceTypes[colIdx].Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			var srcCol _GOTYPESLICE
			if src != nil {
				srcCol = src.TemplateType()
			}
			outCol := out.TemplateType()

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

					// {{if _JOIN_TYPE.IsLeftOuter}}
					// {{/*
					// Null groups on the right can only occur with LEFT OUTER and FULL
					// OUTER joins for both of which IsLeftOuter is true. For other joins,
					// we're omitting this check.
					// */}}
					if rightGroup.nullGroup {
						out.Nulls().SetNullRange(outStartIdx, outStartIdx+toAppend)
					} else
					// {{end}}
					{
						// Optimization in the case that group length is 1, use assign
						// instead of copy.
						if toAppend == 1 {
							// {{if _HAS_SELECTION}}
							// {{if _HAS_NULLS}}
							if src.Nulls().NullAt(sel[o.builderState.right.curSrcStartIdx]) {
								out.Nulls().SetNull(outStartIdx)
							} else
							// {{end}}
							{
								v := execgen.UNSAFEGET(srcCol, sel[o.builderState.right.curSrcStartIdx])
								execgen.SET(outCol, outStartIdx, v)
							}
							// {{else}}
							// {{if _HAS_NULLS}}
							if src.Nulls().NullAt(o.builderState.right.curSrcStartIdx) {
								out.Nulls().SetNull(outStartIdx)
							} else
							// {{end}}
							{
								v := execgen.UNSAFEGET(srcCol, o.builderState.right.curSrcStartIdx)
								execgen.SET(outCol, outStartIdx, v)
							}
							// {{end}}
						} else {
							out.Copy(
								coldata.CopySliceArgs{
									SliceArgs: coldata.SliceArgs{
										Src:         src,
										Sel:         sel,
										DestIdx:     outStartIdx,
										SrcStartIdx: o.builderState.right.curSrcStartIdx,
										SrcEndIdx:   o.builderState.right.curSrcStartIdx + toAppend,
									},
								},
							)
						}
					}

					outStartIdx += toAppend

					// If we haven't materialized all the rows from the group, then we are
					// done with the current column.
					if toAppend < rightGroup.rowEndIdx-o.builderState.right.curSrcStartIdx {
						// If it's the last column, save state and return.
						if colIdx == len(input.sourceTypes)-1 {
							o.builderState.right.curSrcStartIdx += toAppend
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
		}
	// {{end}}
	default:
		colexecerror.InternalError(fmt.Sprintf("unhandled type %s", input.sourceTypes[colIdx].String()))
	}
	// {{end}}
	// {{/*
}

// */}}

// buildRightGroupsFromBatch takes a []group and repeats each group numRepeats
// times. For example, given an input table:
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
// Note: this is different from buildLeftGroupsFromBatch in that each group is
// not expanded but directly copied numRepeats times.
// SIDE EFFECTS: writes into o.output.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) buildRightGroupsFromBatch(
	rightGroups []group, colOffset int, input *mergeJoinInput, batch coldata.Batch, destStartIdx int,
) {
	initialBuilderState := o.builderState.right
	sel := batch.Selection()
	outputBatchSize := o.outputBatchSize

	o.unlimitedAllocator.PerformOperation(
		o.output.ColVecs()[colOffset:colOffset+len(input.sourceTypes)],
		func() {
			// Loop over every column.
		RightColLoop:
			for colIdx := range input.sourceTypes {
				outStartIdx := destStartIdx
				out := o.output.ColVec(colIdx + colOffset)
				var src coldata.Vec
				if batch.Length() > 0 {
					src = batch.ColVec(colIdx)
				}

				if sel != nil {
					if src != nil && src.MaybeHasNulls() {
						_RIGHT_SWITCH(_JOIN_TYPE, true, true)
					} else {
						_RIGHT_SWITCH(_JOIN_TYPE, true, false)
					}
				} else {
					if src != nil && src.MaybeHasNulls() {
						_RIGHT_SWITCH(_JOIN_TYPE, false, true)
					} else {
						_RIGHT_SWITCH(_JOIN_TYPE, false, false)
					}
				}

				o.builderState.right.setBuilderColumnState(initialBuilderState)
			}
			o.builderState.right.reset()
		})
}

// buildRightBufferedGroup is similar to buildRightGroupsFromBatch, but it
// builds the output columns corresponding to the right input based on the
// buffered group. The goal is to repeat the whole buffered group
// rightGroup.numRepeats times.
// Note that all other fields of rightGroup are ignored because, by definition,
// all rows in the buffered group are part of rightGroup (i.e. we don't need to
// look at rowStartIdx and rowEndIdx). Also, all rows in the buffered group do
// have a match, so the group can neither be "nullGroup" nor "unmatched".
func (o *mergeJoin_JOIN_TYPE_STRINGOp) buildRightBufferedGroup(
	ctx context.Context,
	rightGroup group,
	colOffset int,
	input *mergeJoinInput,
	bufferedGroup mjBufferedGroup,
	destStartIdx int,
) {
	var err error
	o.unlimitedAllocator.PerformOperation(
		o.output.ColVecs()[colOffset:colOffset+len(input.sourceTypes)],
		func() {
			outStartIdx := destStartIdx
			// Repeat the buffered group numRepeats times.
			for ; o.builderState.right.numRepeatsIdx < rightGroup.numRepeats; o.builderState.right.numRepeatsIdx++ {
				currentBatch := o.builderState.rBufferedGroupBatch
				if currentBatch == nil {
					currentBatch, err = bufferedGroup.dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					o.builderState.rBufferedGroupBatch = currentBatch
					o.builderState.right.curSrcStartIdx = 0
				}
				batchLength := currentBatch.Length()
				for batchLength > 0 {
					toAppend := batchLength - o.builderState.right.curSrcStartIdx
					if outStartIdx+toAppend > o.outputBatchSize {
						toAppend = o.outputBatchSize - outStartIdx
					}

					// Loop over every column.
					for colIdx := range input.sourceTypes {
						out := o.output.ColVec(colIdx + colOffset)
						src := currentBatch.ColVec(colIdx)
						switch input.canonicalTypeFamilies[colIdx] {
						// {{range $.Overloads}}
						case _CANONICAL_TYPE_FAMILY:
							switch input.sourceTypes[colIdx].Width() {
							// {{range .WidthOverloads}}
							case _TYPE_WIDTH:
								srcCol := src.TemplateType()
								outCol := out.TemplateType()

								// Optimization in the case that group length is 1, use assign
								// instead of copy.
								if toAppend == 1 {
									if src.Nulls().NullAt(o.builderState.right.curSrcStartIdx) {
										out.Nulls().SetNull(outStartIdx)
									} else {
										v := execgen.UNSAFEGET(srcCol, o.builderState.right.curSrcStartIdx)
										execgen.SET(outCol, outStartIdx, v)
									}
								} else {
									out.Copy(
										coldata.CopySliceArgs{
											SliceArgs: coldata.SliceArgs{
												Src:         src,
												DestIdx:     outStartIdx,
												SrcStartIdx: o.builderState.right.curSrcStartIdx,
												SrcEndIdx:   o.builderState.right.curSrcStartIdx + toAppend,
											},
										},
									)
								}
								// {{end}}
							}
							// {{end}}
						default:
							colexecerror.InternalError(fmt.Sprintf("unhandled type %s", input.sourceTypes[colIdx].String()))
						}
					}
					outStartIdx += toAppend

					if toAppend < batchLength-o.builderState.right.curSrcStartIdx {
						// If we haven't materialized all the rows from the batch, then we
						// are ready to emit the output batch.
						o.builderState.right.curSrcStartIdx += toAppend
						return
					}
					// We have fully processed the current batch, so we need to get the
					// next one.
					o.unlimitedAllocator.ReleaseBatch(currentBatch)
					currentBatch, err = bufferedGroup.dequeue(ctx)
					if err != nil {
						colexecerror.InternalError(err)
					}
					o.builderState.rBufferedGroupBatch = currentBatch
					batchLength = currentBatch.Length()
					o.builderState.right.curSrcStartIdx = 0
				}
				// We have fully processed all the batches from the buffered group, so
				// we need to rewind it.
				if err := bufferedGroup.rewind(); err != nil {
					colexecerror.InternalError(err)
				}
				o.builderState.rBufferedGroupBatch = nil
			}
			o.builderState.right.reset()
		})
}

// probe is where we generate the groups slices that are used in the build
// phase. We do this by first assuming that every row in both batches
// contributes to the cross product. Then, with every equality column, we
// filter out the rows that don't contribute to the cross product (i.e. they
// don't have a matching row on the other side in the case of an inner join),
// and set the correct cardinality.
// Note that in this phase, we do this for every group, except the last group
// in the batch.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) probe(ctx context.Context) {
	o.groups.reset(o.proberState.lIdx, o.proberState.lLength, o.proberState.rIdx, o.proberState.rLength)
	lSel := o.proberState.lBatch.Selection()
	rSel := o.proberState.rBatch.Selection()
	if lSel != nil {
		if rSel != nil {
			o.probeBodyLSeltrueRSeltrue(ctx)
		} else {
			o.probeBodyLSeltrueRSelfalse(ctx)
		}
	} else {
		if rSel != nil {
			o.probeBodyLSelfalseRSeltrue(ctx)
		} else {
			o.probeBodyLSelfalseRSelfalse(ctx)
		}
	}
}

// setBuilderSourceToBufferedGroup sets up the builder state to use the
// buffered group.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) setBuilderSourceToBufferedGroup(ctx context.Context) {
	// {{if and (_JOIN_TYPE.IsLeftAnti) (not _JOIN_TYPE.IsSetOp)}}
	// All tuples in the buffered group have matches, so they are not output in
	// case of LEFT ANTI join.
	o.builderState.lGroups = o.builderState.lGroups[:0]
	// {{else}}
	lGroupEndIdx := o.proberState.lBufferedGroup.numTuples
	rGroupEndIdx := o.proberState.rBufferedGroup.numTuples
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.rGroups = o.builderState.rGroups[:1]
	// {{if _JOIN_TYPE.IsLeftAnti}}
	// For EXCEPT ALL join we add (lGroupEndIdx - rGroupEndIdx) number
	// (if positive) of unmatched rows.
	if lGroupEndIdx > rGroupEndIdx {
		o.builderState.lGroups[0] = group{
			rowStartIdx: 0,
			rowEndIdx:   lGroupEndIdx - rGroupEndIdx,
			numRepeats:  1,
			toBuild:     lGroupEndIdx - rGroupEndIdx,
			unmatched:   true,
		}
	} else {
		o.builderState.lGroups = o.builderState.lGroups[:0]
	}
	// {{else if _JOIN_TYPE.IsLeftSemi}}
	numMatched := lGroupEndIdx
	// {{if _JOIN_TYPE.IsSetOp}}
	// For INTERSECT ALL join we add a left group to build of length
	// min(lGroupEndIdx, rGroupEndIdx).
	if rGroupEndIdx < lGroupEndIdx {
		numMatched = rGroupEndIdx
	}
	// {{else}}
	// Remove unused warning.
	_ = rGroupEndIdx
	// {{end}}
	o.builderState.lGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   numMatched,
		numRepeats:  1,
		toBuild:     numMatched,
	}
	// {{else}}
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
	// {{end}}
	// {{end}}

	o.builderState.buildFrom = mjBuildFromBufferedGroup

	// We cannot yet reset the buffered groups because the builder will be taking
	// input from them. The actual reset will take place on the next call to
	// initProberState().
	o.proberState.lBufferedGroupNeedToReset = true
	o.proberState.rBufferedGroupNeedToReset = true
}

// exhaustLeftSource sets up the builder to process any remaining tuples from
// the left source. It should only be called when the right source has been
// exhausted.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) exhaustLeftSource(ctx context.Context) {
	// {{if or _JOIN_TYPE.IsInner _JOIN_TYPE.IsLeftSemi}}
	// {{/*
	// Remaining tuples from the left source do not have a match, so they are
	// ignored in INNER, LEFT SEMI, and INTERSECT ALL joins (the latter has
	// IsLeftSemi == true).
	// */}}
	// {{end}}
	// {{if or _JOIN_TYPE.IsLeftOuter _JOIN_TYPE.IsLeftAnti}}
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
	// {{if _JOIN_TYPE.IsLeftOuter}}
	o.builderState.rGroups = o.builderState.rGroups[:1]
	o.builderState.rGroups[0] = group{
		rowStartIdx: o.proberState.lIdx,
		rowEndIdx:   o.proberState.lLength,
		numRepeats:  1,
		toBuild:     o.proberState.lLength - o.proberState.lIdx,
		nullGroup:   true,
	}
	// {{end}}

	o.proberState.lIdx = o.proberState.lLength
	// {{end}}
	// {{if _JOIN_TYPE.IsRightOuter}}
	// {{/*
	// Remaining tuples from the left source do not have a match, so they are
	// ignored in RIGHT OUTER join.
	// */}}
	// {{end}}
}

// exhaustRightSource sets up the builder to process any remaining tuples from
// the right source. It should only be called when the left source has been
// exhausted.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) exhaustRightSource() {
	// {{if _JOIN_TYPE.IsRightOuter}}
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
	// {{else}}
	// Remaining tuples from the right source do not have a match, so they are
	// ignored in all joins except for RIGHT OUTER and FULL OUTER.
	// {{end}}
}

// calculateOutputCount uses the toBuild field of each group and the output
// batch size to determine the output count. Note that as soon as a group is
// materialized partially or fully to output, its toBuild field is updated
// accordingly.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) calculateOutputCount(groups []group) int {
	count := o.builderState.outCount

	for i := 0; i < len(groups); i++ {
		// {{if _JOIN_TYPE.IsLeftAnti}}
		if !groups[i].unmatched {
			// "Matched" groups are not outputted in LEFT ANTI and EXCEPT ALL
			// joins (for the latter IsLeftAnti == true), so they do not
			// contribute to the output count.
			continue
		}
		// {{end}}
		count += groups[i].toBuild
		groups[i].toBuild = 0
		if count > o.outputBatchSize {
			groups[i].toBuild = count - o.outputBatchSize
			count = o.outputBatchSize
			return count
		}
	}
	o.builderState.outFinished = true
	return count
}

// build creates the cross product, and writes it to the output member.
func (o *mergeJoin_JOIN_TYPE_STRINGOp) build(ctx context.Context) {
	outStartIdx := o.builderState.outCount
	o.builderState.outCount = o.calculateOutputCount(o.builderState.lGroups)
	if o.output.Width() != 0 && o.builderState.outCount > outStartIdx {
		// We will be actually building the output if we have columns in the output
		// batch (meaning that we're not doing query like 'SELECT count(*) ...')
		// and when builderState.outCount has increased (meaning that we have
		// something to build).
		switch o.builderState.buildFrom {
		case mjBuildFromBatch:
			o.buildLeftGroupsFromBatch(o.builderState.lGroups, &o.left, o.proberState.lBatch, outStartIdx)
			// {{if not (or _JOIN_TYPE.IsLeftSemi _JOIN_TYPE.IsLeftAnti)}}
			o.buildRightGroupsFromBatch(o.builderState.rGroups, len(o.left.sourceTypes), &o.right, o.proberState.rBatch, outStartIdx)
		// {{end}}
		case mjBuildFromBufferedGroup:
			o.buildLeftBufferedGroup(ctx, o.builderState.lGroups[0], &o.left, o.proberState.lBufferedGroup, outStartIdx)
			// {{if not (or _JOIN_TYPE.IsLeftSemi _JOIN_TYPE.IsLeftAnti)}}
			o.buildRightBufferedGroup(ctx, o.builderState.rGroups[0], len(o.left.sourceTypes), &o.right, o.proberState.rBufferedGroup, outStartIdx)
		// {{end}}

		default:
			colexecerror.InternalError(fmt.Sprintf("unsupported mjBuildFrom %d", o.builderState.buildFrom))
		}
	}
}

// {{/*
// This code snippet is executed when at least one of the input sources has
// been exhausted. It processes any remaining tuples and then sets up the
// builder.
func _SOURCE_FINISHED_SWITCH(_JOIN_TYPE joinTypeInfo) { // */}}
	// {{define "sourceFinishedSwitch"}}
	o.outputReady = true
	o.builderState.buildFrom = mjBuildFromBatch
	// {{if or $.JoinType.IsInner $.JoinType.IsLeftSemi}}
	o.setBuilderSourceToBufferedGroup(ctx)
	// {{else}}
	// Next, we need to make sure that builder state is set up for a case when
	// neither exhaustLeftSource nor exhaustRightSource is called below. In such
	// scenario the merge joiner is done, so it'll be outputting zero-length
	// batches from now on.
	o.builderState.lGroups = o.builderState.lGroups[:0]
	o.builderState.rGroups = o.builderState.rGroups[:0]
	// {{end}}
	// {{if or $.JoinType.IsLeftOuter $.JoinType.IsLeftAnti}}
	// At least one of the sources is finished. If it was the right one,
	// then we need to emit remaining tuples from the left source with
	// nulls corresponding to the right one. But if the left source is
	// finished, then there is nothing left to do.
	if o.proberState.lIdx < o.proberState.lLength {
		o.exhaustLeftSource(ctx)
		// We unset o.outputReady here because we want to put as many unmatched
		// tuples from the left into the output batch. Once outCount reaches the
		// desired output batch size, the output will be returned.
		o.outputReady = false
	}
	// {{end}}
	// {{if $.JoinType.IsRightOuter}}
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
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

func (o *mergeJoin_JOIN_TYPE_STRINGOp) Next(ctx context.Context) coldata.Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.output.ResetInternalBatch()
	for {
		switch o.state {
		case mjEntry:
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
			o.setBuilderSourceToBufferedGroup(ctx)
			o.state = mjBuild
		case mjProbe:
			o.probe(ctx)
			o.setBuilderSourceToBatch()
			o.state = mjBuild
		case mjBuild:
			o.build(ctx)

			if o.builderState.outFinished {
				o.state = mjEntry
				o.builderState.outFinished = false
			}

			if o.outputReady || o.builderState.outCount == o.outputBatchSize {
				if o.builderState.outCount == 0 {
					// We have already fully emitted the result of the join, so we
					// transition to "finished" state.
					o.state = mjDone
					continue
				}
				o.output.SetLength(o.builderState.outCount)
				// Reset builder out count.
				o.builderState.outCount = 0
				o.outputReady = false
				return o.output
			}
		case mjDone:
			// Note that resetting of buffered groups will close disk queues
			// (if there are any).
			if o.proberState.lBufferedGroupNeedToReset {
				o.proberState.lBufferedGroup.reset(ctx)
				o.proberState.lBufferedGroupNeedToReset = false
			}
			if o.proberState.rBufferedGroupNeedToReset {
				o.proberState.rBufferedGroup.reset(ctx)
				o.proberState.rBufferedGroupNeedToReset = false
			}
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(fmt.Sprintf("unexpected merge joiner state in Next: %v", o.state))
		}
	}
}
