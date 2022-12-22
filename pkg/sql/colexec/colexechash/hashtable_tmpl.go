// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for hashtable.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexechash

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ = coldataext.CompareDatum
	_ tree.AggType
)

// {{/*

// _LEFT_CANONICAL_TYPE_FAMILY is the template variable.
const _LEFT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _LEFT_TYPE_WIDTH is the template variable.
const _LEFT_TYPE_WIDTH = 0

// _RIGHT_CANONICAL_TYPE_FAMILY is the template variable.
const _RIGHT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _RIGHT_TYPE_WIDTH is the template variable.
const _RIGHT_TYPE_WIDTH = 0

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_, _, _, _, _, _ interface{}) int {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// This is a code snippet that is the main body of checkCol* functions. It
// takes in the following template "meta" variables that enable/disable certain
// code paths:
// _GLOBAL - a string replaced by "$global" (the local template variable
// referring to the NotEqual overload) before performing the template function
// call. It is needed because _CHECK_COL_BODY template function is called from
// two places where the overload is in different template context (in one case
// it is `.`, and in another it is `.Global`). We work around it by replacing
// _GLOBAL with the local variable during the initial preprocessing of the
// template.
// _PROBE_HAS_NULLS - a boolean as .ProbeHasNulls that determines whether the
// probe vector might have NULL values.
// _BUILD_HAS_NULLS - a boolean as .BuildHasNulls that determines whether the
// build vector might have NULL values.
// _SELECT_DISTINCT - a boolean as .SelectDistinct that determines whether a
// probe tuple should be marked as "distinct" if there is no tuple in the hash
// table that might be a duplicate of the probe tuple (either because the
// ToCheckID of the probe tuple is 0 - meaning no hash matches - or because the
// probe tuple has a NULL value when NULLs are treated as not equal).
// _USE_PROBE_SEL - a boolean as .UseProbeSel that determines whether there is
// a selection vector on the probe vector.
// _PROBING_AGAINST_ITSELF - a boolean as .ProbingAgainstItself that tells us
// whether the probe and the build vectors are the same object. Having this
// knob allows us to not generate code for the combination of _USE_PROBE_SEL is
// false when _USE_BUILD_SEL is true (if we were to add _USE_BUILD_SEL) because
// such code would never be used.
// _DELETING_PROBE_MODE - a boolean as .DeletingProbeMode that indicates
// whether the hash table is used with HashTableDeletingProbeMode probing mode.
// When it is true, the HashTable uses 'visited' slice to mark previously
// matched tuples as "deleted" so they won't get matched again.
func _CHECK_COL_BODY(
	_GLOBAL interface{},
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
	_SELECT_DISTINCT bool,
	_USE_PROBE_SEL bool,
	_PROBING_AGAINST_ITSELF bool,
	_DELETING_PROBE_MODE bool,
) { // */}}
	// {{define "checkColBody" -}}
	var probeIdx, buildIdx int
	// {{if .ProbeHasNulls}}
	probeVecNulls := probeVec.Nulls()
	// {{end}}
	// {{if .BuildHasNulls}}
	buildVecNulls := buildVec.Nulls()
	// {{end}}
	for _, toCheck := range ht.ProbeScratch.ToCheck[:nToCheck] {
		// {{/*
		//     The build table tuple (identified by ToCheckID value) is being
		//     compared to the corresponding probing tuple (with the ordinal
		//     'toCheck') to determine if it is an equality match. keyID of 0
		//     indicates that for the probing tuple there are no more build
		//     tuples to try.
		// */}}
		keyID := ht.ProbeScratch.ToCheckID[toCheck]
		// {{if or (not .SelectDistinct) (not .ProbingAgainstItself)}}
		// {{/*
		//      When we're selecting distinct tuples and probing against itself,
		//      we're in the code path of the unordered distinct where we're
		//      trying to find duplicates within a single input batch. In such a
		//      case we will never hit keyID of 0 because each tuple in the
		//      batch is equal to itself (and possibly others). Once we find a
		//      match, the tuple is no longer checked, so we never reach the end
		//      of the corresponding hash chain which could result in keyID
		//      being 0.
		// */}}
		if keyID != 0 {
			// {{end}}
			// {{if .DeletingProbeMode}}
			if ht.Visited[keyID] {
				// {{/*
				//     This build tuple has already been matched, so we treat
				//     it as different from the probing tuple.
				// */}}
				ht.ProbeScratch.differs[toCheck] = true
				continue
			}
			// {{end}}
			// {{/*
			//     Figure out the indexes of the tuples we're looking at.
			// */}}
			// {{if .UseProbeSel}}
			probeIdx = probeSel[toCheck]
			// {{else}}
			probeIdx = int(toCheck)
			// {{end}}
			// {{/*
			//     Usually, the build vector is already stored in the hash table,
			//     so there is no selection vector. However, there is a use case
			//     when we want to apply the selection vector to keyID when the
			//     hash table is used by unordered distinct to remove the
			//     duplicates within the vector itself - the vector is being
			//     probed "against itself". In such case .UseProbeSel also
			//     means .UseBuildSel if we were to introduce it.
			// */}}
			// {{if and (.UseProbeSel) (.ProbingAgainstItself)}}
			// {{/*
			//     The vector is probed against itself, so buildVec has the same
			//     selection vector as probeVec.
			// */}}
			buildIdx = probeSel[keyID-1]
			// {{else}}
			buildIdx = int(keyID - 1)
			// {{end}}
			// {{/*
			//     If either of the tuples might have NULLs, check that.
			// */}}
			// {{if .ProbeHasNulls}}
			probeIsNull := probeVecNulls.NullAt(probeIdx)
			// {{end}}
			// {{if .BuildHasNulls}}
			buildIsNull := buildVecNulls.NullAt(buildIdx)
			// {{end}}
			// {{/*
			//     If the probing tuple might have NULLs, handle that case
			//     first.
			// */}}
			// {{if .ProbeHasNulls}}
			if probeIsNull {
				if ht.allowNullEquality {
					// {{/*
					//     The probing tuple has a NULL value and NULLs are
					//     treated as equal, so our behavior will depend on
					//     whether the build tuple also has a NULL value:
					//     - buildIsNull is false, then we have a mismatch and
					//     we want to mark 'differs' accordingly;
					//     - buildIsNull is true, then we have a match for the
					//     current value and want to proceed checking on the
					//     following column.
					//     The template is set up such that in both cases we
					//     fall down to 'continue', and when !buildIsNull, we
					//     also generate code for updating 'differs'.
					// */}}
					// {{if .BuildHasNulls}}
					if !buildIsNull {
						// {{end}}
						ht.ProbeScratch.differs[toCheck] = true
						// {{if .BuildHasNulls}}
					}
					// {{end}}
				} else {
					// {{if .SelectDistinct}}
					// {{/*
					//     We know that nulls are distinct (because
					//     allowNullEquality is false) and our probing tuple has
					//     a NULL value in the current column, so the probing
					//     tuple is distinct from the build table.
					// */}}
					ht.ProbeScratch.distinct[toCheck] = true
					// {{else}}
					ht.ProbeScratch.ToCheckID[toCheck] = 0
					// {{end}}
				}
				continue
			}
			// {{end}}
			// {{/*
			//     At this point only the build tuple might have a NULL value,
			//     and if it is NULL, regardless of allowNullEquality, we have a
			//     mismatch.
			// */}}
			// {{if .BuildHasNulls}}
			if buildIsNull {
				ht.ProbeScratch.differs[toCheck] = true
				continue
			}
			// {{end}}
			// {{/*
			//     Now both values are not NULL, so we have to perform actual
			//     comparison.
			//     TODO(yuzefovich): depending on the type, it might be faster
			//     to check whether differs[toCheck] is already true. My guess
			//     is that for simple types like int64 introducing a conditional
			//     will be slower.
			// */}}
			probeVal := probeKeys.Get(probeIdx)
			buildVal := buildKeys.Get(buildIdx)
			var unique bool
			_ASSIGN_NE(unique, probeVal, buildVal, _, probeKeys, buildKeys)
			ht.ProbeScratch.differs[toCheck] = ht.ProbeScratch.differs[toCheck] || unique
			// {{if and .SelectDistinct (not .ProbingAgainstItself)}}
		} else {
			ht.ProbeScratch.distinct[toCheck] = true
			// {{end}}
			// {{if or (not .SelectDistinct) (not .ProbingAgainstItself)}}
		}
		// {{end}}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_WITH_NULLS(
	_SELECT_DISTINCT bool,
	_USE_PROBE_SEL bool,
	_PROBING_AGAINST_ITSELF bool,
	_DELETING_PROBE_MODE bool,
) { // */}}
	// {{define "checkColWithNulls" -}}
	// {{$global := .Global}}
	// {{$selectDistinct := .SelectDistinct}}
	// {{$probingAgainstItself := .ProbingAgainstItself}}
	// {{$deletingProbeMode := .DeletingProbeMode}}
	if probeVec.MaybeHasNulls() {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(_GLOBAL, true, true, _SELECT_DISTINCT, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		} else {
			_CHECK_COL_BODY(_GLOBAL, true, false, _SELECT_DISTINCT, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		}
	} else {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(_GLOBAL, false, true, _SELECT_DISTINCT, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		} else {
			_CHECK_COL_BODY(_GLOBAL, false, false, _SELECT_DISTINCT, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_FUNCTION_TEMPLATE(
	_SELECT_DISTINCT bool, _PROBING_AGAINST_ITSELF bool, _DELETING_PROBE_MODE bool,
) { // */}}
	// {{define "checkColFunctionTemplate" -}}
	// {{$selectDistinct := .SelectDistinct}}
	// {{$probingAgainstItself := .ProbingAgainstItself}}
	// {{$deletingProbeMode := .DeletingProbeMode}}
	// {{with .Global}}
	switch probeVec.CanonicalTypeFamily() {
	// {{range .LeftFamilies}}
	// {{$leftFamily := .LeftCanonicalFamilyStr}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch probeVec.Type().Width() {
		// {{range .LeftWidths}}
		// {{$leftWidth := .Width}}
		case _LEFT_TYPE_WIDTH:
			switch buildVec.CanonicalTypeFamily() {
			// {{range .RightFamilies}}
			// {{$rightFamily := .RightCanonicalFamilyStr}}
			// {{/*
			//     We currently only support the cases of same-type as well as
			//     integers of mixed widths in the equality conditions (all
			//     other allowed mixed-type comparisons are pushed into the ON
			//     condition, see #43060), so we will generate the code only
			//     for same-type comparisons and for integer ones.
			//  */}}
			// {{if or (eq $leftFamily $rightFamily) (and (eq $leftFamily "types.IntFamily") (eq $rightFamily "types.IntFamily"))}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch buildVec.Type().Width() {
				// {{range .RightWidths}}
				// {{$rightWidth := .Width}}
				// {{if or (not $probingAgainstItself) (eq $leftWidth $rightWidth)}}
				// {{/*
				//     In a special case of probing against itself, we know that
				//     the vectors have the same width (because probeVec and
				//     buildVec are the same object), so we don't generate code
				//     if the width are different.
				// */}}
				case _RIGHT_TYPE_WIDTH:
					probeKeys := probeVec._ProbeType()
					buildKeys := buildVec._BuildType()
					if probeSel != nil {
						_CHECK_COL_WITH_NULLS(_SELECT_DISTINCT, true, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
					} else {
						_CHECK_COL_WITH_NULLS(_SELECT_DISTINCT, false, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
					}
					// {{end}}
					// {{end}}
				}
				// {{end}}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	// {{end}}
	// {{end}}
	// {{/*
} // */}}

// {{if and (not .HashTableMode.IsDistinctBuild) (not .HashTableMode.IsDeletingProbe)}}

// checkCol determines if the current key column in the ToCheckID buckets matches
// the specified equality column key. If there is no match, then the key is
// added to differs. If the bucket has reached the end, the key is rejected. If
// the HashTable disallows null equality, then if any element in the key is
// null, there is no match.
func (ht *HashTable) checkCol(
	probeVec, buildVec coldata.Vec, keyColIdx int, nToCheck uint64, probeSel []int,
) {
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(false, false, false)
	// {{end}}
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}

// checkColAgainstItselfForDistinct is similar to checkCol, but it probes the
// vector against itself for the purposes of finding matches to unordered
// distinct columns.
func (ht *HashTable) checkColAgainstItselfForDistinct(vec coldata.Vec, nToCheck uint64, sel []int) {
	// {{/*
	// In order to reuse the same template function as checkCol uses, we use
	// the same variable names.
	// */}}
	probeVec, buildVec, probeSel := vec, vec, sel
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(true, true, false)
	// {{end}}
}

// {{end}}

// {{if .HashTableMode.IsDeletingProbe}}

// checkColDeleting determines if the current key column in the ToCheckID
// buckets matches the specified equality column key. If there is no match *or*
// the key has been already used, then the key is added to differs. If the
// bucket has reached the end, the key is rejected. If the HashTable disallows
// null equality, then if any element in the key is null, there is no match.
func (ht *HashTable) checkColDeleting(
	probeVec, buildVec coldata.Vec, keyColIdx int, nToCheck uint64, probeSel []int,
) {
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(false, false, true)
	// {{end}}
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}
// {{with .Overloads}}

func (ht *HashTable) checkColForDistinctTuples(
	probeVec, buildVec coldata.Vec, nToCheck uint64, probeSel []int,
) {
	switch probeVec.CanonicalTypeFamily() {
	// {{range .LeftFamilies}}
	// {{$leftFamily := .LeftCanonicalFamilyStr}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch probeVec.Type().Width() {
		// {{range .LeftWidths}}
		// {{$leftWidth := .Width}}
		case _LEFT_TYPE_WIDTH:
			switch probeVec.CanonicalTypeFamily() {
			// {{range .RightFamilies}}
			// {{$rightFamily := .RightCanonicalFamilyStr}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch probeVec.Type().Width() {
				// {{range .RightWidths}}
				// {{$rightWidth := .Width}}
				// {{if and (eq $leftFamily $rightFamily) (eq $leftWidth $rightWidth)}}
				// {{/*
				//      We're being this tricky with code generation because we
				//      know that both probeVec and buildVec are of the same
				//      type, so we need to iterate over one level of type
				//      family - width. But, the checkCol function above needs
				//      two layers, so in order to keep these templated
				//      functions in a single file we make sure that we
				//      generate the code only if the "second" level is the
				//      same as the "first" one
				// */}}
				case _RIGHT_TYPE_WIDTH:
					probeKeys := probeVec._ProbeType()
					buildKeys := buildVec._ProbeType()
					// {{$global := .}}
					if probeVec.MaybeHasNulls() {
						if buildVec.MaybeHasNulls() {
							_CHECK_COL_BODY(_GLOBAL, true, true, true, true, false, false)
						} else {
							_CHECK_COL_BODY(_GLOBAL, true, false, true, true, false, false)
						}
					} else {
						if buildVec.MaybeHasNulls() {
							_CHECK_COL_BODY(_GLOBAL, false, true, true, true, false, false)
						} else {
							_CHECK_COL_BODY(_GLOBAL, false, false, true, true, false, false)
						}
					}
					// {{end}}
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
}

// {{end}}
// {{end}}

// {{/*
func _CHECK_BODY(_SELECT_SAME_TUPLES bool, _DELETING_PROBE_MODE bool, _SELECT_DISTINCT bool) { // */}}
	// {{define "checkBody" -}}
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint64(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		// {{if .SelectDistinct}}
		if ht.ProbeScratch.distinct[toCheck] {
			// {{/*
			//     The hash table is used for the unordered distinct operator.
			//     This code block is only relevant when we're probing the batch
			//     against itself in order to separate all tuples in the batch
			//     into equality buckets (where equality buckets are specified
			//     by the same HeadID values). In this case we see that the
			//     probing tuple is distinct (i.e. it is unique in the batch),
			//     so we want to mark it as equal to itself only.
			// */}}
			ht.ProbeScratch.HeadID[toCheck] = toCheck + 1
			continue
		}
		// {{end}}
		if !ht.ProbeScratch.differs[toCheck] {
			keyID := ht.ProbeScratch.ToCheckID[toCheck]
			// {{if .DeletingProbeMode}}
			// {{/*
			//     We need to check whether this matching tuple hasn't been
			//     "deleted" (we reuse 'visited' array for tracking which tuples
			//     are deleted).
			//     TODO(yuzefovich): rather than reusing 'visited' array to have
			//     "deleted" marks we could be actually removing tuples' keyIDs
			//     from the hash chains. This will require changing our use of
			//     singly linked list 'Next' to doubly linked list.
			// */}}
			if !ht.Visited[keyID] {
				// {{/*
				//     It hasn't been deleted, so we match it with 'toCheck'
				//     probing tuple and "delete" the key.
				// */}}
				ht.ProbeScratch.HeadID[toCheck] = keyID
				ht.Visited[keyID] = true
			} else {
				// {{/*
				//     It has been deleted, so we need to continue probing on
				//     the Next chain if it's not the end of the chain already.
				// */}}
				if keyID != 0 {
					//gcassert:bce
					toCheckSlice[nDiffers] = toCheck
					nDiffers++
				}
			}
			// {{else}}
			// {{/*
			//     If we have an equality match, we want to update HeadID with
			//     the current keyID if it has not been set yet.
			// */}}
			if ht.ProbeScratch.HeadID[toCheck] == 0 {
				ht.ProbeScratch.HeadID[toCheck] = keyID
			}
			// {{if .SelectSameTuples}}
			if !ht.Visited[keyID] {
				// {{/*
				//     This is the first time we found a match for this tuple
				//     from the hash table.
				//
				//     First, we check if there is already another tuple in the
				//     equality chain for this tuple from the hash table (the
				//     value of HeadID is different from the keyID - if it is
				//     the same, then we've just inserted it right above).
				// */}}
				firstID := ht.ProbeScratch.HeadID[toCheck]
				if firstID != keyID {
					// {{/*
					//     There is already at least on other tuple, so we
					//     insert this tuple right after the head of the
					//     equality chain.
					// */}}
					ht.Same[keyID] = ht.Same[firstID]
					ht.Same[firstID] = keyID
				}
				// {{/*
				//     Now mark this tuple as visited so that it doesn't get
				//     inserted into the equality chain again.
				// */}}
				ht.Visited[keyID] = true
				// {{/*
				//     Since there can be more tuples in the hash table that
				//     match this probing tuple, we include the probing tuple in
				//     the ToCheck for the next probing iteration.
				// */}}
				//gcassert:bce
				toCheckSlice[nDiffers] = toCheck
				nDiffers++
			}
			// {{end}}
			// {{end}}
		} else {
			// {{/*
			//     Continue probing in this Next chain for the probe key.
			// */}}
			ht.ProbeScratch.differs[toCheck] = false
			//gcassert:bce
			toCheckSlice[nDiffers] = toCheck
			nDiffers++
		}
	}
	// {{end}}
	// {{/*
} // */}}

// {{/*
//     Note that both probing modes (when hash table is built in full mode)
//     are handled by the same Check() function, so we will generate it only
//     once.
// */}}
// {{if .HashTableMode.IsDeletingProbe}}

// Check performs an equality check between the current key in the ToCheckID
// bucket and the probe key at that index. If there is a match, the HashTable's
// same array is updated to lazily populate the linked list of identical build
// table keys. The visited flag for corresponding build table key is also set. A
// key is removed from ToCheck if it has already been visited in a previous
// probe, or the bucket has reached the end (key not found in build table). The
// new length of ToCheck is returned by this function.
func (ht *HashTable) Check(probeVecs []coldata.Vec, nToCheck uint64, probeSel []int) uint64 {
	ht.checkCols(probeVecs, nToCheck, probeSel)
	nDiffers := uint64(0)
	switch ht.probeMode {
	case HashTableDefaultProbeMode:
		if ht.Same != nil {
			_CHECK_BODY(true, false, false)
		} else {
			_CHECK_BODY(false, false, false)
		}
	case HashTableDeletingProbeMode:
		_CHECK_BODY(false, true, false)
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported hash table probe mode"))
	}
	return nDiffers
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}

// CheckProbeForDistinct performs a column by column check for duplicated tuples
// in the probe table.
func (ht *HashTable) CheckProbeForDistinct(vecs []coldata.Vec, nToCheck uint64, sel []int) uint64 {
	for i := range ht.keyCols {
		ht.checkColAgainstItselfForDistinct(vecs[i], nToCheck, sel)
	}
	nDiffers := uint64(0)
	_CHECK_BODY(false, false, true)
	return nDiffers
}

// {{end}}

// {{/*
func _UPDATE_SEL_BODY(_USE_SEL bool) { // */}}
	// {{define "updateSelBody" -}}
	batchLength := b.Length()
	// Capture the slices in order for BCE to occur.
	HeadIDs := ht.ProbeScratch.HeadID
	hashBuffer := ht.ProbeScratch.HashBuffer
	_ = HeadIDs[batchLength-1]
	_ = hashBuffer[batchLength-1]
	// Reuse the buffer allocated for distinct.
	visited := ht.ProbeScratch.distinct
	copy(visited, colexecutils.ZeroBoolColumn)
	distinctCount := 0
	for i := 0; i < batchLength && distinctCount < batchLength; i++ {
		//gcassert:bce
		HeadID := HeadIDs[i]
		if HeadID != 0 {
			if hasVisited := visited[HeadID-1]; !hasVisited {
				// {{if .UseSel}}
				sel[distinctCount] = sel[HeadID-1]
				// {{else}}
				sel[distinctCount] = int(HeadID - 1)
				// {{end}}
				visited[HeadID-1] = true
				// {{/*
				//     Compacting and de-duplicating hash buffer.
				// */}}
				//gcassert:bce
				hashBuffer[distinctCount] = hashBuffer[i]
				distinctCount++
			}
		}
	}
	b.SetLength(distinctCount)
	// {{end}}
	// {{/*
} // */}}

// {{if .HashTableMode.IsDistinctBuild}}

// updateSel updates the selection vector in the given batch using the HeadID
// buffer. For each nonzero keyID in HeadID, it will be translated to the actual
// key index using the convention keyID = keys.indexOf(key) + 1. If the input
// batch's selection vector is nil, the key index will be directly used to
// populate the selection vector. Otherwise, the selection vector's value at the
// key index will be used. The duplicated keyIDs will be discarded. The
// HashBuffer will also compact and discard hash values of duplicated keys.
func (ht *HashTable) updateSel(b coldata.Batch) {
	if b.Length() == 0 {
		return
	}
	if sel := b.Selection(); sel != nil {
		_UPDATE_SEL_BODY(true)
	} else {
		b.SetSelection(true)
		sel = b.Selection()
		_UPDATE_SEL_BODY(false)
	}
}

// {{end}}
