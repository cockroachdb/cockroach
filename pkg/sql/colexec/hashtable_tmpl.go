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
// +build execgen_template
//
// This file is the execgen template for hashtable.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
// to the result of the the second input != the third input.
func _ASSIGN_NE(_, _, _, _, _, _ interface{}) int {
	colexecerror.InternalError("")
}

// _L_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _L_TYP.
func _L_UNSAFEGET(_, _ interface{}) interface{} {
	colexecerror.InternalError("")
}

// _R_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _R_TYP.
func _R_UNSAFEGET(_, _ interface{}) interface{} {
	colexecerror.InternalError("")
}

// This is a code snippet that is the main body of checkCol* functions. It
// takes in the following template "meta" variables that enable/disable certain
// code paths:
// _PROBE_HAS_NULLS - a boolean as .ProbeHasNulls that determines whether the
// probe vector might have NULL values.
// _BUILD_HAS_NULLS - a boolean as .BuildHasNulls that determines whether the
// build vector might have NULL values.
// _ALLOW_NULL_EQUALITY - a boolean as .AllowNullEquality that determines
// whether NULL values should be treated as equal.
// _SELECT_DISTINCT - a boolean as .SelectDistinct that determines whether a
// probe tuple should be marked as "distinct" if its groupID is zero (meaning
// that there is no tuple in the hash table with the same hash code).
// _USE_PROBE_SEL - a boolean as .UseProbeSel that determines whether there is
// a selection vector on the probe vector.
// _PROBING_AGAINST_ITSELF - a boolean as .ProbingAgainstItself that tells us
// whether the probe and the build vectors are the same object. Having this
// knob allows us to not generate code for the combination of _USE_PROBE_SEL is
// false when _USE_BUILD_SEL is true (if we were to add _USE_BUILD_SEL) because
// such code would never be used.
// _DELETING_PROBE_MODE - a boolean as .DeletingProbeMode that indicates
// whether the hash table is used with hashTableDeletingProbeMode probing mode.
// When it is true, the hashTable uses 'visited' slice to mark previously
// matched tuples as "deleted" so they won't get matched again.
func _CHECK_COL_BODY(
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
	_ALLOW_NULL_EQUALITY bool,
	_SELECT_DISTINCT bool,
	_USE_PROBE_SEL bool,
	_PROBING_AGAINST_ITSELF bool,
	_DELETING_PROBE_MODE bool,
) { // */}}
	// {{define "checkColBody" -}}
	var (
		probeIdx, buildIdx       int
		probeIsNull, buildIsNull bool
	)
	// Early bounds check.
	_ = ht.probeScratch.toCheck[nToCheck-1]
	for i := uint64(0); i < nToCheck; i++ {
		// keyID of 0 is reserved to represent the end of the next chain.
		toCheck := ht.probeScratch.toCheck[i]
		keyID := ht.probeScratch.groupID[toCheck]
		if keyID != 0 {
			// the build table key (calculated using keys[keyID - 1] = key) is
			// compared to the corresponding probe table to determine if a match is
			// found.
			// {{if .DeletingProbeMode}}
			if ht.visited[keyID] {
				// This build tuple has already been matched, so we treat
				// it as different from the probe tuple.
				ht.probeScratch.differs[toCheck] = true
				continue
			}
			// {{end}}

			// {{if .UseProbeSel}}
			probeIdx = probeSel[toCheck]
			// {{else}}
			probeIdx = int(toCheck)
			// {{end}}
			// {{if .ProbeHasNulls}}
			probeIsNull = probeVec.Nulls().NullAt(probeIdx)
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
			// The vector is probed against itself, so buildVec has the same
			// selection vector as probeVec.
			buildIdx = probeSel[keyID-1]
			// {{else}}
			buildIdx = int(keyID - 1)
			// {{end}}
			// {{if .BuildHasNulls}}
			buildIsNull = buildVec.Nulls().NullAt(buildIdx)
			// {{end}}
			// {{if .AllowNullEquality}}
			if probeIsNull && buildIsNull {
				// Both values are NULLs, and since we're allowing null equality, we
				// proceed to the next value to check.
				continue
			} else if probeIsNull {
				// Only probing value is NULL, so it is different from the build value
				// (which is non-NULL). We mark it as "different" and proceed to the
				// next value to check. This behavior is special in case of allowing
				// null equality because we don't want to reset the groupID of the
				// current probing tuple.
				ht.probeScratch.differs[toCheck] = true
				continue
			}
			// {{end}}
			if probeIsNull {
				ht.probeScratch.groupID[toCheck] = 0
			} else if buildIsNull {
				ht.probeScratch.differs[toCheck] = true
			} else {
				probeVal := _L_UNSAFEGET(probeKeys, probeIdx)
				buildVal := _R_UNSAFEGET(buildKeys, buildIdx)
				var unique bool
				_ASSIGN_NE(unique, probeVal, buildVal, _, probeKeys, buildKeys)
				ht.probeScratch.differs[toCheck] = ht.probeScratch.differs[toCheck] || unique
			}
		}
		// {{if .SelectDistinct}}
		if keyID == 0 {
			ht.probeScratch.distinct[toCheck] = true
		}
		// {{end}}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_WITH_NULLS(
	_USE_PROBE_SEL bool, _PROBING_AGAINST_ITSELF bool, _DELETING_PROBE_MODE bool,
) { // */}}
	// {{define "checkColWithNulls" -}}
	// {{$probingAgainstItself := .ProbingAgainstItself}}
	// {{$deletingProbeMode := .DeletingProbeMode}}
	if probeVec.MaybeHasNulls() {
		if buildVec.MaybeHasNulls() {
			if ht.allowNullEquality {
				// {{/*
				// The allowNullEquality flag only matters if both vectors have nulls.
				// This lets us avoid writing all 2^3 conditional branches.
				// */}}
				_CHECK_COL_BODY(true, true, true, false, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
			} else {
				_CHECK_COL_BODY(true, true, false, false, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
			}
		} else {
			_CHECK_COL_BODY(true, false, false, false, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		}
	} else {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(false, true, false, false, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		} else {
			_CHECK_COL_BODY(false, false, false, false, _USE_PROBE_SEL, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
		}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_FUNCTION_TEMPLATE(_PROBING_AGAINST_ITSELF bool, _DELETING_PROBE_MODE bool) { // */}}
	// {{define "checkColFunctionTemplate" -}}
	// {{$probingAgainstItself := .ProbingAgainstItself}}
	// {{$deletingProbeMode := .DeletingProbeMode}}
	// {{with .Global}}
	// In order to inline the templated code of overloads, we need to have a
	// `_overloadHelper` local variable of type `overloadHelper`.
	_overloadHelper := ht.overloadHelper
	switch probeVec.CanonicalTypeFamily() {
	// {{range .LeftFamilies}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch probeVec.Type().Width() {
		// {{range .LeftWidths}}
		case _LEFT_TYPE_WIDTH:
			switch buildVec.CanonicalTypeFamily() {
			// {{range .RightFamilies}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch buildVec.Type().Width() {
				// {{range .RightWidths}}
				case _RIGHT_TYPE_WIDTH:
					probeKeys := probeVec._ProbeType()
					buildKeys := buildVec._BuildType()
					if probeSel != nil {
						_CHECK_COL_WITH_NULLS(true, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
					} else {
						_CHECK_COL_WITH_NULLS(false, _PROBING_AGAINST_ITSELF, _DELETING_PROBE_MODE)
					}
					// {{end}}
				}
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

// checkCol determines if the current key column in the groupID buckets matches
// the specified equality column key. If there is no match, then the key is
// added to differs. If the bucket has reached the end, the key is rejected. If
// the hashTable disallows null equality, then if any element in the key is
// null, there is no match.
func (ht *hashTable) checkCol(
	probeVec, buildVec coldata.Vec, keyColIdx int, nToCheck uint64, probeSel []int,
) {
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(false, false)
	// {{end}}
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}

// checkColAgainstItself is similar to checkCol, but it probes the vector
// against itself.
func (ht *hashTable) checkColAgainstItself(vec coldata.Vec, nToCheck uint64, sel []int) {
	// {{/*
	// In order to reuse the same template function as checkCol uses, we use
	// the same variable names.
	// */}}
	probeVec, buildVec, probeSel := vec, vec, sel
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(true, false)
	// {{end}}
}

// {{end}}

// {{if .HashTableMode.IsDeletingProbe}}

// checkColDeleting determines if the current key column in the groupID buckets
// matches the specified equality column key. If there is no match *or* the key
// has been already used, then the key is added to differs. If the bucket has
// reached the end, the key is rejected. If the hashTable disallows null
// equality, then if any element in the key is null, there is no match.
func (ht *hashTable) checkColDeleting(
	probeVec, buildVec coldata.Vec, keyColIdx int, nToCheck uint64, probeSel []int,
) {
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(false, true)
	// {{end}}
}

// {{end}}

// {{/*
func _CHECK_COL_FOR_DISTINCT_WITH_NULLS(_USE_PROBE_SEL bool) { // */}}
	// {{define "checkColForDistinctWithNulls" -}}
	if probeVec.MaybeHasNulls() {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(true, true, true, true, _USE_PROBE_SEL, false, false)
		} else {
			_CHECK_COL_BODY(true, false, true, true, _USE_PROBE_SEL, false, false)
		}
	} else {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(false, true, true, true, _USE_PROBE_SEL, false, false)
		} else {
			_CHECK_COL_BODY(false, false, true, true, _USE_PROBE_SEL, false, false)
		}
	}

	// {{end}}
	// {{/*
} // */}}

// {{if .HashTableMode.IsDistinctBuild}}
// {{with .Overloads}}

func (ht *hashTable) checkColForDistinctTuples(
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
				// {{/* We're being this tricky with code generation because we
				//      know that both probeVec and buildVec are of the same
				//      type, so we need to iterate over one level of type
				//      family - width. But, the checkCol function above needs
				//      two layers, so in order to keep these templated
				//      functions in a single file we make sure that we
				//      generate the code only if the "second" level is the
				//      same as the "first" one */}}
				case _RIGHT_TYPE_WIDTH:
					probeKeys := probeVec._ProbeType()
					buildKeys := buildVec._ProbeType()
					if probeSel != nil {
						_CHECK_COL_FOR_DISTINCT_WITH_NULLS(true)
					} else {
						_CHECK_COL_FOR_DISTINCT_WITH_NULLS(false)
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
func _CHECK_BODY(_SELECT_SAME_TUPLES bool, _DELETING_PROBE_MODE bool) { // */}}
	// {{define "checkBody" -}}
	for _, toCheck := range ht.probeScratch.toCheck[:nToCheck] {
		if !ht.probeScratch.differs[toCheck] {
			// If the current key matches with the probe key, we want to update headID
			// with the current key if it has not been set yet.
			keyID := ht.probeScratch.groupID[toCheck]
			// {{if .DeletingProbeMode}}
			// We need to check whether this key hasn't been "deleted" (we
			// reuse 'visited' array for tracking which tuples are deleted).
			// TODO(yuzefovich): rather than reusing 'visited' array to have
			// "deleted" marks we could be actually removing tuples' keyIDs
			// from the hash chains. This will require changing our use of
			// singly linked list 'next' to doubly linked list.
			if !ht.visited[keyID] {
				// It hasn't been deleted, so we match it with 'toCheck'
				// probing tuple and "delete" the key.
				ht.probeScratch.headID[toCheck] = keyID
				ht.visited[keyID] = true
			} else {
				// It has been deleted, so we need to continue probing on the
				// next chain if it's not the end of the chain already.
				if keyID != 0 {
					ht.probeScratch.toCheck[nDiffers] = toCheck
					nDiffers++
				}
			}
			continue
			// {{else}}
			if ht.probeScratch.headID[toCheck] == 0 {
				ht.probeScratch.headID[toCheck] = keyID
			}
			// {{if .SelectSameTuples}}
			firstID := ht.probeScratch.headID[toCheck]
			if !ht.visited[keyID] {
				// We can then add this keyID into the same array at the end of the
				// corresponding linked list and mark this ID as visited. Since there
				// can be multiple keys that match this probe key, we want to mark
				// differs at this position to be true. This way, the prober will
				// continue probing for this key until it reaches the end of the next
				// chain.
				ht.probeScratch.differs[toCheck] = true
				ht.visited[keyID] = true
				if firstID != keyID {
					ht.same[keyID] = ht.same[firstID]
					ht.same[firstID] = keyID
				}
			}
			// {{end}}
			// {{end}}
		}
		if ht.probeScratch.differs[toCheck] {
			// Continue probing in this next chain for the probe key.
			ht.probeScratch.differs[toCheck] = false
			ht.probeScratch.toCheck[nDiffers] = toCheck
			nDiffers++
		}
	}
	// {{end}}
	// {{/*
} // */}}

// {{if .HashTableMode.IsDistinctBuild}}

// checkBuildForDistinct finds all tuples in probeVecs that are not present in
// buffered tuples stored in ht.vals. It stores the probeVecs's distinct tuples'
// keyIDs in headID buffer.
// NOTE: It assumes that probeVecs does not contain any duplicates itself.
// NOTE: It assumes that probeSel has already been populated and it is not nil.
func (ht *hashTable) checkBuildForDistinct(
	probeVecs []coldata.Vec, nToCheck uint64, probeSel []int,
) uint64 {
	if probeSel == nil {
		colexecerror.InternalError("invalid selection vector")
	}
	copy(ht.probeScratch.distinct, zeroBoolColumn)

	ht.checkColsForDistinctTuples(probeVecs, nToCheck, probeSel)
	nDiffers := uint64(0)
	for i := uint64(0); i < nToCheck; i++ {
		if ht.probeScratch.distinct[ht.probeScratch.toCheck[i]] {
			ht.probeScratch.distinct[ht.probeScratch.toCheck[i]] = false
			// Calculated using the convention: keyID = keys.indexOf(key) + 1.
			ht.probeScratch.headID[ht.probeScratch.toCheck[i]] = ht.probeScratch.toCheck[i] + 1
		} else if ht.probeScratch.differs[ht.probeScratch.toCheck[i]] {
			// Continue probing in this next chain for the probe key.
			ht.probeScratch.differs[ht.probeScratch.toCheck[i]] = false
			ht.probeScratch.toCheck[nDiffers] = ht.probeScratch.toCheck[i]
			nDiffers++
		}
	}
	return nDiffers
}

// {{end}}

// {{/*
//     Note that both probing modes (when hash table is built in full mode)
//     are handled by the same check() function, so we will generate it only
//     once.
// */}}
// {{if .HashTableMode.IsDeletingProbe}}

// check performs an equality check between the current key in the groupID bucket
// and the probe key at that index. If there is a match, the hashTable's same
// array is updated to lazily populate the linked list of identical build
// table keys. The visited flag for corresponding build table key is also set. A
// key is removed from toCheck if it has already been visited in a previous
// probe, or the bucket has reached the end (key not found in build table). The
// new length of toCheck is returned by this function.
func (ht *hashTable) check(
	probeVecs []coldata.Vec, buildKeyCols []uint32, nToCheck uint64, probeSel []int,
) uint64 {
	ht.checkCols(probeVecs, ht.vals.ColVecs(), buildKeyCols, nToCheck, probeSel)
	nDiffers := uint64(0)
	switch ht.probeMode {
	case hashTableDefaultProbeMode:
		_CHECK_BODY(true, false)
	case hashTableDeletingProbeMode:
		_CHECK_BODY(true, true)
	default:
		colexecerror.InternalError("unsupported hash table probe mode")
	}
	return nDiffers
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}

// checkProbeForDistinct performs a column by column check for duplicated tuples
// in the probe table.
func (ht *hashTable) checkProbeForDistinct(vecs []coldata.Vec, nToCheck uint64, sel []int) uint64 {
	for i := range ht.keyCols {
		ht.checkColAgainstItself(vecs[i], nToCheck, sel)
	}
	nDiffers := uint64(0)
	_CHECK_BODY(false, false)
	return nDiffers
}

// {{end}}

// {{/*
func _UPDATE_SEL_BODY(_USE_SEL bool) { // */}}
	// {{define "updateSelBody" -}}
	// Reuse the buffer allocated for distinct.
	visited := ht.probeScratch.distinct
	copy(visited, zeroBoolColumn)
	for i := 0; i < b.Length(); i++ {
		if ht.probeScratch.headID[i] != 0 {
			if hasVisited := visited[ht.probeScratch.headID[i]-1]; !hasVisited {
				// {{if .UseSel}}
				sel[distinctCount] = sel[ht.probeScratch.headID[i]-1]
				// {{else}}
				sel[distinctCount] = int(ht.probeScratch.headID[i] - 1)
				// {{end}}
				visited[ht.probeScratch.headID[i]-1] = true
				// Compacting and deduplicating hash buffer.
				ht.probeScratch.hashBuffer[distinctCount] = ht.probeScratch.hashBuffer[i]
				distinctCount++
			}
		}
		ht.probeScratch.headID[i] = 0
		ht.probeScratch.differs[i] = false
	}
	// {{end}}
	// {{/*
} // */}}

// {{if .HashTableMode.IsDistinctBuild}}

// updateSel updates the selection vector in the given batch using the headID
// buffer. For each nonzero keyID in headID, it will be translated to the actual
// key index using the convention keyID = keys.indexOf(key) + 1. If the input
// batch's selection vector is nil, the key index will be directly used to
// populate the selection vector. Otherwise, the selection vector's value at the
// key index will be used. The duplicated keyIDs will be discarded. The
// hashBuffer will also compact and discard hash values of duplicated keys.
func (ht *hashTable) updateSel(b coldata.Batch) {
	distinctCount := 0
	if sel := b.Selection(); sel != nil {
		_UPDATE_SEL_BODY(true)
	} else {
		b.SetSelection(true)
		sel = b.Selection()
		_UPDATE_SEL_BODY(false)
	}
	b.SetLength(distinctCount)
}

// distinctCheck determines if the current key in the groupID bucket matches the
// equality column key. If there is a match, then the key is removed from
// toCheck. If the bucket has reached the end, the key is rejected. The toCheck
// list is reconstructed to only hold the indices of the eqCol keys that have
// not been found. The new length of toCheck is returned by this function.
func (ht *hashTable) distinctCheck(nToCheck uint64, probeSel []int) uint64 {
	probeVecs := ht.probeScratch.keys
	buildVecs := ht.vals.ColVecs()
	buildKeyCols := ht.keyCols
	ht.checkCols(probeVecs, buildVecs, buildKeyCols, nToCheck, probeSel)
	// Select the indices that differ and put them into toCheck.
	nDiffers := uint64(0)
	for i := uint64(0); i < nToCheck; i++ {
		if ht.probeScratch.differs[ht.probeScratch.toCheck[i]] {
			ht.probeScratch.differs[ht.probeScratch.toCheck[i]] = false
			ht.probeScratch.toCheck[nDiffers] = ht.probeScratch.toCheck[i]
			nDiffers++
		}
	}
	return nDiffers
}

// {{end}}
