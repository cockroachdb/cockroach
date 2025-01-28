// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

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
		//     If the tuple is already marked as different, there is no point in
		//     checking it further.
		// */}}
		if ht.ProbeScratch.differs[toCheck] {
			continue
		}
		// {{/*
		//     The build table tuple (identified by ToCheckID value) is being
		//     compared to the corresponding probing tuple (with the ordinal
		//     'toCheck') to determine if it is an equality match.
		//
		//     We assume that keyID is non-zero because zero keyID indicates
		//     that for the probing tuple there are no more build tuples to try,
		//     so there wouldn't be any point in including such a tuple to be
		//     checked.
		// */}}
		keyID := ht.ProbeScratch.ToCheckID[toCheck]
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
				ht.ProbeScratch.differs[toCheck] = true
				ht.ProbeScratch.foundNull[toCheck] = true
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
		// */}}
		probeVal := probeKeys.Get(probeIdx)
		buildVal := buildKeys.Get(buildIdx)
		var unique bool
		_ASSIGN_NE(unique, probeVal, buildVal, _, probeKeys, buildKeys)
		ht.ProbeScratch.differs[toCheck] = unique
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
	probeVec, buildVec *coldata.Vec, keyColIdx int, nToCheck uint32, probeSel []int,
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
func (ht *HashTable) checkColAgainstItselfForDistinct(
	vec *coldata.Vec, nToCheck uint32, sel []int,
) {
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
	probeVec, buildVec *coldata.Vec, keyColIdx int, nToCheck uint32, probeSel []int,
) {
	// {{with .Overloads}}
	_CHECK_COL_FUNCTION_TEMPLATE(false, false, true)
	// {{end}}
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}
// {{with .Overloads}}

func (ht *HashTable) checkColForDistinctTuples(
	probeVec, buildVec *coldata.Vec, nToCheck uint32, probeSel []int,
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
func _CHECK_BODY(
	_SELECT_SAME_TUPLES bool,
	_DELETING_PROBE_MODE bool,
	_SELECT_DISTINCT bool,
	_ALLOW_NULL_EQUALITY bool,
) { // */}}
	// {{define "checkBody" -}}
	toCheckSlice := ht.ProbeScratch.ToCheck
	_ = toCheckSlice[nToCheck-1]
	for toCheckPos := uint32(0); toCheckPos < nToCheck && nDiffers < nToCheck; toCheckPos++ {
		//gcassert:bce
		toCheck := toCheckSlice[toCheckPos]
		// {{if not .AllowNullEquality}}
		if ht.ProbeScratch.foundNull[toCheck] {
			// {{if .SelectDistinct}}
			// {{/*
			//     The hash table is used for the unordered distinct operator.
			//     This code block is only relevant when we're probing the batch
			//     against itself in order to separate all tuples in the batch
			//     into equality buckets (where equality buckets are specified
			//     by the same HeadID values) and when NULLs are considered
			//     different. In this case we see that the probing tuple has a
			//     NULL value, so we want to mark it as equal to itself only.
			// */}}
			ht.ProbeScratch.HeadID[toCheck] = toCheck + 1
			// {{else}}
			// {{/*
			//     The hash table is used by the hash joiner when the NULL
			//     equality is not allowed. In such case, since we found a NULL
			//     value in the equality column, we know for sure that this
			//     probing tuple won't ever get a match.
			// */}}
			ht.ProbeScratch.ToCheckID[toCheck] = 0
			// {{end}}
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
			//
			//     However, if we're selecting distinct tuples, we know for sure
			//     that HeadID has not been set yet for the current tuple - if
			//     it was set, then we wouldn't have included this tuple into
			//     the ToCheck slice after the previous probing iteration.
			// */}}
			// {{if not .SelectDistinct}}
			if ht.ProbeScratch.HeadID[toCheck] == 0 {
				// {{end}}
				ht.ProbeScratch.HeadID[toCheck] = keyID
				// {{if not .SelectDistinct}}
			}
			// {{end}}
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
// Same array is updated to lazily populate the linked list of identical build
// table keys. The Visited flag for corresponding build table key is also set. A
// key is removed from ToCheck if it has already been visited in a previous
// probe, or the bucket has reached the end (key not found in build table). The
// new length of ToCheck is returned by this function.
//
// Let's walk through an example of how this function works for the INNER join
// when the right source is not distinct (i.e. with the
// HashTableDefaultProbeMode probe mode and Same being non-nil).
//
// In this example the right source contains the following tuples:
//
//	[-1, -2, -4, -1, -3, -2, -1, -3]
//
// and for simplicity we will assume that our hash function is
//
//	h(i) = i % 2
//
// while the hash table only uses two buckets.
//
// During the build phase, before we get to Check function, the hash table is
// constructed in the following manner:
//
//	ht.BuildScratch.First = [2, 1]
//	ht.BuildScratch.Next  = [x, 4, 3, 6, 5, 7, 0, 8, 0]    ('x' means reserved)
//
// Describing in words what this tells us is: say, if we have a value i with
// h(i) = 1, then in order to iterate over all possible matches we do the
// following traversal:
//   - start at keyID = First[h(i)] = First[1] = 1 (i.e. zeroth tuple is the
//     first "candidate")
//   - then for each "candidate" keyID, do nextKeyID = Next[keyID] until it
//     becomes zero, so we will try keyIDs Next[1] = 4, Next[4] = 5,
//     Next[5] = 7, Next[7] = 8, and then stop.
//
// With this background in mind, let's imagine that the first probing batch from
// the left is [-3, -1, -5].
//
// On the first iteration of probing (i.e. the first Check call), we populate
// the following:
// -    ToCheck = [0, 1, 2]
// -  ToCheckID = [1, 1, 1].
// which means that for each probing tuple we have a hash match, and it's the
// zeroth build tuple. Check function finds that only the first probing tuple -1
// matches with the zeroth build tuple, so we end up with the following:
// -    differs = [true, false, true]
// -  HeadID[1] = 1.
// However, since we're selecting the "same" tuples (i.e. populating the Same
// slice), we actually want to continue probing the first tuple from the batch
// for more matches, so we still include it into the ToCheck Slice and do:
//
//	Visited[1] = true.
//
// Then we proceed to the second iteration of probing with all tuples still
// included:
// -    ToCheck = [0, 1, 2]
// -  ToCheckID = [4, 4, 4].
// Check function again finds that only the first probing tuple matches with the
// "candidate", so we end up with the following:
// -    differs = [true, false, true].
// Again, since we want to find all "same" tuples, we find that build tuples
// with firstID=1 and keyID=4 are the same, so we do
// -    Same[1] = 4
// - Visited[4] = true.
//
// On the third iteration, we're still checking all the probing tuples:
// -    ToCheck = [0, 1, 2]
// -  ToCheckID = [5, 5, 5].
// This time the Check function finds that the zeroth probing tuple has a match:
// -    differs = [false, true, true]
// -  HeadID[0] = 5
// - Visited[5] = true.
//
// On the fourth iteration, we're still checking all the probing tuples:
// -    ToCheck = [0, 1, 2]
// -  ToCheckID = [7, 7, 7].
// The Check function finds that the first probing tuple has a match:
// -    differs = [true, false, true].
// We have firstID = 1 and keyID = 7 and do:
// -    Same[7] = Same[1] = 4
// -    Same[1] = 7
// - Visited[7] = true.
//
// On the fifth iteration, we're still checking all the probing tuples:
// -    ToCheck = [0, 1, 2]
// -  ToCheckID = [8, 8, 8].
// The Check function finds that the zeroth probing tuple has a match:
// -    differs = [false, true, true].
// We have firstID = 5 and keyID = 8 and do:
// -    Same[5] = 8
// - Visited[8] = true.
//
// At this point, after five Check calls, there are no more "candidates" for any
// probing tuples (since we reached the end of the hash chain for each probing
// tuple), and the hash joiner will proceed to collecting the matches. Let's
// summarize the current state of things:
// - BuildScratch.First = [2, 1]
// - BuildScratch.Next  = [x, 4, 3, 6, 5, 7, 0, 8, 0]    ('x' means reserved)
// -              Same  = [x, 7, 0, 0, 0, 8, 0, 4, 0]
// -            Visited = [t, t, f, f, t, t, f, t, t]    ('f' means false, 't' means true)
// -             HeadID = [5, 1, 0].
// What this means is that the zeroth probing tuple -3 has an equality match
// with (HeadID[0] = 5) the fourth build tuple -3 as well as (Same[5] = 8) the
// seventh build tuple -3; the first probing tuple -1 has an equality match with
// build tuples with keyIDs HeadID[1] = 1, Same[1] = 7, Same[7] = 4 (i.e. the
// zeroth, the sixth, and the third build tuples with value -1); and the second
// probing tuple -5 doesn't have any matches.
//
// Now let's quickly go through the second probing batch from the left source
// that has values [-1, -2, -3].
//
// On the first Check call we have:
// -    ToCheck = [0, 1, 2]
// -  ToCheckID = [1, 2, 1].
// We find that zeroth and first probing tuple have matches, but since the build
// tuple with keyID=1 has already been visited, we don't include it to be
// checked again, yet keyID=2 hasn't been visited, so we still continue probing
// the first tuple:
// -     HeadID = [1, 2, 0]
// -    ToCheck = [1, 2]
// -  ToCheckID = [x, 3, 4]
// - Visited[2] = true.
//
// On the second Check call we find that both probing tuples didn't get matches
// with the candidates (i.e. they had hash collisions), so we continue probing
// both of them:
// -     HeadID = [1, 2, 0]
// -    ToCheck = [1, 2]
// -  ToCheckID = [x, 6, 5]
// - Visited[3] = true.
//
// On the third Check call both tuples got matches.
// -     HeadID = [1, 2, 5]
// -    Same[2] = 6
// - Visited[6] = true.
// Both tuples are removed from the ToCheck slice because for the -2 probing
// tuple we reached the end of the hash chain and for the -3 probing tuple we
// found a match that has been previously visited.
//
// At this point, after three Check calls, there are no more "candidates" for
// any probing tuples, and the hash joiner will proceed to collecting the
// matches. The current state of things:
// - BuildScratch.First = [2, 1]
// - BuildScratch.Next  = [x, 4, 3, 6, 5, 7, 0, 8, 0]    ('x' means reserved)
// -              Same  = [x, 7, 6, 0, 0, 8, 0, 4, 0]
// -            Visited = [t, t, t, t, t, t, t, t, t]    ('t' means true)
// -             HeadID = [1, 2, 5].
// This means that the zeroth probing tuple -1 has equality matches with keyIDs
// 1, 7, 4; the first probing tuple -2 with keyIDs 2, 6; the second probing
// tuple -3 with keyIDs 5, 8.
//
// We also have fully visited all tuples in the hash table, so all future
// probing batches will be handled more efficiently (namely, once we find a
// match, we stop probing for the corresponding tuple).
func (ht *HashTable) Check(nToCheck uint32, probeSel []int) uint32 {
	ht.checkCols(ht.Keys, nToCheck, probeSel)
	nDiffers := uint32(0)
	switch ht.probeMode {
	case HashTableDefaultProbeMode:
		// {{/*
		//     HashTableDefaultProbeMode is used by join types other than
		//     set-operation joins, so it never allows the null equality.
		// */}}
		if ht.Same != nil {
			_CHECK_BODY(true, false, false, false)
		} else {
			_CHECK_BODY(false, false, false, false)
		}
	case HashTableDeletingProbeMode:
		// {{/*
		//     HashTableDeletingProbeMode is only used by the set-operation
		//     joins, which always allow the null equality.
		// */}}
		_CHECK_BODY(false, true, false, true)
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported hash table probe mode"))
	}
	return nDiffers
}

// {{end}}

// {{if .HashTableMode.IsDistinctBuild}}

// CheckProbeForDistinct performs a column by column check for duplicated tuples
// in the probe table.
func (ht *HashTable) CheckProbeForDistinct(vecs []*coldata.Vec, nToCheck uint32, sel []int) uint32 {
	for i := range ht.keyCols {
		ht.checkColAgainstItselfForDistinct(vecs[i], nToCheck, sel)
	}
	nDiffers := uint32(0)
	if ht.allowNullEquality {
		_CHECK_BODY(false, false, true, true)
	} else {
		_CHECK_BODY(false, false, true, false)
	}
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
	// Reuse the buffer allocated for foundNull.
	visited := ht.ProbeScratch.foundNull
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

// FindBuckets finds the buckets for all tuples in batch when probing against a
// hash table that is specified by 'first' and 'next' vectors as well as
// 'duplicatesChecker'. `duplicatesChecker` takes a slice of key columns of the
// batch, number of tuples to check, and the selection vector of the batch, and
// it returns number of tuples that needs to be checked for next iteration.
// The "buckets" are specified by equal values in ht.ProbeScratch.HeadID.
//
// - zeroHeadIDForDistinctTuple controls how a bucket with a single distinct
// tuple will be represented (i.e. this tuple is distinct from all tuples in the
// hash table). If zeroHeadIDForDistinctTuple is true, then this tuple will have
// HeadID of 0, otherwise, it'll have HeadID[i] = keyID(i) = i + 1.
// - probingAgainstItself, if true, indicates that the hash table was built on
// the probing batch itself. In such a scenario the behavior can be optimized
// since we know that every tuple has at least one match.
//
// NOTE: *first* and *next* vectors should be properly populated.
// NOTE: batch is assumed to be non-zero length.
func (ht *HashTable) FindBuckets(
	batch coldata.Batch,
	keyCols []*coldata.Vec,
	first, next []keyID,
	duplicatesChecker func([]*coldata.Vec, uint32, []int) uint32,
	zeroHeadIDForDistinctTuple bool,
	probingAgainstItself bool,
) {
	ht.ProbeScratch.SetupLimitedSlices(batch.Length())
	if zeroHeadIDForDistinctTuple {
		if probingAgainstItself {
			findBuckets(ht, batch, keyCols, first, next, duplicatesChecker, true, true)
		} else {
			findBuckets(ht, batch, keyCols, first, next, duplicatesChecker, true, false)
		}
	} else {
		if probingAgainstItself {
			findBuckets(ht, batch, keyCols, first, next, duplicatesChecker, false, true)
		} else {
			findBuckets(ht, batch, keyCols, first, next, duplicatesChecker, false, false)
		}
	}
}

// execgen:inline
// execgen:template<zeroHeadIDForDistinctTuple,probingAgainstItself>
func findBuckets(
	ht *HashTable,
	batch coldata.Batch,
	keyCols []*coldata.Vec,
	first []keyID,
	next []keyID,
	duplicatesChecker func([]*coldata.Vec, uint32, []int) uint32,
	zeroHeadIDForDistinctTuple bool,
	probingAgainstItself bool,
) {
	batchLength := batch.Length()
	sel := batch.Selection()
	// Early bounds checks.
	toCheckIDs := ht.ProbeScratch.ToCheckID
	_ = toCheckIDs[batchLength-1]
	if !zeroHeadIDForDistinctTuple || probingAgainstItself {
		headIDs := ht.ProbeScratch.HeadID
		_ = headIDs[batchLength-1]
	}
	var nToCheck uint32
	for i, hash := range ht.ProbeScratch.HashBuffer[:batchLength] {
		toCheck := uint32(i)
		nextToCheckID := first[hash]
		handleNextToCheckID(ht, toCheck, nextToCheckID, toCheckIDs, zeroHeadIDForDistinctTuple, probingAgainstItself, true)
	}

	for nToCheck > 0 {
		// {{/*
		//     Continue searching for the build table matching keys while the
		//     ToCheck array is non-empty.
		// */}}
		nToCheck = duplicatesChecker(keyCols, nToCheck, sel)
		toCheckSlice := ht.ProbeScratch.ToCheck[:nToCheck]
		nToCheck = 0
		for _, toCheck := range toCheckSlice {
			nextToCheckID := next[toCheckIDs[toCheck]]
			handleNextToCheckID(ht, toCheck, nextToCheckID, toCheckIDs, zeroHeadIDForDistinctTuple, probingAgainstItself, false)
		}
	}
}

// execgen:inline
// execgen:template<zeroHeadIDForDistinctTuple,probingAgainstItself,headIDsBCE>
func handleNextToCheckID(
	ht *HashTable,
	toCheck uint32,
	nextToCheckID keyID,
	toCheckIDs []keyID,
	zeroHeadIDForDistinctTuple bool,
	probingAgainstItself bool,
	headIDsBCE bool,
) {
	if probingAgainstItself {
		// {{/*
		//      When probing against itself, we know for sure that each tuple
		//      has at least one hash match (with itself), so nextToCheckID will
		//      never be zero, so we skip the non-zero conditional.
		// */}}
		if toCheck+1 == nextToCheckID {
			// {{/*
			//     When our "match candidate" tuple is the tuple itself, we know
			//     for sure they will be equal, so we can just mark this tuple
			//     accordingly, without including it into the ToCheck slice.
			// */}}
			if headIDsBCE {
				//gcassert:gce
			}
			headIDs[toCheck] = nextToCheckID
		} else {
			includeTupleToCheck(ht, toCheck, nextToCheckID, toCheckIDs, headIDsBCE)
		}
	} else {
		if nextToCheckID != 0 {
			includeTupleToCheck(ht, toCheck, nextToCheckID, toCheckIDs, headIDsBCE)
		} else {
			// {{/*
			//     This tuple doesn't have a duplicate.
			// */}}
			if zeroHeadIDForDistinctTuple {
				// {{/*
				//     We leave the HeadID of this tuple unchanged (i.e. zero -
				//     that was set in SetupLimitedSlices).
				// */}}
			} else {
				// {{/*
				//     Set the HeadID of this tuple to point to itself since it
				//     is an equality chain consisting only of a single element.
				// */}}
				if headIDsBCE {
					//gcassert:gce
				}
				headIDs[toCheck] = toCheck + 1
			}
		}
	}
}

// {{/*
//
//	useI indicates whether 'i' variable should be used directly as the index
//	into toCheckIDs for the assignment. This should be the case for the
//	first call to handleNextToCheckID and is needed to achieve bounds checks
//	elimination (in that case toCheck := uint32(i), and apparently the cast
//	trips up the compiler).
//
// */}}
// execgen:inline
// execgen:template<useI>
func includeTupleToCheck(
	ht *HashTable, toCheck uint32, nextToCheckID uint32, toCheckIDs []uint32, useI bool,
) {
	// {{/*
	//     We should always get BCE on toCheckIDs slice because:
	//     - for the first call site of handleNextToCheckID, we access
	//     toCheckIDs[i] for largest value of i that we have in the loop before
	//     the loop
	//     - for the second call site of handleNextToCheckID, bounds checks are
	//     done when evaluating nextToCheckID.
	// */}}
	if useI {
		//gcassert:bce
		toCheckIDs[i] = nextToCheckID
	} else {
		//gcassert:bce
		toCheckIDs[toCheck] = nextToCheckID
	}
	ht.ProbeScratch.ToCheck[nToCheck] = toCheck
	nToCheck++
}

// {{end}}
