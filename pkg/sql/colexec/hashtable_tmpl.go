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
	"bytes"
	"fmt"
	"math"

	// {{/*
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the the second input != the third input.
func _ASSIGN_NE(_, _, _ interface{}) int {
	execerror.VectorizedInternalPanic("")
}

// _PROBE_TYPE is the template type variable for coltypes.T. It will be
// replaced by coltypes.Foo for each type Foo in the coltypes.T type.
const _PROBE_TYPE = coltypes.Unhandled

// _BUILD_TYPE is the template type variable for coltypes.T. It will be
// replaced by coltypes.Foo for each type Foo in the coltypes.T type.
const _BUILD_TYPE = coltypes.Unhandled

func _CHECK_COL_BODY(
	ht *hashTable,
	probeVec, buildVec coldata.Vec,
	probeKeys, buildKeys []interface{},
	nToCheck uint64,
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
	_ALLOW_NULL_EQUALITY bool,
) { // */}}
	// {{define "checkColBody" -}}
	probeIsNull := false
	buildIsNull := false
	// Early bounds check.
	_ = ht.toCheck[nToCheck-1]
	for i := uint64(0); i < nToCheck; i++ {
		// keyID of 0 is reserved to represent the end of the next chain.

		toCheck := ht.toCheck[i]
		if keyID := ht.groupID[toCheck]; keyID != 0 {
			// the build table key (calculated using keys[keyID - 1] = key) is
			// compared to the corresponding probe table to determine if a match is
			// found.

			// {{if .UseSel}}
			selIdx := sel[toCheck]
			// {{else}}
			selIdx := int(toCheck)
			// {{end}}
			/* {{if .ProbeHasNulls }} */
			probeIsNull = probeVec.Nulls().NullAt(selIdx)
			/* {{end}} */

			/* {{if .BuildHasNulls }} */
			buildIsNull = buildVec.Nulls().NullAt(int(keyID - 1))
			/* {{end}} */

			/* {{if .AllowNullEquality}} */
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
				ht.differs[toCheck] = true
				continue
			}
			/* {{end}} */
			if probeIsNull {
				ht.groupID[toCheck] = 0
			} else if buildIsNull {
				ht.differs[toCheck] = true
			} else {
				probeVal := execgen.UNSAFEGET(probeKeys, selIdx)
				buildVal := execgen.UNSAFEGET(buildKeys, int(keyID-1))
				var unique bool
				_ASSIGN_NE(unique, probeVal, buildVal)

				ht.differs[toCheck] = ht.differs[toCheck] || unique
			}
		}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_WITH_NULLS(
	ht *hashTable,
	probeVec, buildVec coldata.Vec,
	probeKeys, buildKeys []interface{},
	nToCheck uint64,
	_USE_SEL bool,
) { // */}}
	// {{define "checkColWithNulls" -}}
	if probeVec.MaybeHasNulls() {
		if buildVec.MaybeHasNulls() {
			if ht.allowNullEquality {
				// The allowNullEquality flag only matters if both vectors have nulls.
				// This lets us avoid writing all 2^3 conditional branches.
				_CHECK_COL_BODY(ht, probeVec, buildVec, probeKeys, buildKeys, nToCheck, true, true, true)
			} else {
				_CHECK_COL_BODY(ht, probeVec, buildVec, probeKeys, buildKeys, nToCheck, true, true, false)
			}
		} else {
			_CHECK_COL_BODY(ht, probeVec, buildVec, probeKeys, buildKeys, nToCheck, true, false, false)
		}
	} else {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(ht, probeVec, buildVec, probeKeys, buildKeys, nToCheck, false, true, false)
		} else {
			_CHECK_COL_BODY(ht, probeVec, buildVec, probeKeys, buildKeys, nToCheck, false, false, false)
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// checkCol determines if the current key column in the groupID buckets matches
// the specified equality column key. If there is a match, then the key is added
// to differs. If the bucket has reached the end, the key is rejected. If the
// hashTable disallows null equality, then if any element in the key is null,
// there is no match.
func (ht *hashTable) checkCol(
	probeType, buildType coltypes.T, keyColIdx int, nToCheck uint64, sel []int,
) {
	// In order to inline the templated code of overloads, we need to have a
	// `decimalScratch` local variable of type `decimalOverloadScratch`.
	decimalScratch := ht.decimalScratch
	switch probeType {
	// {{range $lTyp, $rTypToOverload := .}}
	case _PROBE_TYPE:
		switch buildType {
		// {{range $rTyp, $overload := $rTypToOverload}}
		case _BUILD_TYPE:
			probeVec := ht.keys[keyColIdx]
			buildVec := ht.vals.ColVec(int(ht.keyCols[keyColIdx]))
			probeKeys := probeVec._ProbeType()
			buildKeys := buildVec._BuildType()

			if sel != nil {
				_CHECK_COL_WITH_NULLS(
					ht,
					probeVec, buildVec,
					probeKeys, buildKeys,
					nToCheck,
					true)
			} else {
				_CHECK_COL_WITH_NULLS(
					ht,
					probeVec, buildVec,
					probeKeys, buildKeys,
					nToCheck,
					false)
			}
			// {{end}}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", buildType))
		}
	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", probeType))
	}
}

// {{/*
func _CHECK_BODY(_IS_HASHTABLE_IN_FULL_MODE bool) { // */}}
	// {{define "checkBody" -}}
	for i := uint64(0); i < nToCheck; i++ {
		if !ht.differs[ht.toCheck[i]] {
			// If the current key matches with the probe key, we want to update headID
			// with the current key if it has not been set yet.
			keyID := ht.groupID[ht.toCheck[i]]
			if ht.headID[ht.toCheck[i]] == 0 {
				ht.headID[ht.toCheck[i]] = keyID
			}

			// {{if .IsHashTableInFullMode}}
			firstID := ht.headID[ht.toCheck[i]]

			if !ht.visited[keyID] {
				// We can then add this keyID into the same array at the end of the
				// corresponding linked list and mark this ID as visited. Since there
				// can be multiple keys that match this probe key, we want to mark
				// differs at this position to be true. This way, the prober will
				// continue probing for this key until it reaches the end of the next
				// chain.
				ht.differs[ht.toCheck[i]] = true
				ht.visited[keyID] = true

				if firstID != keyID {
					ht.same[keyID] = ht.same[firstID]
					ht.same[firstID] = keyID
				}
			}
			// {{end}}
		}

		if ht.differs[ht.toCheck[i]] {
			// Continue probing in this next chain for the probe key.
			ht.differs[ht.toCheck[i]] = false
			ht.toCheck[nDiffers] = ht.toCheck[i]
			nDiffers++
		}
	}

	// {{end}}
	// {{/*
} // */}}

// check performs an equality check between the current key in the groupID bucket
// and the probe key at that index. If there is a match, the hashTable's same
// array is updated to lazily populate the linked list of identical build
// table keys. The visited flag for corresponding build table key is also set. A
// key is removed from toCheck if it has already been visited in a previous
// probe, or the bucket has reached the end (key not found in build table). The
// new length of toCheck is returned by this function.
func (ht *hashTable) check(probeKeyTypes []coltypes.T, nToCheck uint64, sel []int) uint64 {
	ht.checkCols(probeKeyTypes, nToCheck, sel)
	nDiffers := uint64(0)

	switch ht.mode {
	case hashTableFullMode:
		_CHECK_BODY(true)
	case hashTableDistinctMode:
		_CHECK_BODY(false)
	default:
		execerror.VectorizedInternalPanic("hashTable in unhandled state")
	}

	return nDiffers
}
