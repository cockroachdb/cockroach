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

	"github.com/cockroachdb/apd"
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

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the the second input != the third input.
func _ASSIGN_NE(_, _, _ interface{}) uint64 {
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
	nToCheck uint16,
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
	_ALLOW_NULL_EQUALITY bool,
) { // */}}
	// {{define "checkColBody" -}}
	probeIsNull := false
	buildIsNull := false
	// Early bounds check.
	_ = ht.toCheck[nToCheck-1]
	for i := uint16(0); i < nToCheck; i++ {
		// keyID of 0 is reserved to represent the end of the next chain.

		toCheck := ht.toCheck[i]
		if keyID := ht.groupID[toCheck]; keyID != 0 {
			// the build table key (calculated using keys[keyID - 1] = key) is
			// compared to the corresponding probe table to determine if a match is
			// found.

			// {{if .UseSel}}
			selIdx := sel[toCheck]
			// {{else}}
			selIdx := toCheck
			// {{end}}
			/* {{if .ProbeHasNulls }} */
			probeIsNull = probeVec.Nulls().NullAt(selIdx)
			/* {{end}} */

			/* {{if .BuildHasNulls }} */
			buildIsNull = buildVec.Nulls().NullAt64(keyID - 1)
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
				probeVal := execgen.UNSAFEGET(probeKeys, int(selIdx))
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
	nToCheck uint16,
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
	probeType, buildType coltypes.T, keyColIdx int, nToCheck uint16, sel []uint16,
) {
	switch probeType {
	// {{range $lTyp, $rTypToOverload := .}}
	case _PROBE_TYPE:
		switch buildType {
		// {{range $rTyp, $overload := $rTypToOverload}}
		case _BUILD_TYPE:
			probeVec := ht.keys[keyColIdx]
			buildVec := ht.vals.colVecs[ht.keyCols[keyColIdx]]
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
