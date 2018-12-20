// Copyright 2018 The Cockroach Authors.
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
// This file is the execgen template for hashjoiner.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package
var _ = math.Pi

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// _ASSIGN_HASH is the template equality function for assigning the first input
// to the result of the hash value of the second input.
func _ASSIGN_HASH(_, _ interface{}) uint64 {
	panic("")
}

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the the second input != the third input.
func _ASSIGN_NE(_, _, _ interface{}) uint64 {
	panic("")
}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _SEL_IND is the template type variable for the loop variable that's either
// i or sel[i] depending on whether we're in a selection or not.
const _SEL_IND = 0

func _CHECK_COL_MAIN(
	prober *hashJoinProber, buildKeys, probeKeys []interface{}, keyID uint64, i uint16,
) { // */}}
	// {{define "checkColMain"}}
	buildVal := buildKeys[keyID-1]
	probeVal := probeKeys[_SEL_IND]
	var unique bool
	_ASSIGN_NE(unique, buildVal, probeVal)

	if unique {
		prober.differs[prober.toCheck[i]] = true
	}
	// {{end}}

	// {{/*
}

func _CHECK_COL_BODY(
	prober *hashJoinProber,
	probeVec, buildVec ColVec,
	buildKeys, probeKeys []interface{},
	nToCheck uint16,
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
) { // */}}
	// {{define "checkColBody"}}
	for i := uint16(0); i < nToCheck; i++ {
		// keyID of 0 is reserved to represent the end of the next chain.

		if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
			// the build table key (calculated using keys[keyID - 1] = key) is
			// compared to the corresponding probe table to determine if a match is
			// found.

			/* {{if .ProbeHasNulls }} */
			if probeVec.NullAt(_SEL_IND) {
				prober.groupID[prober.toCheck[i]] = 0
			} else /*{{end}} {{if .BuildHasNulls}} */ if buildVec.NullAt64(keyID - 1) {
				prober.differs[prober.toCheck[i]] = true
			} else /*{{end}} */ {
				_CHECK_COL_MAIN(prober, buildKeys, probeKeys, keyID, i)
			}
		}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_WITH_NULLS(
	prober *hashJoinProber,
	probeVec, buildVec ColVec,
	buildKeys, probeKeys []interface{},
	nToCheck uint16,
	_SEL_STRING string,
) { // */}}
	// {{define "checkColWithNulls"}}
	if probeVec.HasNulls() {
		if buildVec.HasNulls() {
			_CHECK_COL_BODY(prober, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, true)
		} else {
			_CHECK_COL_BODY(prober, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, false)
		}
	} else {
		if buildVec.HasNulls() {
			_CHECK_COL_BODY(prober, probeVec, buildVec, buildKeys, probeKeys, nToCheck, false, true)
		} else {
			_CHECK_COL_BODY(prober, probeVec, buildVec, buildKeys, probeKeys, nToCheck, false, false)
		}
	}
	// {{end}}
	// {{/*
}

func _REHASH_BODY(buckets []uint64, keys []interface{}, nKeys uint64, _ string) { // */}}
	// {{define "rehashBody"}}
	for i := uint64(0); i < nKeys; i++ {
		v := keys[_SEL_IND]
		var hash uint64
		_ASSIGN_HASH(hash, v)
		buckets[i] = buckets[i]*31 + hash
	}
	// {{end}}

	// {{/*
}

// */}}

// rehash takes a element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func (ht *hashTable) rehash(
	buckets []uint64, keyIdx int, t types.T, col ColVec, nKeys uint64, sel []uint16,
) {
	switch t {
	// {{range $hashType := .HashTemplate}}
	case _TYPES_T:
		keys := col._TemplateType()
		if sel != nil {
			_REHASH_BODY(buckets, keys, nKeys, "sel[i]")
		} else {
			_REHASH_BODY(buckets, keys, nKeys, "i")
		}

	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}

// checkCol determines if the current key column in the groupID buckets matches
// the specified equality column key. If there is a match, then the key is added
// to differs. If the bucket has reached the end, the key is rejected. If any
// element in the key is null, then there is no match.
func (prober *hashJoinProber) checkCol(t types.T, keyColIdx int, nToCheck uint16, sel []uint16) {
	switch t {
	// {{range $neType := .NETemplate}}
	case _TYPES_T:
		buildVec := prober.ht.vals[prober.ht.keyCols[keyColIdx]]
		probeVec := prober.keys[keyColIdx]

		buildKeys := buildVec._TemplateType()
		probeKeys := probeVec._TemplateType()

		if sel != nil {
			_CHECK_COL_WITH_NULLS(
				prober,
				probeVec, buildVec,
				buildKeys, probeKeys,
				nToCheck,
				"sel[prober.toCheck[i]]")
		} else {
			_CHECK_COL_WITH_NULLS(
				prober,
				probeVec, buildVec,
				buildKeys, probeKeys,
				nToCheck,
				"prober.toCheck[i]")
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}
