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
	"context"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "unsafe" package
var _ unsafe.Pointer

// Dummy import to pull in "reflect" package
var _ reflect.SliceHeader

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

func _CHECK_COL_MAIN(ht *hashTable, buildKeys, probeKeys []interface{}, keyID uint64, i uint16) { // */}}
	// {{define "checkColMain"}}
	buildVal := buildKeys[keyID-1]
	probeVal := probeKeys[_SEL_IND]
	var unique bool
	_ASSIGN_NE(unique, buildVal, probeVal)

	if unique {
		ht.differs[ht.toCheck[i]] = true
	}
	// {{end}}

	// {{/*
}

func _CHECK_COL_BODY(
	ht *hashTable,
	probeVec, buildVec coldata.Vec,
	buildKeys, probeKeys []interface{},
	nToCheck uint16,
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
) { // */}}
	// {{define "checkColBody"}}
	for i := uint16(0); i < nToCheck; i++ {
		// keyID of 0 is reserved to represent the end of the next chain.

		if keyID := ht.groupID[ht.toCheck[i]]; keyID != 0 {
			// the build table key (calculated using keys[keyID - 1] = key) is
			// compared to the corresponding probe table to determine if a match is
			// found.

			/* {{if .ProbeHasNulls }} */
			if probeVec.Nulls().NullAt(_SEL_IND) {
				ht.groupID[ht.toCheck[i]] = 0
			} else /*{{end}} {{if .BuildHasNulls}} */ if buildVec.Nulls().NullAt64(keyID - 1) {
				ht.differs[ht.toCheck[i]] = true
			} else /*{{end}} */ {
				_CHECK_COL_MAIN(ht, buildKeys, probeKeys, keyID, i)
			}
		}
	}
	// {{end}}
	// {{/*
}

func _CHECK_COL_WITH_NULLS(
	ht *hashTable,
	probeVec, buildVec coldata.Vec,
	buildKeys, probeKeys []interface{},
	nToCheck uint16,
	_SEL_STRING string,
) { // */}}
	// {{define "checkColWithNulls"}}
	if probeVec.HasNulls() {
		if buildVec.HasNulls() {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, true)
		} else {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, false)
		}
	} else {
		if buildVec.HasNulls() {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, false, true)
		} else {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, false, false)
		}
	}
	// {{end}}
	// {{/*
}

func _REHASH_BODY(
	ctx context.Context,
	ht *hashTable,
	buckets []uint64,
	keys []interface{},
	nKeys uint64,
	_SEL_STRING string,
) { // */}}
	// {{define "rehashBody"}}
	for i := uint64(0); i < nKeys; i++ {
		ht.cancelChecker.check(ctx)
		v := keys[_SEL_IND]
		p := uintptr(buckets[i])
		_ASSIGN_HASH(p, v)
		buckets[i] = uint64(p)
	}
	// {{end}}

	// {{/*
}

func _COLLECT_RIGHT_OUTER(
	prober *hashJoinProber,
	batchSize uint16,
	nResults uint16,
	batch coldata.Batch,
	_SEL_STRING string,
) uint16 { // */}}
	// {{define "collectRightOuter"}}
	for i := uint16(0); i < batchSize; i++ {
		currentID := prober.ht.headID[i]

		if currentID == 0 {
			prober.probeRowUnmatched[nResults] = true
		}

		for {
			if nResults >= coldata.BatchSize {
				prober.prevBatch = batch
				return nResults
			}

			prober.buildIdx[nResults] = currentID - 1
			prober.probeIdx[nResults] = _SEL_IND
			currentID = prober.ht.same[currentID]
			prober.ht.headID[i] = currentID
			nResults++

			if currentID == 0 {
				break
			}
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

func _COLLECT_NO_OUTER(
	prober *hashJoinProber,
	batchSize uint16,
	nResults uint16,
	batch coldata.Batch,
	_SEL_STRING string,
) uint16 { // */}}
	// {{define "collectNoOuter"}}
	for i := uint16(0); i < batchSize; i++ {
		currentID := prober.ht.headID[i]
		for currentID != 0 {
			if nResults >= coldata.BatchSize {
				prober.prevBatch = batch
				return nResults
			}

			prober.buildIdx[nResults] = currentID - 1
			prober.probeIdx[nResults] = _SEL_IND
			currentID = prober.ht.same[currentID]
			prober.ht.headID[i] = currentID
			nResults++
		}
	}
	// {{end}}
	// {{/*
	// Dummy return value that is never used.
	return 0
}

func _DISTINCT_COLLECT_RIGHT_OUTER(prober *hashJoinProber, batchSize uint16, _SEL_STRING string) { // */}}
	// {{define "distinctCollectRightOuter"}}
	for i := uint16(0); i < batchSize; i++ {
		// Index of keys and outputs in the hash table is calculated as ID - 1.
		prober.buildIdx[i] = prober.ht.groupID[i] - 1
		prober.probeIdx[i] = _SEL_IND

		prober.probeRowUnmatched[i] = prober.ht.groupID[i] == 0
	}
	// {{end}}
	// {{/*
}

func _DISTINCT_COLLECT_NO_OUTER(
	prober *hashJoinProber, batchSize uint16, nResults uint16, _ string,
) { // */}}
	// {{define "distinctCollectNoOuter"}}
	for i := uint16(0); i < batchSize; i++ {
		if prober.ht.groupID[i] != 0 {
			// Index of keys and outputs in the hash table is calculated as ID - 1.
			prober.buildIdx[nResults] = prober.ht.groupID[i] - 1
			prober.probeIdx[nResults] = _SEL_IND
			nResults++
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func (ht *hashTable) rehash(
	ctx context.Context,
	buckets []uint64,
	keyIdx int,
	t types.T,
	col coldata.Vec,
	nKeys uint64,
	sel []uint16,
) {
	switch t {
	// {{range $hashType := .HashTemplate}}
	case _TYPES_T:
		keys := col._TemplateType()
		if sel != nil {
			_REHASH_BODY(ctx, ht, buckets, keys, nKeys, "sel[i]")
		} else {
			_REHASH_BODY(ctx, ht, buckets, keys, nKeys, "i")
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
func (ht *hashTable) checkCol(t types.T, keyColIdx int, nToCheck uint16, sel []uint16) {
	switch t {
	// {{range $neType := .NETemplate}}
	case _TYPES_T:
		buildVec := ht.vals[ht.keyCols[keyColIdx]]
		probeVec := ht.keys[keyColIdx]

		buildKeys := buildVec._TemplateType()
		probeKeys := probeVec._TemplateType()

		if sel != nil {
			_CHECK_COL_WITH_NULLS(
				ht,
				probeVec, buildVec,
				buildKeys, probeKeys,
				nToCheck,
				"sel[ht.toCheck[i]]")
		} else {
			_CHECK_COL_WITH_NULLS(
				ht,
				probeVec, buildVec,
				buildKeys, probeKeys,
				nToCheck,
				"ht.toCheck[i]")
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}

// collect prepares the buildIdx and probeIdx arrays where the buildIdx and
// probeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (prober *hashJoinProber) collect(batch coldata.Batch, batchSize uint16, sel []uint16) uint16 {
	nResults := uint16(0)

	if prober.spec.outer {
		if sel != nil {
			_COLLECT_RIGHT_OUTER(prober, batchSize, nResults, batch, "sel[i]")
		} else {
			_COLLECT_RIGHT_OUTER(prober, batchSize, nResults, batch, "i")
		}
	} else {
		if sel != nil {
			_COLLECT_NO_OUTER(prober, batchSize, nResults, batch, "sel[i]")
		} else {
			_COLLECT_NO_OUTER(prober, batchSize, nResults, batch, "i")
		}
	}

	return nResults
}

// distinctCollect prepares the batch with the joined output columns where the build
// row index for each probe row is given in the groupID slice. This function
// requires assumes a N-1 hash join.
func (prober *hashJoinProber) distinctCollect(
	batch coldata.Batch, batchSize uint16, sel []uint16,
) uint16 {
	nResults := uint16(0)

	if prober.spec.outer {
		nResults = batchSize

		if sel != nil {
			_DISTINCT_COLLECT_RIGHT_OUTER(prober, batchSize, "sel[i]")
		} else {
			_DISTINCT_COLLECT_RIGHT_OUTER(prober, batchSize, "i")
		}
	} else {
		if sel != nil {
			_DISTINCT_COLLECT_NO_OUTER(prober, batchSize, nResults, "sel[i]")
		} else {
			_DISTINCT_COLLECT_NO_OUTER(prober, batchSize, nResults, "i")
		}
	}

	return nResults
}
