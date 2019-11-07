// Copyright 2018 The Cockroach Authors.
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
// This file is the execgen template for hashjoiner.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
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

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _GOTYPESLICE is a template Go type slice variable.
type _GOTYPESLICE interface{}

// _ASSIGN_HASH is the template equality function for assigning the first input
// to the result of the hash value of the second input.
func _ASSIGN_HASH(_, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the the second input != the third input.
func _ASSIGN_NE(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _SEL_IND is the template type variable for the loop variable that's either
// i or sel[i] depending on whether we're in a selection or not.
const _SEL_IND = 0

func _CHECK_COL_BODY(
	ht *hashTable,
	probeVec, buildVec coldata.Vec,
	buildKeys, probeKeys []interface{},
	nToCheck uint16,
	_PROBE_HAS_NULLS bool,
	_BUILD_HAS_NULLS bool,
	_ALLOW_NULL_EQUALITY bool,
) { // */}}
	// {{define "checkColBody"}}
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
				continue
			}
			/* {{end}} */
			if probeIsNull {
				ht.groupID[toCheck] = 0
			} else if buildIsNull {
				ht.differs[toCheck] = true
			} else {
				buildVal := execgen.UNSAFEGET(buildKeys, int(keyID-1))
				probeVal := execgen.UNSAFEGET(probeKeys, int(selIdx))
				var unique bool
				_ASSIGN_NE(unique, buildVal, probeVal)

				if unique {
					ht.differs[toCheck] = true
				}
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
	_USE_SEL bool,
) { // */}}
	// {{define "checkColWithNulls"}}
	if probeVec.MaybeHasNulls() {
		if buildVec.MaybeHasNulls() {
			if ht.allowNullEquality {
				// The allowNullEquality flag only matters if both vectors have nulls.
				// This lets us avoid writing all 2^3 conditional branches.
				_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, true, true)
			} else {
				_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, true, false)
			}
		} else {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, true, false, false)
		}
	} else {
		if buildVec.MaybeHasNulls() {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, false, true, false)
		} else {
			_CHECK_COL_BODY(ht, probeVec, buildVec, buildKeys, probeKeys, nToCheck, false, false, false)
		}
	}
	// {{end}}
	// {{/*
}

func _REHASH_BODY(
	ctx context.Context,
	ht *hashTable,
	buckets []uint64,
	keys _GOTYPESLICE,
	nulls *coldata.Nulls,
	nKeys uint64,
	sel []uint16,
	_HAS_SEL bool,
	_HAS_NULLS bool,
) { // */}}
	// {{define "rehashBody"}}
	// Early bounds checks.
	_ = buckets[nKeys-1]
	// {{ if .HasSel }}
	_ = sel[nKeys-1]
	// {{ else }}
	_ = execgen.UNSAFEGET(keys, int(nKeys-1))
	// {{ end }}
	for i := uint64(0); i < nKeys; i++ {
		ht.cancelChecker.check(ctx)
		// {{ if .HasSel }}
		selIdx := sel[i]
		// {{ else }}
		selIdx := i
		// {{ end }}
		// {{ if .HasNulls }}
		if nulls.NullAt(uint16(selIdx)) {
			continue
		}
		// {{ end }}
		v := execgen.UNSAFEGET(keys, int(selIdx))
		p := uintptr(buckets[i])
		_ASSIGN_HASH(p, v)
		buckets[i] = uint64(p)
	}
	// {{end}}

	// {{/*
}

func _COLLECT_RIGHT_OUTER(
	prober *hashJoinProber, batchSize uint16, nResults uint16, batch coldata.Batch, _USE_SEL bool,
) uint16 { // */}}
	// {{define "collectRightOuter"}}
	// Early bounds checks.
	_ = prober.ht.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		currentID := prober.ht.headID[i]

		for {
			if nResults >= prober.outputBatchSize {
				prober.prevBatch = batch
				return nResults
			}

			prober.probeRowUnmatched[nResults] = currentID == 0
			if currentID > 0 {
				prober.buildIdx[nResults] = currentID - 1
			} else {
				// If currentID == 0, then probeRowUnmatched will have been set - and
				// we set the corresponding buildIdx to zero so that (as long as the
				// build hash table has at least one row) we can copy the values vector
				// without paying attention to probeRowUnmatched.
				prober.buildIdx[nResults] = 0
			}
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
			// {{end}}
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
	prober *hashJoinProber, batchSize uint16, nResults uint16, batch coldata.Batch, _USE_SEL bool,
) uint16 { // */}}
	// {{define "collectNoOuter"}}
	// Early bounds checks.
	_ = prober.ht.headID[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		currentID := prober.ht.headID[i]
		for currentID != 0 {
			if nResults >= prober.outputBatchSize {
				prober.prevBatch = batch
				return nResults
			}

			prober.buildIdx[nResults] = currentID - 1
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
			// {{end}}
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

func _DISTINCT_COLLECT_RIGHT_OUTER(prober *hashJoinProber, batchSize uint16, _USE_SEL bool) { // */}}
	// {{define "distinctCollectRightOuter"}}
	// Early bounds checks.
	_ = prober.ht.groupID[batchSize-1]
	_ = prober.probeRowUnmatched[batchSize-1]
	_ = prober.buildIdx[batchSize-1]
	_ = prober.probeIdx[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		// Index of keys and outputs in the hash table is calculated as ID - 1.
		id := prober.ht.groupID[i]
		rowUnmatched := id == 0
		prober.probeRowUnmatched[i] = rowUnmatched
		if !rowUnmatched {
			prober.buildIdx[i] = id - 1
		}
		// {{if .UseSel}}
		prober.probeIdx[i] = sel[i]
		// {{else}}
		prober.probeIdx[i] = i
		// {{end}}
	}
	// {{end}}
	// {{/*
}

func _DISTINCT_COLLECT_NO_OUTER(
	prober *hashJoinProber, batchSize uint16, nResults uint16, _USE_SEL bool,
) { // */}}
	// {{define "distinctCollectNoOuter"}}
	// Early bounds checks.
	_ = prober.ht.groupID[batchSize-1]
	_ = prober.buildIdx[batchSize-1]
	_ = prober.probeIdx[batchSize-1]
	// {{if .UseSel}}
	_ = sel[batchSize-1]
	// {{end}}
	for i := uint16(0); i < batchSize; i++ {
		if prober.ht.groupID[i] != 0 {
			// Index of keys and outputs in the hash table is calculated as ID - 1.
			prober.buildIdx[nResults] = prober.ht.groupID[i] - 1
			// {{if .UseSel}}
			prober.probeIdx[nResults] = sel[i]
			// {{else}}
			prober.probeIdx[nResults] = i
			// {{end}}
			nResults++
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func (ht *hashTable) rehash(
	ctx context.Context,
	buckets []uint64,
	keyIdx int,
	t coltypes.T,
	col coldata.Vec,
	nKeys uint64,
	sel []uint16,
) {
	switch t {
	// {{range $hashType := .HashTemplate}}
	case _TYPES_T:
		keys, nulls := col._TemplateType(), col.Nulls()
		if col.MaybeHasNulls() {
			if sel != nil {
				_REHASH_BODY(ctx, ht, buckets, keys, nulls, nKeys, sel, true, true)
			} else {
				_REHASH_BODY(ctx, ht, buckets, keys, nulls, nKeys, sel, false, true)
			}
		} else {
			if sel != nil {
				_REHASH_BODY(ctx, ht, buckets, keys, nulls, nKeys, sel, true, false)
			} else {
				_REHASH_BODY(ctx, ht, buckets, keys, nulls, nKeys, sel, false, false)
			}
		}

	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", t))
	}
}

// checkCol determines if the current key column in the groupID buckets matches
// the specified equality column key. If there is a match, then the key is added
// to differs. If the bucket has reached the end, the key is rejected. If the
// hashTable disallows null equality, then if any element in the key is null,
// there is no match.
func (ht *hashTable) checkCol(t coltypes.T, keyColIdx int, nToCheck uint16, sel []uint16) {
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
				true)
		} else {
			_CHECK_COL_WITH_NULLS(
				ht,
				probeVec, buildVec,
				buildKeys, probeKeys,
				nToCheck,
				false)
		}
	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", t))
	}
}

// collect prepares the buildIdx and probeIdx arrays where the buildIdx and
// probeIdx at each index are joined to make an output row. The total number of
// resulting rows is returned.
func (prober *hashJoinProber) collect(batch coldata.Batch, batchSize uint16, sel []uint16) uint16 {
	nResults := uint16(0)

	if prober.spec.outer {
		if sel != nil {
			_COLLECT_RIGHT_OUTER(prober, batchSize, nResults, batch, true)
		} else {
			_COLLECT_RIGHT_OUTER(prober, batchSize, nResults, batch, false)
		}
	} else {
		if sel != nil {
			_COLLECT_NO_OUTER(prober, batchSize, nResults, batch, true)
		} else {
			_COLLECT_NO_OUTER(prober, batchSize, nResults, batch, false)
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
			_DISTINCT_COLLECT_RIGHT_OUTER(prober, batchSize, true)
		} else {
			_DISTINCT_COLLECT_RIGHT_OUTER(prober, batchSize, false)
		}
	} else {
		if sel != nil {
			_DISTINCT_COLLECT_NO_OUTER(prober, batchSize, nResults, true)
		} else {
			_DISTINCT_COLLECT_NO_OUTER(prober, batchSize, nResults, false)
		}
	}

	return nResults
}
