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
// This file is the execgen template for hash_utils.eg.go. It's formatted in a
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ coldataext.Datum
	_ json.JSON
)

// {{/*

// _GOTYPESLICE is a template Go type slice variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _ASSIGN_HASH is the template equality function for assigning the first input
// to the result of the hash value of the second input.
func _ASSIGN_HASH(_, _, _, _ interface{}) uint64 {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// {{/*
func _REHASH_BODY(
	buckets []uint64,
	keys _GOTYPESLICE,
	nulls *coldata.Nulls,
	nKeys int,
	sel []int,
	_HAS_SEL bool,
	_HAS_NULLS bool,
) { // */}}
	// {{define "rehashBody" -}}
	// Early bounds checks.
	_ = buckets[nKeys-1]
	// {{if .HasSel}}
	_ = sel[nKeys-1]
	// {{else if .Sliceable}}
	_ = keys.Get(nKeys - 1)
	// {{end}}
	var selIdx int
	for i := 0; i < nKeys; i++ {
		// {{if .HasSel}}
		//gcassert:bce
		selIdx = sel[i]
		// {{else}}
		selIdx = i
		// {{end}}
		// {{if .HasNulls}}
		if nulls.NullAt(selIdx) {
			continue
		}
		// {{end}}
		// {{if .Sliceable}}
		//gcassert:bce
		// {{end}}
		v := keys.Get(selIdx)
		//gcassert:bce
		p := uintptr(buckets[i])
		_ASSIGN_HASH(p, v, _, keys)
		//gcassert:bce
		buckets[i] = uint64(p)
	}
	cancelChecker.CheckEveryCall()
	// {{end}}

	// {{/*
}

// */}}

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func rehash(
	buckets []uint64,
	col coldata.Vec,
	nKeys int,
	sel []int,
	cancelChecker colexecutils.CancelChecker,
	overloadHelper *execgen.OverloadHelper,
	datumAlloc *rowenc.DatumAlloc,
) {
	// In order to inline the templated code of overloads, we need to have a
	// "_overloadHelper" local variable of type "execgen.OverloadHelper".
	_overloadHelper := overloadHelper
	switch col.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch col.Type().Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			keys, nulls := col.TemplateType(), col.Nulls()
			if col.MaybeHasNulls() {
				if sel != nil {
					_REHASH_BODY(buckets, keys, nulls, nKeys, sel, true, true)
				} else {
					_REHASH_BODY(buckets, keys, nulls, nKeys, sel, false, true)
				}
			} else {
				if sel != nil {
					_REHASH_BODY(buckets, keys, nulls, nKeys, sel, true, false)
				} else {
					_REHASH_BODY(buckets, keys, nulls, nKeys, sel, false, false)
				}
			}
			// {{end}}
		}
		// {{end}}
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", col.Type()))
	}
}
