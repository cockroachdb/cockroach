// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for hash_utils.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexechash

import (
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ = coldataext.Hash
	_ json.JSON
	_ tree.Datum
	_ apd.Context
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
	buckets []uint32,
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
	// {{else if .Global.Sliceable}}
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
		// {{if not (eq .Global.VecMethod "JSON")}}
		// {{/*
		//     No need to decode the JSON value (which is done in Get) since
		//     we'll be operating directly on the underlying []byte.
		// */}}
		// {{if and (not .HasSel) .Global.Sliceable}}
		//gcassert:bce
		// {{end}}
		v := keys.Get(selIdx)
		// {{end}}
		//gcassert:bce
		p := uintptr(buckets[i])
		_ASSIGN_HASH(p, v, keys, selIdx)
		//gcassert:bce
		buckets[i] = T(p)
	}
	// {{end}}

	// {{/*
}

// */}}

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func rehash[T uint32 | uint64](
	buckets []T,
	col *coldata.Vec,
	nKeys int,
	sel []int,
	cancelChecker colexecutils.CancelChecker,
	datumAlloc *tree.DatumAlloc,
) {
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
	cancelChecker.CheckEveryCall()
}
