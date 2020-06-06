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

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	colexecerror.InternalError("")
}

// */}}

// {{/*
func _REHASH_BODY(
	ctx context.Context,
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
	// {{else}}
	_ = execgen.UNSAFEGET(keys, nKeys-1)
	// {{end}}
	var selIdx int
	for i := 0; i < nKeys; i++ {
		cancelChecker.check(ctx)
		// {{if .HasSel}}
		selIdx = sel[i]
		// {{else}}
		selIdx = i
		// {{end}}
		// {{if .HasNulls}}
		if nulls.NullAt(selIdx) {
			continue
		}
		// {{end}}
		v := execgen.UNSAFEGET(keys, selIdx)
		p := uintptr(buckets[i])
		_ASSIGN_HASH(p, v, _, keys)
		buckets[i] = uint64(p)
	}
	// {{end}}

	// {{/*
}

// */}}

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func rehash(
	ctx context.Context,
	buckets []uint64,
	col coldata.Vec,
	nKeys int,
	sel []int,
	cancelChecker CancelChecker,
	overloadHelper overloadHelper,
	datumAlloc *sqlbase.DatumAlloc,
) {
	// In order to inline the templated code of overloads, we need to have a
	// "_overloadHelper" local variable of type "overloadHelper".
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
					_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, true, true)
				} else {
					_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, false, true)
				}
			} else {
				if sel != nil {
					_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, true, false)
				} else {
					_REHASH_BODY(ctx, buckets, keys, nulls, nKeys, sel, false, false)
				}
			}
			// {{end}}
		}
		// {{end}}
	default:
		colexecerror.InternalError(fmt.Sprintf("unhandled type %s", col.Type()))
	}
}
