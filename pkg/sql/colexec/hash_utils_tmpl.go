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
	"math"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	// HACK: crlfmt removes the "*/}}" comment if it's the last line in the
	// import block. This was picked because it sorts after
	// "pkg/sql/colexec/execgen" and has no deps.
	_ "github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

// {{/*

// Dummy import to pull in "unsafe" package
var _ unsafe.Pointer

// Dummy import to pull in "reflect" package
var _ reflect.SliceHeader

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _GOTYPESLICE is a template Go type slice variable.
type _GOTYPESLICE interface{}

// _ASSIGN_HASH is the template equality function for assigning the first input
// to the result of the hash value of the second input.
func _ASSIGN_HASH(_, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{/*
func _REHASH_BODY(
	ctx context.Context,
	buckets []uint64,
	keys _GOTYPESLICE,
	nulls *coldata.Nulls,
	nKeys uint64,
	sel []uint16,
	_HAS_SEL bool,
	_HAS_NULLS bool,
) { // */}}
	// {{define "rehashBody" -}}
	// Early bounds checks.
	_ = buckets[nKeys-1]
	// {{ if .HasSel }}
	_ = sel[nKeys-1]
	// {{ else }}
	_ = execgen.UNSAFEGET(keys, int(nKeys-1))
	// {{ end }}
	for i := uint64(0); i < nKeys; i++ {
		cancelChecker.check(ctx)
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

// */}}

// rehash takes an element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func rehash(
	ctx context.Context,
	buckets []uint64,
	t coltypes.T,
	col coldata.Vec,
	nKeys uint64,
	sel []uint16,
	cancelChecker CancelChecker,
	decimalScratch decimalOverloadScratch,
) {
	switch t {
	// {{range $hashType := .}}
	case _TYPES_T:
		keys, nulls := col._TemplateType(), col.Nulls()
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
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", t))
	}
}
