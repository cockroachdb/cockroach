// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for values_differ.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_, _, _ string) bool {
	execerror.VectorizedInternalPanic("")
}

// */}}

// valuesDiffer takes in two ColVecs as well as values indices to check whether
// the values differ. This function pays attention to NULLs, and two NULL
// values do *not* differ.
func valuesDiffer(
	t coltypes.T, aColVec coldata.Vec, aValueIdx int, bColVec coldata.Vec, bValueIdx int,
) bool {
	switch t {
	// {{range .}}
	case _TYPES_T:
		aCol := aColVec._TemplateType()
		bCol := bColVec._TemplateType()
		aNulls := aColVec.Nulls()
		bNulls := bColVec.Nulls()
		aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
		bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
		if aNull && bNull {
			return false
		} else if aNull || bNull {
			return true
		}
		arg1 := execgen.UNSAFEGET(aCol, aValueIdx)
		arg2 := execgen.UNSAFEGET(bCol, bValueIdx)
		var unique bool
		_ASSIGN_NE(unique, arg1, arg2)
		return unique
	// {{end}}
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unsupported valuesDiffer type %s", t))
		// This code is unreachable, but the compiler cannot infer that.
		return false
	}
}
