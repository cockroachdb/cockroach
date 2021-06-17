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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ coldataext.Datum
	_ tree.AggType
)

// {{/*

// Declarations to make the template compile properly.

// _GOTYPE is the template variable.
type _GOTYPE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_, _, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// valuesDiffer takes in two ColVecs as well as values indices to check whether
// the values differ. This function pays attention to NULLs.
func valuesDiffer(
	aColVec coldata.Vec, aValueIdx int, bColVec coldata.Vec, bValueIdx int, nullsAreDistinct bool,
) bool {
	switch aColVec.CanonicalTypeFamily() {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch aColVec.Type().Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			aCol := aColVec.TemplateType()
			bCol := bColVec.TemplateType()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return nullsAreDistinct
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool
			_ASSIGN_NE(unique, arg1, arg2, _, aCol, bCol)
			return unique
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported valuesDiffer type %s", aColVec.Type()))
	// This code is unreachable, but the compiler cannot infer that.
	return false
}
