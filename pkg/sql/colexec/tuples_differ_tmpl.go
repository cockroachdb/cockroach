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
// This file is the execgen template for tuples_differ.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
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

// tuplesDiffer takes in two ColVecs as well as tuple indices to check whether
// the tuples differ.
func tuplesDiffer(
	t coltypes.T,
	aColVec coldata.Vec,
	aTupleIdx int,
	bColVec coldata.Vec,
	bTupleIdx int,
	differ *bool,
) error {
	switch t {
	// {{range .}}
	case _TYPES_T:
		aCol := aColVec._TemplateType()
		bCol := bColVec._TemplateType()
		var unique bool
		arg1 := execgen.UNSAFEGET(aCol, aTupleIdx)
		arg2 := execgen.UNSAFEGET(bCol, bTupleIdx)
		_ASSIGN_NE(unique, arg1, arg2)
		*differ = *differ || unique
		return nil
	// {{end}}
	default:
		return errors.Errorf("unsupported tuplesDiffer type %s", t)
	}
}
