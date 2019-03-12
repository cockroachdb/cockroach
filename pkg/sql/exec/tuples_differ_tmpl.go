// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for tuples_differ.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _ASSIGN_NE is the template equality function for assigning the first input
// to the result of the second input != the third input.
func _ASSIGN_NE(_, _, _ string) bool {
	panic("")
}

// */}}

// tuplesDiffer takes in two ColVecs as well as tuple indices to check whether
// the tuples differ.
func tuplesDiffer(
	t types.T, aColVec coldata.Vec, aTupleIdx int, bColVec coldata.Vec, bTupleIdx int, differ *bool,
) error {
	switch t {
	// {{range .}}
	case _TYPES_T:
		aCol := aColVec._TemplateType()
		bCol := bColVec._TemplateType()
		var unique bool
		_ASSIGN_NE("unique", "aCol[aTupleIdx]", "bCol[bTupleIdx]")
		*differ = *differ || unique
		return nil
	// {{end}}
	default:
		return errors.Errorf("unsupported tuplesDiffer type %s", t)
	}
}
