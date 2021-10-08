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
// This file is the execgen template for datum_to_vec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colconv

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type. Note that the signature of the
// return function doesn't contain an error since we assume that the conversion
// must succeed. If for some reason it fails, a panic will be emitted and will
// be caught by the panic-catcher mechanism of the vectorized engine and will
// be propagated as an error accordingly.
func GetDatumToPhysicalFn(ct *types.T) func(tree.Datum) interface{} {
	switch ct.Family() {
	// {{range .}}
	case _TYPE_FAMILY:
		switch ct.Width() {
		// {{range .Widths}}
		case _TYPE_WIDTH:
			return func(datum tree.Datum) interface{} {
				_PRELUDE(datum)
				return _CONVERT(datum)
			}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly unhandled type %s", ct.DebugString()))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
