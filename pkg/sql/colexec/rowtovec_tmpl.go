// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for rowtovec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ apd.Context
	_ duration.Duration
	_ encoding.Direction
	_ json.JSON
	_ ipaddr.IPAddr
)

// EncDatumRowToColVecs converts the provided rowenc.EncDatumRow to the columnar
// format and writes the converted values at the rowIdx position of the given
// vecs.
// Note: it is the caller's responsibility to perform the memory accounting.
func EncDatumRowToColVecs(
	row rowenc.EncDatumRow, rowIdx int, vecs coldata.TypedVecs, alloc *tree.DatumAlloc,
) {
	for vecIdx, vec := range vecs.Vecs {
		if row[vecIdx].Datum == nil {
			if err := row[vecIdx].EnsureDecoded(vec.Type(), alloc); err != nil {
				colexecerror.InternalError(err)
			}
		}
		datum := row[vecIdx].Datum
		if datum == tree.DNull {
			vecs.Nulls[vecIdx].SetNull(rowIdx)
		} else {
			t := vec.Type()
			family := t.Family()
			if family == types.INetFamily && vec.CanonicalTypeFamily() == typeconv.DatumVecCanonicalTypeFamily {
				// Use this trick so that we fall into the default case that
				// handles all datum-backed types.
				family = typeconv.DatumVecCanonicalTypeFamily + 1
			}
			switch family {
			// {{range .}}
			case _TYPE_FAMILY:
				switch t.Width() {
				// {{range .Widths}}
				case _TYPE_WIDTH:
					_PRELUDE(datum)
					v := _CONVERT(datum)
					vecs._TYPECols[vecs.ColsMap[vecIdx]].Set(rowIdx, v)
					// {{end}}
				}
				// {{end}}
			}
		}
	}
}
