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
// This file is the execgen template for vec_to_datum.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// vecToDatumConverter is a helper struct that converts vectors from batches to
// their datum representations.
// TODO(yuzefovich): the result of converting the vectors to datums is usually
// put into sqlbase.EncDatumRow, so it might make sense to look into creating
// a converter that would store EncDatumRows directly. I prototyped such
// converter, but it showed worse performance both in the microbenchmarks and
// some of the TPCH queries. I think the main reason for the slowdown is that
// the amount of memory allocated increases just because EncDatums take more
// space than Datums. Another thing is that allocating whole W vectors, one
// vector at a time, in vecToDatumConverter is noticeably faster that
// allocating N rows of W length, one row at a time (meaning that
// O(batch width) vs O(batch length) comparison). We could probably play around
// with allocating a big flat []EncDatum slice in which datums from the same
// column are contiguous and then populate the output row in the materializer
// by choosing appropriate elements, but I'm not sure whether it would be more
// performant.
type vecToDatumConverter struct {
	convertedVecs    []tree.Datums
	vecIdxsToConvert []int
	// TODO(yuzefovich): consider customizing the allocation size of DatumAlloc
	// for vectorized purposes.
	da sqlbase.DatumAlloc
}

// newVecToDatumConverter creates a new vecToDatumConverter.
// - batchWidth determines the width of the batches that it will be converting.
// - vecIdxsToConvert determines which vectors need to be converted. It can be
// nil indicating that all vectors need to be converted.
func newVecToDatumConverter(batchWidth int, vecIdxsToConvert []int) *vecToDatumConverter {
	if vecIdxsToConvert == nil {
		vecIdxsToConvert = make([]int, batchWidth)
		for i := range vecIdxsToConvert {
			vecIdxsToConvert[i] = i
		}
	}
	return &vecToDatumConverter{
		convertedVecs:    make([]tree.Datums, batchWidth),
		vecIdxsToConvert: vecIdxsToConvert,
	}
}

// convertBatch converts the selected vectors from the batch.
func (c *vecToDatumConverter) convertBatch(batch coldata.Batch) {
	if len(c.vecIdxsToConvert) == 0 {
		// No vectors were selected for conversion, so there is nothing to do.
		return
	}
	batchLength := batch.Length()
	// Ensure that convertedVecs are of sufficient length.
	if cap(c.convertedVecs[c.vecIdxsToConvert[0]]) < batchLength {
		for _, vecIdx := range c.vecIdxsToConvert {
			c.convertedVecs[vecIdx] = make([]tree.Datum, batchLength)
		}
	} else {
		for _, vecIdx := range c.vecIdxsToConvert {
			c.convertedVecs[vecIdx] = c.convertedVecs[vecIdx][:batchLength]
		}
	}
	sel := batch.Selection()
	vecs := batch.ColVecs()
	for _, vecIdx := range c.vecIdxsToConvert {
		PhysicalTypeColVecToDatum(
			c.convertedVecs[vecIdx], vecs[vecIdx], batchLength, sel, &c.da,
		)
	}
}

// getDatumColumn returns the converted column of tree.Datum of the vector on
// position colIdx from the last converted batch.
// NOTE: this column is "dense" in regards to the selection vector - if there
// was a selection vector on the batch, only elements that were selected are
// converted, so in order to access the tuple at position tupleIdx, use
// getDatumColumn(colIdx)[tupleIdx] and *NOT*
// getDatumColumn(colIdx)[sel[tupleIdx]].
func (c *vecToDatumConverter) getDatumColumn(colIdx int) tree.Datums {
	return c.convertedVecs[colIdx]
}

func PhysicalTypeColVecToDatum(
	converted []tree.Datum, col coldata.Vec, length int, sel []int, da *sqlbase.DatumAlloc,
) {
	if col.MaybeHasNulls() {
		nulls := col.Nulls()
		if sel != nil {
			_VEC_TO_DATUM(converted, col, length, sel, da, true, true)
		} else {
			_VEC_TO_DATUM(converted, col, length, sel, da, true, false)
		}
	} else {
		if sel != nil {
			_VEC_TO_DATUM(converted, col, length, sel, da, false, true)
		} else {
			_VEC_TO_DATUM(converted, col, length, sel, da, false, false)
		}
	}
}

// {{/*
// This code snippet is a small helper that updates tupleIdx based on the fact
// whether there is a selection vector or not.
func _SET_TUPLE_IDX(tupleIdx, idx int, sel []int, _HAS_SEL bool) { // */}}
	// {{define "setTupleIdx" -}}
	// {{if .HasSel}}
	tupleIdx = sel[idx]
	// {{else}}
	tupleIdx = idx
	// {{end}}
	// {{end}}
	// {{/*
} // */}}

// {{/*
// This code snippet converts the columnar data in col to the corresponding
// tree.Datum representation that is assigned to converted. length determines
// how many columnar values need to be converted and sel is an optional
// selection vector.
// NOTE: if sel is non-nil, this code snippet performs the deselection step
// meaning that it densely populates converted with only values that are
// selected according to sel.
// Note: len(converted) must be no less than length.
func _VEC_TO_DATUM(
	converted []tree.Datum,
	col coldata.Vec,
	length int,
	sel []int,
	da *sqlbase.DatumAlloc,
	_HAS_NULLS bool,
	_HAS_SEL bool,
) { // */}}
	// {{define "vecToDatum" -}}
	// {{if .HasSel}}
	sel = sel[:length]
	// {{end}}
	var idx, tupleIdx int
	switch ct := col.Type(); ct.Family() {
	// {{/*
	//     String family is handled separately because the conversion changes
	//     based on the runtime parameter (type's Oid). We don't want to
	//     perform the oid check within the tight loop, so, instead, we choose
	//     to spell out the whole code block manually. Also, reducing the
	//     duplication within String family's case would introduce even more
	//     verbosity and is not worth it.
	// */}}
	case types.StringFamily:
		// Note that there is no need for a copy since casting to a string will
		// do that.
		bytes := col.Bytes()
		if ct.Oid() == oid.T_name {
			for idx = 0; idx < length; idx++ {
				_SET_TUPLE_IDX(tupleIdx, idx, sel, _HAS_SEL)
				// {{if .HasNulls}}
				if nulls.NullAt(tupleIdx) {
					converted[idx] = tree.DNull
					continue
				}
				// {{end}}
				converted[idx] = da.NewDName(tree.DString(bytes.Get(tupleIdx)))
			}
			return
		}
		for idx = 0; idx < length; idx++ {
			_SET_TUPLE_IDX(tupleIdx, idx, sel, _HAS_SEL)
			// {{if .HasNulls}}
			if nulls.NullAt(tupleIdx) {
				converted[idx] = tree.DNull
				continue
			}
			// {{end}}
			converted[idx] = da.NewDString(tree.DString(bytes.Get(tupleIdx)))
		}
	// {{range .Global}}
	case _TYPE_FAMILY:
		switch ct.Width() {
		// {{range .Widths}}
		case _TYPE_WIDTH:
			typedCol := col._VEC_METHOD()
			for idx = 0; idx < length; idx++ {
				_SET_TUPLE_IDX(tupleIdx, idx, sel, _HAS_SEL)
				// {{if _HAS_NULLS}}
				if nulls.NullAt(tupleIdx) {
					converted[idx] = tree.DNull
					continue
				}
				// {{end}}
				_ASSIGN_CONVERTED(converted[idx], typedCol, tupleIdx, da)
			}
			// {{end}}
		}
		// {{end}}
	}
	// {{end}}
	// {{/*
} // */}}
