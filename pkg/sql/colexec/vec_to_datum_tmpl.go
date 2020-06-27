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
	"math/big"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/lib/pq/oid"
)

// vecToDatumConverter is a helper struct that converts vectors from batches to
// their datum representations.
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
		vec := vecs[vecIdx]
		PhysicalTypeColVecToDatum(
			c.convertedVecs[vecIdx], vec, batchLength, sel, &c.da,
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
// This code snippet converts all selected tuples using the provided converter
// to the corresponding tree.Datum representation that is assigned to
// converted. length determines how many columnar values need to be converted
// and sel is an optional selection vector.
// NOTE: if sel is non-nil, this code snippet performs the deselection step
// meaning that it densely populates converted with only values that are
// selected according to sel.
// NOTE: this snippet assumes that there is a function named "_CONVERTER" in
// the scope with the appropriate signature.
// Note: len(converted) must be no less than length.
func _TYPED_SLICE_TO_DATUM(
	converted []tree.Datum,
	length int,
	sel []int,
	_CONVERTER func(tupleIdx int) tree.Datum,
	_HAS_NULLS bool,
	_HAS_SEL bool,
) { // */}}
	// {{define "typedSliceToDatum" -}}
	var idx, tupleIdx int
	for idx = 0; idx < length; idx++ {
		_SET_TUPLE_IDX(tupleIdx, idx, sel, _HAS_SEL)
		// {{if .HasNulls}}
		if nulls.NullAt(tupleIdx) {
			converted[idx] = tree.DNull
			continue
		}
		// {{end}}
		converted[idx] = _CONVERTER(tupleIdx)
	}
	return
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
	switch ct := col.Type(); ct.Family() {
	case types.BoolFamily:
		bools := col.Bool()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			if bools[tupleIdx] {
				return tree.DBoolTrue
			}
			return tree.DBoolFalse
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			int16s := col.Int16()
			_CONVERTER := func(tupleIdx int) tree.Datum {
				return da.NewDInt(tree.DInt(int16s[tupleIdx]))
			}
			_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
		case 32:
			int32s := col.Int32()
			_CONVERTER := func(tupleIdx int) tree.Datum {
				return da.NewDInt(tree.DInt(int32s[tupleIdx]))
			}
			_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
		default:
			int64s := col.Int64()
			_CONVERTER := func(tupleIdx int) tree.Datum {
				return da.NewDInt(tree.DInt(int64s[tupleIdx]))
			}
			_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
		}
	case types.FloatFamily:
		floats := col.Float64()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDFloat(tree.DFloat(floats[tupleIdx]))
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.DecimalFamily:
		decimals := col.Decimal()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			d := da.NewDDecimal(tree.DDecimal{Decimal: decimals[tupleIdx]})
			// Clear the Coeff so that the Set below allocates a new slice for the
			// Coeff.abs field.
			d.Coeff = big.Int{}
			d.Coeff.Set(&decimals[tupleIdx].Coeff)
			return d
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.DateFamily:
		int64s := col.Int64()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDDate(tree.DDate{Date: pgdate.MakeCompatibleDateFromDisk(int64s[tupleIdx])})
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.StringFamily:
		// Note that there is no need for a copy since casting to a string will do
		// that.
		bytes := col.Bytes()
		if ct.Oid() == oid.T_name {
			_CONVERTER := func(tupleIdx int) tree.Datum {
				return da.NewDName(tree.DString(bytes.Get(tupleIdx)))
			}
			_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
		}
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDString(tree.DString(bytes.Get(tupleIdx)))
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.BytesFamily:
		// Note that there is no need for a copy since DBytes uses a string as
		// underlying storage, which will perform the copy for us.
		bytes := col.Bytes()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDBytes(tree.DBytes(bytes.Get(tupleIdx)))
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.OidFamily:
		int64s := col.Int64()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDOid(tree.MakeDOid(tree.DInt(int64s[tupleIdx])))
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.UuidFamily:
		// Note that there is no need for a copy because uuid.FromBytes will perform
		// a copy.
		bytes := col.Bytes()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			id, err := uuid.FromBytes(bytes.Get(tupleIdx))
			if err != nil {
				colexecerror.InternalError(err)
			}
			return da.NewDUuid(tree.DUuid{UUID: id})
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.TimestampFamily:
		timestamps := col.Timestamp()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDTimestamp(tree.DTimestamp{Time: timestamps[tupleIdx]})
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.TimestampTZFamily:
		timestamps := col.Timestamp()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDTimestampTZ(tree.DTimestampTZ{Time: timestamps[tupleIdx]})
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	case types.IntervalFamily:
		intervals := col.Interval()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return da.NewDInterval(tree.DInterval{Duration: intervals[tupleIdx]})
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	default:
		datumVec := col.Datum()
		_CONVERTER := func(tupleIdx int) tree.Datum {
			return datumVec.Get(tupleIdx).(*coldataext.Datum).Datum
		}
		_TYPED_SLICE_TO_DATUM(converted, length, sel, _CONVERTER, _HAS_NULLS, _HAS_SEL)
	}
	// {{end}}
	// {{/*
} // */}}
