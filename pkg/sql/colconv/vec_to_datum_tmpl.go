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

package colconv

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// VecToDatumConverter is a helper struct that converts vectors from batches to
// their datum representations.
// TODO(yuzefovich): the result of converting the vectors to datums is usually
// put into rowenc.EncDatumRow, so it might make sense to look into creating
// a converter that would store EncDatumRows directly. I prototyped such
// converter, but it showed worse performance both in the microbenchmarks and
// some of the TPCH queries. I think the main reason for the slowdown is that
// the amount of memory allocated increases just because EncDatums take more
// space than Datums. Another thing is that allocating whole W vectors, one
// vector at a time, in VecToDatumConverter is noticeably faster that
// allocating N rows of W length, one row at a time (meaning that
// O(batch width) vs O(batch length) comparison). We could probably play around
// with allocating a big flat []EncDatum slice in which datums from the same
// column are contiguous and then populate the output row in the materializer
// by choosing appropriate elements, but I'm not sure whether it would be more
// performant.
type VecToDatumConverter struct {
	convertedVecs    []tree.Datums
	vecIdxsToConvert []int
	da               rowenc.DatumAlloc
}

var _ execinfra.Releasable = &VecToDatumConverter{}

var vecToDatumConverterPool = sync.Pool{
	New: func() interface{} {
		return &VecToDatumConverter{}
	},
}

func getNewVecToDatumConverter(batchWidth int) *VecToDatumConverter {
	c := vecToDatumConverterPool.Get().(*VecToDatumConverter)
	if cap(c.convertedVecs) < batchWidth {
		c.convertedVecs = make([]tree.Datums, batchWidth)
	} else {
		c.convertedVecs = c.convertedVecs[:batchWidth]
	}
	return c
}

// NewVecToDatumConverter creates a new VecToDatumConverter.
// - batchWidth determines the width of the batches that it will be converting.
// - vecIdxsToConvert determines which vectors need to be converted.
func NewVecToDatumConverter(batchWidth int, vecIdxsToConvert []int) *VecToDatumConverter {
	c := getNewVecToDatumConverter(batchWidth)
	c.vecIdxsToConvert = vecIdxsToConvert
	return c
}

// NewAllVecToDatumConverter is like NewVecToDatumConverter except all of the
// vectors in the batch will be converted.
func NewAllVecToDatumConverter(batchWidth int) *VecToDatumConverter {
	c := getNewVecToDatumConverter(batchWidth)
	if cap(c.vecIdxsToConvert) < batchWidth {
		c.vecIdxsToConvert = make([]int, batchWidth)
	} else {
		c.vecIdxsToConvert = c.vecIdxsToConvert[:batchWidth]
	}
	for i := 0; i < batchWidth; i++ {
		c.vecIdxsToConvert[i] = i
	}
	return c
}

// Release is part of the execinfra.Releasable interface.
func (c *VecToDatumConverter) Release() {
	// Deeply reset the converted vectors so that we don't hold onto the old
	// datums.
	for _, vec := range c.convertedVecs {
		for i := range vec {
			//gcassert:bce
			vec[i] = nil
		}
	}
	*c = VecToDatumConverter{
		convertedVecs: c.convertedVecs[:0],
		// This slice is of integers, so there is no need to reset it deeply.
		vecIdxsToConvert: c.vecIdxsToConvert[:0],
	}
	vecToDatumConverterPool.Put(c)
}

// ConvertBatchAndDeselect converts the selected vectors from the batch while
// performing a deselection step. It doesn't account for the memory used by the
// newly created tree.Datums, so it is up to the caller to do the memory
// accounting.
// NOTE: converted columns are "dense" in regards to the selection vector - if
// there was a selection vector on the batch, only elements that were selected
// are converted, so in order to access the tuple at position tupleIdx, use
// GetDatumColumn(colIdx)[tupleIdx] and *NOT*
// GetDatumColumn(colIdx)[sel[tupleIdx]].
func (c *VecToDatumConverter) ConvertBatchAndDeselect(batch coldata.Batch) {
	if len(c.vecIdxsToConvert) == 0 {
		// No vectors were selected for conversion, so there is nothing to do.
		return
	}
	batchLength := batch.Length()
	if batchLength == 0 {
		return
	}
	// Ensure that convertedVecs are of sufficient length.
	for _, vecIdx := range c.vecIdxsToConvert {
		if cap(c.convertedVecs[vecIdx]) < batchLength {
			c.convertedVecs[vecIdx] = make([]tree.Datum, batchLength)
		} else {
			c.convertedVecs[vecIdx] = c.convertedVecs[vecIdx][:batchLength]
		}
	}
	if c.da.AllocSize < batchLength {
		// Adjust the datum alloc according to the length of the batch since
		// this batch is the longest we've seen so far.
		c.da.AllocSize = batchLength
	}
	sel := batch.Selection()
	vecs := batch.ColVecs()
	for _, vecIdx := range c.vecIdxsToConvert {
		ColVecToDatumAndDeselect(
			c.convertedVecs[vecIdx], vecs[vecIdx], batchLength, sel, &c.da,
		)
	}
}

// ConvertBatch converts the selected vectors from the batch *without*
// performing a deselection step. It doesn't account for the memory used by the
// newly created tree.Datums, so it is up to the caller to do the memory
// accounting.
// NOTE: converted columns are "sparse" in regards to the selection vector - if
// there was a selection vector, only elements that were selected are
// converted, but the results are put at position sel[tupleIdx], so use
// GetDatumColumn(colIdx)[sel[tupleIdx]] and *NOT*
// GetDatumColumn(colIdx)[tupleIdx].
func (c *VecToDatumConverter) ConvertBatch(batch coldata.Batch) {
	c.ConvertVecs(batch.ColVecs(), batch.Length(), batch.Selection())
}

// ConvertVecs converts the selected vectors from vecs *without* performing a
// deselection step. It doesn't account for the memory used by the newly
// created tree.Datums, so it is up to the caller to do the memory accounting.
// Note that this method is equivalent to ConvertBatch with the only difference
// being the fact that it takes in a "disassembled" batch and not coldata.Batch.
// Consider whether you should be using ConvertBatch instead.
func (c *VecToDatumConverter) ConvertVecs(vecs []coldata.Vec, inputLen int, sel []int) {
	if len(c.vecIdxsToConvert) == 0 || inputLen == 0 {
		// No vectors were selected for conversion or there are no tuples to
		// convert, so there is nothing to do.
		return
	}
	// Ensure that convertedVecs are of sufficient length.
	requiredLength := inputLen
	if sel != nil {
		// When sel is non-nil, it might be something like sel = [1023], so we
		// need to allocate up to the largest index mentioned in sel. Here, we
		// rely on the fact that selection vectors are increasing sequences.
		requiredLength = sel[inputLen-1] + 1
	}
	for _, vecIdx := range c.vecIdxsToConvert {
		if cap(c.convertedVecs[vecIdx]) < requiredLength {
			c.convertedVecs[vecIdx] = make([]tree.Datum, requiredLength)
		} else {
			c.convertedVecs[vecIdx] = c.convertedVecs[vecIdx][:requiredLength]
		}
	}
	if c.da.AllocSize < requiredLength {
		// Adjust the datum alloc according to the length of the batch since
		// this batch is the longest we've seen so far.
		c.da.AllocSize = requiredLength
	}
	for _, vecIdx := range c.vecIdxsToConvert {
		ColVecToDatum(
			c.convertedVecs[vecIdx], vecs[vecIdx], inputLen, sel, &c.da,
		)
	}
}

// GetDatumColumn returns the converted column of tree.Datum of the vector on
// position colIdx from the last converted batch.
func (c *VecToDatumConverter) GetDatumColumn(colIdx int) tree.Datums {
	return c.convertedVecs[colIdx]
}

// ColVecToDatumAndDeselect converts a vector of coldata-represented values in
// col into tree.Datum representation while performing a deselection step.
// length specifies the number of values to be converted and sel is an optional
// selection vector. It doesn't account for the memory used by the newly
// created tree.Datums, so it is up to the caller to do the memory accounting.
func ColVecToDatumAndDeselect(
	converted []tree.Datum, col coldata.Vec, length int, sel []int, da *rowenc.DatumAlloc,
) {
	if length == 0 {
		return
	}
	if sel == nil {
		ColVecToDatum(converted, col, length, sel, da)
		return
	}
	if col.MaybeHasNulls() {
		nulls := col.Nulls()
		_VEC_TO_DATUM(converted, col, length, sel, da, true, true, true)
	} else {
		_VEC_TO_DATUM(converted, col, length, sel, da, false, true, true)
	}
}

// ColVecToDatum converts a vector of coldata-represented values in col into
// tree.Datum representation *without* performing a deselection step. It
// doesn't account for the memory used by the newly created tree.Datums, so it
// is up to the caller to do the memory accounting.
func ColVecToDatum(
	converted []tree.Datum, col coldata.Vec, length int, sel []int, da *rowenc.DatumAlloc,
) {
	if length == 0 {
		return
	}
	if col.MaybeHasNulls() {
		nulls := col.Nulls()
		if sel != nil {
			_VEC_TO_DATUM(converted, col, length, sel, da, true, true, false)
		} else {
			_VEC_TO_DATUM(converted, col, length, sel, da, true, false, false)
		}
	} else {
		if sel != nil {
			_VEC_TO_DATUM(converted, col, length, sel, da, false, true, false)
		} else {
			_VEC_TO_DATUM(converted, col, length, sel, da, false, false, false)
		}
	}
}

// {{/*
// This code snippet is a small helper that updates destIdx based on the fact
// whether we want the deselection behavior.
func _SET_DEST_IDX(destIdx, idx int, sel []int, _HAS_SEL bool, _DESELECT bool) { // */}}
	// {{define "setDestIdx" -}}
	// {{if and (.HasSel) (not .Deselect)}}
	//gcassert:bce
	destIdx = sel[idx]
	// {{else}}
	destIdx = idx
	// {{end}}
	// {{end}}
	// {{/*
} // */}}

// {{/*
// This code snippet is a small helper that updates srcIdx based on the fact
// whether there is a selection vector or not.
func _SET_SRC_IDX(srcIdx, idx int, sel []int, _HAS_SEL bool) { // */}}
	// {{define "setSrcIdx" -}}
	// {{if .HasSel}}
	//gcassert:bce
	srcIdx = sel[idx]
	// {{else}}
	srcIdx = idx
	// {{end}}
	// {{end}}
	// {{/*
} // */}}

// {{/*
// This code snippet converts the columnar data in col to the corresponding
// tree.Datum representation that is assigned to converted. length determines
// how many columnar values need to be converted and sel is an optional
// selection vector.
// NOTE: if sel is non-nil, this code snippet might perform the deselection
// step meaning (that it will densely populate converted with only values that
// are selected according to sel) based on _DESELECT value.
// Note: len(converted) must be of sufficient length.
func _VEC_TO_DATUM(
	converted []tree.Datum,
	col coldata.Vec,
	length int,
	sel []int,
	da *rowenc.DatumAlloc,
	_HAS_NULLS bool,
	_HAS_SEL bool,
	_DESELECT bool,
) { // */}}
	// {{define "vecToDatum" -}}
	// {{if or (not _HAS_SEL) (_DESELECT)}}
	_ = converted[length-1]
	// {{end}}
	// {{if .HasSel}}
	_ = sel[length-1]
	// {{end}}
	var idx, destIdx, srcIdx int
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
				_SET_DEST_IDX(destIdx, idx, sel, _HAS_SEL, _DESELECT)
				_SET_SRC_IDX(srcIdx, idx, sel, _HAS_SEL)
				// {{if .HasNulls}}
				if nulls.NullAt(srcIdx) {
					// {{if or (not _HAS_SEL) (_DESELECT)}}
					//gcassert:bce
					// {{end}}
					converted[destIdx] = tree.DNull
					continue
				}
				// {{end}}
				v := da.NewDName(tree.DString(bytes.Get(srcIdx)))
				// {{if or (not _HAS_SEL) (_DESELECT)}}
				//gcassert:bce
				// {{end}}
				converted[destIdx] = v
			}
			return
		}
		for idx = 0; idx < length; idx++ {
			_SET_DEST_IDX(destIdx, idx, sel, _HAS_SEL, _DESELECT)
			_SET_SRC_IDX(srcIdx, idx, sel, _HAS_SEL)
			// {{if .HasNulls}}
			if nulls.NullAt(srcIdx) {
				// {{if or (not _HAS_SEL) (_DESELECT)}}
				//gcassert:bce
				// {{end}}
				converted[destIdx] = tree.DNull
				continue
			}
			// {{end}}
			v := da.NewDString(tree.DString(bytes.Get(srcIdx)))
			// {{if or (not _HAS_SEL) (_DESELECT)}}
			//gcassert:bce
			// {{end}}
			converted[destIdx] = v
		}
	// {{range .Global}}
	case _TYPE_FAMILY:
		switch ct.Width() {
		// {{range .Widths}}
		case _TYPE_WIDTH:
			typedCol := col._VEC_METHOD()
			// {{if and (.Sliceable) (not _HAS_SEL)}}
			_ = typedCol.Get(length - 1)
			// {{end}}
			for idx = 0; idx < length; idx++ {
				_SET_DEST_IDX(destIdx, idx, sel, _HAS_SEL, _DESELECT)
				_SET_SRC_IDX(srcIdx, idx, sel, _HAS_SEL)
				// {{if _HAS_NULLS}}
				if nulls.NullAt(srcIdx) {
					// {{if or (not _HAS_SEL) (_DESELECT)}}
					//gcassert:bce
					// {{end}}
					converted[destIdx] = tree.DNull
					continue
				}
				// {{end}}
				// {{if and (.Sliceable) (not _HAS_SEL)}}
				//gcassert:bce
				// {{end}}
				v := typedCol.Get(srcIdx)
				_ASSIGN_CONVERTED(_converted, v, da)
				// {{if or (not _HAS_SEL) (_DESELECT)}}
				//gcassert:bce
				// {{end}}
				converted[destIdx] = _converted
			}
			// {{end}}
		}
		// {{end}}
	}
	// {{end}}
	// {{/*
} // */}}
