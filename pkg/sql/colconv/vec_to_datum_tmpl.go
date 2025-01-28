// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

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
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/lib/pq/oid"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ colexecerror.StorageError
	_ json.JSON
	_ pgdate.Date
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ uuid.UUID
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
	da               tree.DatumAlloc
}

var _ execreleasable.Releasable = &VecToDatumConverter{}

var vecToDatumConverterPool = sync.Pool{
	New: func() interface{} {
		return &VecToDatumConverter{}
	},
}

// getNewVecToDatumConverter returns a new VecToDatumConverter that is able to
// handle batchWidth number of columns. willRelease indicates whether the caller
// will call Release() on the converter.
func getNewVecToDatumConverter(batchWidth int, willRelease bool) *VecToDatumConverter {
	var c *VecToDatumConverter
	// Having willRelease knob (i.e. not defaulting to using the pool all the
	// time) is justified by the following scenario: there is constant workload
	// running against the cluster (e.g. TPCC) that has the same "access
	// pattern", so all converters used by that workload can be reused very
	// effectively - the width of the batches are the same, etc. However, when a
	// random query comes in and picks up a converter from the sync.Pool, it'll
	// use it once and discard it afterwards. It is likely that for this random
	// query it's better to allocate a fresh converter and not touch the ones in
	// the pool.
	if willRelease {
		c = vecToDatumConverterPool.Get().(*VecToDatumConverter)
	} else {
		c = &VecToDatumConverter{}
	}
	if cap(c.convertedVecs) < batchWidth {
		c.convertedVecs = make([]tree.Datums, batchWidth)
	} else {
		c.convertedVecs = c.convertedVecs[:batchWidth]
	}
	return c
}

// NewVecToDatumConverter creates a new VecToDatumConverter.
//   - batchWidth determines the width of the batches that it will be converting.
//   - vecIdxsToConvert determines which vectors need to be converted.
//   - willRelease indicates whether the caller intends to call Release() on the
//     converter.
func NewVecToDatumConverter(
	batchWidth int, vecIdxsToConvert []int, willRelease bool,
) *VecToDatumConverter {
	c := getNewVecToDatumConverter(batchWidth, willRelease)
	c.vecIdxsToConvert = vecIdxsToConvert
	return c
}

// NewAllVecToDatumConverter is like NewVecToDatumConverter except all of the
// vectors in the batch will be converted.
// NOTE: it is assumed that the caller will Release the returned converter.
func NewAllVecToDatumConverter(batchWidth int) *VecToDatumConverter {
	c := getNewVecToDatumConverter(batchWidth, true /* willRelease */)
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
	if c.da.DefaultAllocSize < batchLength {
		// Adjust the datum alloc according to the length of the batch since
		// this batch is the longest we've seen so far.
		c.da.DefaultAllocSize = batchLength
	}
	vecs := batch.ColVecs()
	c.da.ResetTypeAllocSizes()
	for _, vecIdx := range c.vecIdxsToConvert {
		// Provide the datum allocator with hints about the number of
		// allocations for each type.
		c.da.AddTypeAllocSize(batchLength, vecs[vecIdx].Type().Family())
	}
	sel := batch.Selection()
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
	if c == nil {
		// If the converter is nil, then it wasn't allocated because there are
		// no vectors to convert, so exit early.
		return
	}
	c.ConvertVecs(batch.ColVecs(), batch.Length(), batch.Selection())
}

// ConvertVecs converts the selected vectors from vecs *without* performing a
// deselection step. It doesn't account for the memory used by the newly
// created tree.Datums, so it is up to the caller to do the memory accounting.
// Note that this method is equivalent to ConvertBatch with the only difference
// being the fact that it takes in a "disassembled" batch and not coldata.Batch.
// Consider whether you should be using ConvertBatch instead.
func (c *VecToDatumConverter) ConvertVecs(vecs []*coldata.Vec, inputLen int, sel []int) {
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
	if c.da.DefaultAllocSize < requiredLength {
		// Adjust the datum alloc according to the length of the batch since
		// this batch is the longest we've seen so far.
		c.da.DefaultAllocSize = requiredLength
	}
	c.da.ResetTypeAllocSizes()
	for _, vecIdx := range c.vecIdxsToConvert {
		// Provide the datum allocator with hints about the number of
		// allocations for each type.
		c.da.AddTypeAllocSize(inputLen, vecs[vecIdx].Type().Family())
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
	converted []tree.Datum, col *coldata.Vec, length int, sel []int, da *tree.DatumAlloc,
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
		vecToDatum(converted, col, length, sel, da, true, true, true)
	} else {
		vecToDatum(converted, col, length, sel, da, false, true, true)
	}
}

// ColVecToDatum converts a vector of coldata-represented values in col into
// tree.Datum representation *without* performing a deselection step. It
// doesn't account for the memory used by the newly created tree.Datums, so it
// is up to the caller to do the memory accounting.
func ColVecToDatum(
	converted []tree.Datum, col *coldata.Vec, length int, sel []int, da *tree.DatumAlloc,
) {
	if length == 0 {
		return
	}
	if col.MaybeHasNulls() {
		nulls := col.Nulls()
		if sel != nil {
			vecToDatum(converted, col, length, sel, da, true, true, false)
		} else {
			vecToDatum(converted, col, length, sel, da, true, false, false)
		}
	} else {
		if sel != nil {
			vecToDatum(converted, col, length, sel, da, false, true, false)
		} else {
			vecToDatum(converted, col, length, sel, da, false, false, false)
		}
	}
}

// This template function is a small helper that updates destIdx based on
// whether we want the deselection behavior.
// execgen:inline
// execgen:template<hasSel, deselect>
func setDestIdx(destIdx int, idx int, sel []int, hasSel bool, deselect bool) {
	if hasSel && !deselect {
		//gcassert:bce
		destIdx = sel[idx]
	} else {
		destIdx = idx
	}
}

// execgen:inline
// execgen:template<hasSel>
func setSrcIdx(srcIdx int, idx int, sel []int, hasSel bool) {
	if hasSel {
		//gcassert:bce
		srcIdx = sel[idx]
	} else {
		srcIdx = idx
	}
}

// vecToDatum converts the columnar data in col to the corresponding
// tree.Datum representation that is assigned to converted. length determines
// how many columnar values need to be converted and sel is an optional
// selection vector.
// NOTE: if sel is non-nil, it might perform the deselection
// step meaning (that it will densely populate converted with only values that
// are selected according to sel) based on deselect value.
// Note: len(converted) must be of sufficient length.
// execgen:inline
// execgen:template<hasNulls, hasSel, deselect>
func vecToDatum(
	converted []tree.Datum,
	col *coldata.Vec,
	length int,
	sel []int,
	da *tree.DatumAlloc,
	hasNulls bool,
	hasSel bool,
	deselect bool,
) {
	if !hasSel || deselect {
		_ = converted[length-1]
	}
	if hasSel {
		_ = sel[length-1]
	}
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
				setDestIdx(destIdx, idx, sel, hasSel, deselect)
				setSrcIdx(srcIdx, idx, sel, hasSel)
				if hasNulls {
					if nulls.NullAt(srcIdx) {
						if !hasSel || deselect {
							//gcassert:bce
						}
						converted[destIdx] = tree.DNull
						continue
					}
				}
				v := da.NewDName(tree.DString(bytes.Get(srcIdx)))
				if !hasSel || deselect {
					//gcassert:bce
				}
				converted[destIdx] = v
			}
			return
		}
		for idx = 0; idx < length; idx++ {
			setDestIdx(destIdx, idx, sel, hasSel, deselect)
			setSrcIdx(srcIdx, idx, sel, hasSel)
			if hasNulls {
				if nulls.NullAt(srcIdx) {
					if !hasSel || deselect {
						//gcassert:bce
					}
					converted[destIdx] = tree.DNull
					continue
				}
			}
			v := da.NewDString(tree.DString(bytes.Get(srcIdx)))
			if !hasSel || deselect {
				//gcassert:bce
			}
			converted[destIdx] = v
		}
	// {{range .}}
	case _TYPE_FAMILY:
		switch ct.Width() {
		// {{range .Widths}}
		case _TYPE_WIDTH:
			typedCol := col._VEC_METHOD()
			// {{if .Sliceable}}
			if !hasSel {
				_ = typedCol.Get(length - 1)
			}
			// {{end}}
			for idx = 0; idx < length; idx++ {
				setDestIdx(destIdx, idx, sel, hasSel, deselect)
				setSrcIdx(srcIdx, idx, sel, hasSel)
				if hasNulls {
					if nulls.NullAt(srcIdx) {
						if !hasSel || deselect {
							//gcassert:bce
						}
						converted[destIdx] = tree.DNull
						continue
					}
				}
				// {{if .Sliceable}}
				if !hasSel {
					//gcassert:bce
				}
				// {{end}}
				v := typedCol.Get(srcIdx)
				_ASSIGN_CONVERTED(_converted, v, da)
				if !hasSel || deselect {
					//gcassert:bce
				}
				converted[destIdx] = _converted
			}
			// {{end}}
		}
		// {{end}}
	}
}
