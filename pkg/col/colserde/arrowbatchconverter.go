// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colserde

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// ConversionMode describes how ArrowBatchConverter will be utilized.
type ConversionMode int

const (
	// BiDirectional indicates that both arrow-to-batch and batch-to-arrow
	// conversions can happen.
	BiDirectional ConversionMode = iota
	// ArrowToBatchOnly indicates that only arrow-to-batch conversion can
	// happen.
	ArrowToBatchOnly
	// BatchToArrowOnly indicates that only batch-to-arrow conversion can
	// happen.
	BatchToArrowOnly
)

// ArrowBatchConverter converts batches to arrow column data
// ([]array.Data) and back again.
type ArrowBatchConverter struct {
	typs []*types.T

	// TODO(yuzefovich): perform memory accounting for these slices.
	scratch struct {
		// arrowData is used as scratch space returned as the corresponding
		// conversion result.
		arrowData []array.Data
		// buffers is scratch space for two or three buffers per element in
		// arrowData.
		buffers [][]*memory.Buffer
		// values and offsets will only be populated if there is at least one
		// type that needs three buffers. If populated, then vecIdx'th elements
		// of the slices will store the values and offsets, respectively, of the
		// last serialized representation of vecIdx'th vector which allows us to
		// reuse the same memory across BatchToArrow calls.
		values  [][]byte
		offsets [][]int32
	}
}

//gcassert:inline
func numBuffers(t *types.T) int {
	// Most types need three buffers: one for the nulls, one for the values, and
	// one for the offsets; however, some simple types don't need the offsets
	// buffer, so two buffers are sufficient.
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	case types.BoolFamily, types.IntFamily, types.FloatFamily:
		return 2
	default:
		return 3
	}
}

// NewArrowBatchConverter converts coldata.Batches to []array.Data and back
// again according to the schema specified by typs. Converting data that does
// not conform to typs results in undefined behavior.
func NewArrowBatchConverter(typs []*types.T, mode ConversionMode) (*ArrowBatchConverter, error) {
	c := &ArrowBatchConverter{typs: typs}
	if mode == ArrowToBatchOnly {
		// All the allocations below are only used in BatchToArrow, so we don't
		// need to allocate them.
		return c, nil
	}
	c.scratch.arrowData = make([]array.Data, len(typs))
	c.scratch.buffers = make([][]*memory.Buffer, len(typs))
	// Calculate the number of buffers needed for all types to be able to batch
	// allocate them below.
	var numBuffersTotal int
	var needOffsets bool
	for _, t := range typs {
		n := numBuffers(t)
		numBuffersTotal += n
		needOffsets = needOffsets || n == 3
	}
	buffers := make([]memory.Buffer, numBuffersTotal)
	var buffersIdx int
	for i, t := range typs {
		c.scratch.buffers[i] = make([]*memory.Buffer, numBuffers(t))
		for j := range c.scratch.buffers[i] {
			c.scratch.buffers[i][j] = &buffers[buffersIdx]
			buffersIdx++
		}
	}
	if needOffsets {
		c.scratch.values = make([][]byte, len(typs))
		c.scratch.offsets = make([][]int32, len(typs))
	}
	return c, nil
}

// BatchToArrow converts the first batch.Length elements of the batch into an
// arrow []array.Data. It is assumed that the batch is not larger than
// coldata.BatchSize(). The returned []array.Data may only be used until the
// next call to BatchToArrow.
func (c *ArrowBatchConverter) BatchToArrow(batch coldata.Batch) ([]array.Data, error) {
	if batch.Width() != len(c.typs) {
		return nil, errors.AssertionFailedf("mismatched batch width and schema length: %d != %d", batch.Width(), len(c.typs))
	}
	n := batch.Length()
	for vecIdx, typ := range c.typs {
		vec := batch.ColVec(vecIdx)

		var nulls *coldata.Nulls
		if vec.MaybeHasNulls() {
			nulls = vec.Nulls()
			// To conform to the Arrow spec, zero out all trailing null values.
			nulls.Truncate(batch.Length())
		}

		var (
			values       []byte
			offsetsBytes []byte
		)

		if f := typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()); f == types.IntFamily ||
			f == types.FloatFamily || f == types.BoolFamily {
			// For ints and floats we have a fast path where we cast the
			// underlying slice directly to the arrow format. Bools are handled
			// in a special manner by casting to []byte since []byte and []bool
			// have exactly the same structure (only a single bit of each byte
			// is addressable).
			//
			// dataHeader is the reflect.SliceHeader of the coldata.Vec's
			// underlying data slice that we are casting to bytes.
			var dataHeader *reflect.SliceHeader
			// datumSize is the size of one datum that we are casting to a byte
			// slice.
			var datumSize int64
			switch f {
			case types.BoolFamily:
				bools := vec.Bool()[:n]
				dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&bools))
				datumSize = 1
			case types.IntFamily:
				switch typ.Width() {
				case 16:
					ints := vec.Int16()[:n]
					dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
					datumSize = memsize.Int16
				case 32:
					ints := vec.Int32()[:n]
					dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
					datumSize = memsize.Int32
				case 0, 64:
					ints := vec.Int64()[:n]
					dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
					datumSize = memsize.Int64
				default:
					panic(fmt.Sprintf("unexpected int width: %d", typ.Width()))
				}
			case types.FloatFamily:
				floats := vec.Float64()[:n]
				dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&floats))
				datumSize = memsize.Float64
			}
			// Update values to point to the underlying slices while keeping the
			// offsetBytes unset.
			valuesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&values))
			valuesHeader.Data = dataHeader.Data
			valuesHeader.Len = dataHeader.Len * int(datumSize)
			valuesHeader.Cap = dataHeader.Cap * int(datumSize)
		} else {
			values = c.scratch.values[vecIdx][:0]
			offsets := c.scratch.offsets[vecIdx][:0]
			if cap(offsets) < n+1 {
				offsets = make([]int32, 0, n+1)
			}
			switch f {
			case types.BytesFamily:
				values, offsets = vec.Bytes().ToArrowSerializationFormat(n, values, offsets)
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case types.JsonFamily:
				values, offsets = vec.JSON().Bytes.ToArrowSerializationFormat(n, values, offsets)
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case types.DecimalFamily:
				decimals := vec.Decimal()[:n]
				// We don't know exactly how big a decimal will serialize to.
				// Let's estimate 16 bytes.
				if cap(values) < n*16 {
					values = make([]byte, 0, n*16)
				}
				for i := range decimals {
					offsets = append(offsets, int32(len(values)))
					if nulls != nil && nulls.NullAt(i) {
						continue
					}
					// See apd.Decimal.String(): we use the 'G' format string for
					// serialization.
					values = decimals[i].Append(values, 'G')
				}
				offsets = append(offsets, int32(len(values)))
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case types.IntervalFamily:
				intervals := vec.Interval()[:n]
				intervalSize := int(memsize.Int64) * 3
				if cap(values) < intervalSize*n {
					values = make([]byte, intervalSize*n)
				}
				var curNonNullInterval int
				for i := range intervals {
					offsets = append(offsets, int32(curNonNullInterval*intervalSize))
					if nulls != nil && nulls.NullAt(i) {
						continue
					}
					nanos, months, days, err := intervals[i].Encode()
					if err != nil {
						return nil, err
					}
					curSlice := values[intervalSize*curNonNullInterval : intervalSize*(curNonNullInterval+1)]
					binary.LittleEndian.PutUint64(curSlice[0:memsize.Int64], uint64(nanos))
					binary.LittleEndian.PutUint64(curSlice[memsize.Int64:memsize.Int64*2], uint64(months))
					binary.LittleEndian.PutUint64(curSlice[memsize.Int64*2:memsize.Int64*3], uint64(days))
					curNonNullInterval++
				}
				values = values[:intervalSize*curNonNullInterval]
				offsets = append(offsets, int32(len(values)))
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case types.TimestampTZFamily:
				timestamps := vec.Timestamp()[:n]
				// See implementation of time.MarshalBinary.
				const timestampSize = 14
				if cap(values) < n*timestampSize {
					values = make([]byte, 0, n*timestampSize)
				}
				for i := range timestamps {
					offsets = append(offsets, int32(len(values)))
					if nulls != nil && nulls.NullAt(i) {
						continue
					}
					marshaled, err := timestamps[i].MarshalBinary()
					if err != nil {
						return nil, err
					}
					values = append(values, marshaled...)
				}
				offsets = append(offsets, int32(len(values)))
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case typeconv.DatumVecCanonicalTypeFamily:
				datums := vec.Datum().Window(0 /* start */, n)
				// Make a very very rough estimate of the number of bytes we'll have to
				// allocate for the datums in this vector. This will likely be an
				// undercount, but the estimate is better than nothing.
				size, _ := tree.DatumTypeSize(typ)
				if cap(values) < n*int(size) {
					values = make([]byte, 0, n*int(size))
				}
				for i := 0; i < n; i++ {
					offsets = append(offsets, int32(len(values)))
					if nulls != nil && nulls.NullAt(i) {
						continue
					}
					var err error
					values, err = datums.MarshalAt(values, i)
					if err != nil {
						return nil, err
					}
				}
				offsets = append(offsets, int32(len(values)))
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			default:
				panic(fmt.Sprintf("unsupported type for conversion to arrow data %s", typ))
			}

			// Store the serialized slices as scratch space for the next call.
			c.scratch.values[vecIdx] = values
			c.scratch.offsets[vecIdx] = offsets
		}

		var arrowBitmap []byte
		if nulls != nil {
			arrowBitmap = nulls.NullBitmap()
		}

		// Construct the underlying arrow buffers.
		// WARNING: The ordering of construction is critical.
		c.scratch.buffers[vecIdx][0].Reset(arrowBitmap)
		bufferIdx := 1
		if offsetsBytes != nil {
			c.scratch.buffers[vecIdx][1].Reset(offsetsBytes)
			bufferIdx++
		}
		c.scratch.buffers[vecIdx][bufferIdx].Reset(values)

		// Create the data from the buffers. It might be surprising that we don't
		// set a type or a null count, but these fields are not used in the way that
		// we're using array.Data. The part we care about are the buffers. Type
		// information is inferred from the ArrowBatchConverter schema, null count
		// is an optimization we can use when working with nulls, and childData is
		// only used for nested types like Lists, Structs, or Unions.
		c.scratch.arrowData[vecIdx].Reset(
			nil /* dtype */, n, c.scratch.buffers[vecIdx], nil /* childData */, 0 /* nulls */, 0, /* offset */
		)
	}
	return c.scratch.arrowData, nil
}

// unsafeCastOffsetsArray unsafe-casts the input offsetsBytes slice to point at
// the data inside of the int32 offsets slice.
func unsafeCastOffsetsArray(offsetsInt32 []int32, offsetsBytes *[]byte) {
	// Cast int32Offsets to []byte.
	int32Header := (*reflect.SliceHeader)(unsafe.Pointer(&offsetsInt32))
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(offsetsBytes))
	bytesHeader.Data = int32Header.Data
	bytesHeader.Len = int32Header.Len * int(memsize.Int32)
	bytesHeader.Cap = int32Header.Cap * int(memsize.Int32)
}

// ArrowToBatch converts []array.Data to a coldata.Batch. There must not be
// more than coldata.BatchSize() elements in data.
//
// The passed in batch is overwritten, but after this method returns it stays
// valid as long as `data` stays valid. Callers can use this to control the
// lifetimes of the batches, saving allocations when they can be reused (i.e.
// reused by passing them back into this function).
//
// The passed in data is also mutated (we store nulls differently than arrow and
// the adjustment is done in place).
func (c *ArrowBatchConverter) ArrowToBatch(
	data []array.Data, batchLength int, b coldata.Batch,
) error {
	if len(data) != len(c.typs) {
		return errors.Errorf("mismatched data and schema length: %d != %d", len(data), len(c.typs))
	}

	for i, typ := range c.typs {
		vec := b.ColVec(i)
		d := &data[i]

		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BytesFamily:
			valueBytes, offsets := getValueBytesAndOffsets(d, vec.Nulls(), batchLength)
			coldata.BytesFromArrowSerializationFormat(vec.Bytes(), valueBytes, offsets)

		case types.JsonFamily:
			valueBytes, offsets := getValueBytesAndOffsets(d, vec.Nulls(), batchLength)
			coldata.BytesFromArrowSerializationFormat(&vec.JSON().Bytes, valueBytes, offsets)

		case types.DecimalFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			valueBytes, offsets := getValueBytesAndOffsets(d, vec.Nulls(), batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			vecArr := vec.Decimal()
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					if err := vecArr[j].UnmarshalText(valueBytes[offsets[j]:offsets[j+1]]); err != nil {
						return err
					}
				}
			}

		case types.TimestampTZFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			valueBytes, offsets := getValueBytesAndOffsets(d, vec.Nulls(), batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			vecArr := vec.Timestamp()
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					if err := vecArr[j].UnmarshalBinary(valueBytes[offsets[j]:offsets[j+1]]); err != nil {
						return err
					}
				}
			}

		case types.IntervalFamily:
			// TODO(asubiotto): this serialization is quite inefficient compared to
			//  the direct casts below. Improve it.
			valueBytes, offsets := getValueBytesAndOffsets(d, vec.Nulls(), batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			vecArr := vec.Interval()
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					intervalBytes := valueBytes[offsets[j]:offsets[j+1]]
					var err error
					vecArr[j], err = duration.Decode(
						int64(binary.LittleEndian.Uint64(intervalBytes[0:memsize.Int64])),
						int64(binary.LittleEndian.Uint64(intervalBytes[memsize.Int64:memsize.Int64*2])),
						int64(binary.LittleEndian.Uint64(intervalBytes[memsize.Int64*2:memsize.Int64*3])),
					)
					if err != nil {
						return err
					}
				}
			}

		case typeconv.DatumVecCanonicalTypeFamily:
			valueBytes, offsets := getValueBytesAndOffsets(d, vec.Nulls(), batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			vecArr := vec.Datum()
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					if err := vecArr.UnmarshalTo(j, valueBytes[offsets[j]:offsets[j+1]]); err != nil {
						return err
					}
				}
			}

		default:
			// For bools, integers, and floats we can just directly cast the
			// slice without performing the copy.
			//
			// However, we have to be careful to set the capacity on each slice
			// explicitly to protect memory regions that come after the slice
			// from corruption in case the slice will be appended to in the
			// future.
			var col coldata.Column
			switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
			case types.BoolFamily:
				buffers := d.Buffers()
				// Nulls are always stored in the first buffer whereas the
				// second one is the byte representation of the boolean vector
				// (where each byte corresponds to a single bool value), so we
				// need to unsafely cast from []byte to []bool.
				nullsBitmap, bytes := buffers[0].Bytes(), buffers[1].Bytes()
				vec.Nulls().SetNullBitmap(nullsBitmap, batchLength)
				bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
				var bools coldata.Bools
				boolsHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bools))
				boolsHeader.Data = bytesHeader.Data
				boolsHeader.Len = batchLength
				boolsHeader.Cap = batchLength
				col = bools[:batchLength:batchLength]
			case types.IntFamily:
				switch typ.Width() {
				case 16:
					intArr := array.NewInt16Data(d)
					vec.Nulls().SetNullBitmap(intArr.NullBitmapBytes(), batchLength)
					int16s := coldata.Int16s(intArr.Int16Values())
					col = int16s[:len(int16s):len(int16s)]
				case 32:
					intArr := array.NewInt32Data(d)
					vec.Nulls().SetNullBitmap(intArr.NullBitmapBytes(), batchLength)
					int32s := coldata.Int32s(intArr.Int32Values())
					col = int32s[:len(int32s):len(int32s)]
				case 0, 64:
					intArr := array.NewInt64Data(d)
					vec.Nulls().SetNullBitmap(intArr.NullBitmapBytes(), batchLength)
					int64s := coldata.Int64s(intArr.Int64Values())
					col = int64s[:len(int64s):len(int64s)]
				default:
					panic(fmt.Sprintf("unexpected int width: %d", typ.Width()))
				}
			case types.FloatFamily:
				floatArr := array.NewFloat64Data(d)
				vec.Nulls().SetNullBitmap(floatArr.NullBitmapBytes(), batchLength)
				float64s := coldata.Float64s(floatArr.Float64Values())
				col = float64s[:len(float64s):len(float64s)]
			default:
				panic(
					fmt.Sprintf("unsupported type for conversion to column batch %s", d.DataType().Name()),
				)
			}
			vec.SetCol(col)
		}

		// Eagerly release our data references to make sure they can be collected
		// as quickly as possible as we copy each (or simply reference each) by
		// coldata.Vecs below.
		data[i] = array.Data{}
	}
	b.SetSelection(false)
	b.SetLength(batchLength)
	return nil
}

// getValueBytesAndOffsets picks into d and returns two of the three buffers
// (the values and the offsets). The remaining buffer that corresponds to the
// null bitmap is not returned since the passed-in nulls are updated in place.
func getValueBytesAndOffsets(
	d *array.Data, nulls *coldata.Nulls, batchLength int,
) ([]byte, []int32) {
	bytesArr := array.NewBinaryData(d)
	nulls.SetNullBitmap(bytesArr.NullBitmapBytes(), batchLength)
	valueBytes := bytesArr.ValueBytes()
	if valueBytes == nil {
		// All bytes values are empty, so the representation is solely with the
		// offsets slice, so create an empty slice so that the conversion
		// corresponds.
		valueBytes = make([]byte, 0)
	}
	offsets := bytesArr.ValueOffsets()
	return valueBytes, offsets[:batchLength+1]
}

// Release should be called once the converter is no longer needed so that its
// memory could be GCed.
func (c *ArrowBatchConverter) Release() {
	*c = ArrowBatchConverter{}
}
