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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// ArrowBatchConverter converts batches to arrow column data
// ([]*array.Data) and back again.
type ArrowBatchConverter struct {
	typs []*types.T

	// builders are the set of builders that need to be kept around in order to
	// construct arrow representations of certain types when they cannot be simply
	// cast from our current data representation.
	builders struct {
		// boolBuilder builds arrow bool columns as a bitmap from a bool slice.
		boolBuilder *array.BooleanBuilder
		// binaryBuilder builds arrow []byte columns as one []byte slice with
		// accompanying offsets from a [][]byte slice.
		binaryBuilder *array.BinaryBuilder
	}

	scratch struct {
		// arrowData is used as scratch space returned as the corresponding
		// conversion result.
		arrowData []*array.Data
		// buffers is scratch space for exactly two buffers per element in
		// arrowData.
		buffers [][]*memory.Buffer
	}
}

// NewArrowBatchConverter converts coldata.Batches to []*array.Data and back
// again according to the schema specified by typs. Converting data that does
// not conform to typs results in undefined behavior.
func NewArrowBatchConverter(typs []*types.T) (*ArrowBatchConverter, error) {
	c := &ArrowBatchConverter{typs: typs}
	c.builders.boolBuilder = array.NewBooleanBuilder(memory.DefaultAllocator)
	c.builders.binaryBuilder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	c.scratch.arrowData = make([]*array.Data, len(typs))
	c.scratch.buffers = make([][]*memory.Buffer, len(typs))
	for i := range c.scratch.buffers {
		// Some types need only two buffers: one for the nulls, and one for the
		// values, but others (i.e. Bytes) need an extra buffer for the
		// offsets.
		c.scratch.buffers[i] = make([]*memory.Buffer, 0, 3)
	}
	return c, nil
}

const (
	sizeOfInt16   = int(unsafe.Sizeof(int16(0)))
	sizeOfInt32   = int(unsafe.Sizeof(int32(0)))
	sizeOfInt64   = int(unsafe.Sizeof(int64(0)))
	sizeOfFloat64 = int(unsafe.Sizeof(float64(0)))
)

// BatchToArrow converts the first batch.Length elements of the batch into an
// arrow []*array.Data. It is assumed that the batch is not larger than
// coldata.BatchSize(). The returned []*array.Data may only be used until the
// next call to BatchToArrow.
func (c *ArrowBatchConverter) BatchToArrow(batch coldata.Batch) ([]*array.Data, error) {
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

		data, err := c.batchToArrowSpecialType(vec, n, nulls)
		if err != nil {
			return nil, err
		}
		if data != nil {
			c.scratch.arrowData[vecIdx] = data
			continue
		}

		var (
			values  []byte
			offsets []byte

			// dataHeader is the reflect.SliceHeader of the coldata.Vec's underlying
			// data slice that we are casting to bytes.
			dataHeader *reflect.SliceHeader
			// datumSize is the size of one datum that we are casting to a byte slice.
			datumSize int
		)

		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BytesFamily:
			var int32Offsets []int32
			values, int32Offsets = vec.Bytes().ToArrowSerializationFormat(n)
			// Cast int32Offsets to []byte.
			int32Header := (*reflect.SliceHeader)(unsafe.Pointer(&int32Offsets))
			offsetsHeader := (*reflect.SliceHeader)(unsafe.Pointer(&offsets))
			offsetsHeader.Data = int32Header.Data
			offsetsHeader.Len = int32Header.Len * sizeOfInt32
			offsetsHeader.Cap = int32Header.Cap * sizeOfInt32
		case types.IntFamily:
			switch typ.Width() {
			case 16:
				ints := vec.Int16()[:n]
				dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
				datumSize = sizeOfInt16
			case 32:
				ints := vec.Int32()[:n]
				dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
				datumSize = sizeOfInt32
			case 0, 64:
				ints := vec.Int64()[:n]
				dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
				datumSize = sizeOfInt64
			default:
				panic(fmt.Sprintf("unexpected int width: %d", typ.Width()))
			}
		case types.FloatFamily:
			floats := vec.Float64()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&floats))
			datumSize = sizeOfFloat64
		default:
			panic(fmt.Sprintf("unsupported type for conversion to arrow data %s", typ))
		}

		// Cast values if not set (mostly for non-byte types).
		if values == nil {
			valuesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&values))
			valuesHeader.Data = dataHeader.Data
			valuesHeader.Len = dataHeader.Len * datumSize
			valuesHeader.Cap = dataHeader.Cap * datumSize
		}

		var arrowBitmap []byte
		if nulls != nil {
			arrowBitmap = nulls.NullBitmap()
		}

		// Construct the underlying arrow buffers.
		// WARNING: The ordering of construction is critical.
		c.scratch.buffers[vecIdx] = c.scratch.buffers[vecIdx][:0]
		c.scratch.buffers[vecIdx] = append(c.scratch.buffers[vecIdx], memory.NewBufferBytes(arrowBitmap))
		if offsets != nil {
			c.scratch.buffers[vecIdx] = append(c.scratch.buffers[vecIdx], memory.NewBufferBytes(offsets))
		}
		c.scratch.buffers[vecIdx] = append(c.scratch.buffers[vecIdx], memory.NewBufferBytes(values))

		// Create the data from the buffers. It might be surprising that we don't
		// set a type or a null count, but these fields are not used in the way that
		// we're using array.Data. The part we care about are the buffers. Type
		// information is inferred from the ArrowBatchConverter schema, null count
		// is an optimization we can use when working with nulls, and childData is
		// only used for nested types like Lists, Structs, or Unions.
		c.scratch.arrowData[vecIdx] = array.NewData(
			nil /* dtype */, n, c.scratch.buffers[vecIdx], nil /* childData */, 0 /* nulls */, 0, /* offset */
		)
	}
	return c.scratch.arrowData, nil
}

// batchToArrowSpecialType checks whether the vector requires special handling
// and performs the conversion to the Arrow format if so. If we support "native"
// conversion for this vector, then nil is returned.
func (c *ArrowBatchConverter) batchToArrowSpecialType(
	vec coldata.Vec, n int, nulls *coldata.Nulls,
) (*array.Data, error) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(vec.Type().Family()) {
	case types.BoolFamily:
		c.builders.boolBuilder.AppendValues(vec.Bool()[:n], nil /* valid */)
		data := c.builders.boolBuilder.NewBooleanArray().Data()
		// Overwrite incorrect null bitmap (that has all values as "valid")
		// with the actual null bitmap. Note that if we actually don't have
		// any nulls, we use a bitmap with zero length for it in order to
		// reduce the size of serialized representation.
		var arrowBitmap []byte
		if nulls != nil {
			arrowBitmap = nulls.NullBitmap()
		}
		data.Buffers()[0] = memory.NewBufferBytes(arrowBitmap)
		return data, nil

	case types.DecimalFamily:
		decimals := vec.Decimal()[:n]
		for i := range decimals {
			if nulls != nil && nulls.NullAt(i) {
				c.builders.binaryBuilder.AppendNull()
				continue
			}
			marshaled, err := decimals[i].MarshalText()
			if err != nil {
				return nil, err
			}
			c.builders.binaryBuilder.Append(marshaled)
		}
		return c.builders.binaryBuilder.NewBinaryArray().Data(), nil

	case types.TimestampTZFamily:
		timestamps := vec.Timestamp()[:n]
		for i := range timestamps {
			if nulls != nil && nulls.NullAt(i) {
				c.builders.binaryBuilder.AppendNull()
				continue
			}
			marshaled, err := timestamps[i].MarshalBinary()
			if err != nil {
				return nil, err
			}
			c.builders.binaryBuilder.Append(marshaled)
		}
		return c.builders.binaryBuilder.NewBinaryArray().Data(), nil

	case types.IntervalFamily:
		intervals := vec.Interval()[:n]
		// Appending to the binary builder will copy the bytes, so it's safe to
		// reuse a scratch bytes to encode the interval into.
		scratchIntervalBytes := make([]byte, sizeOfInt64*3)
		for i := range intervals {
			if nulls != nil && nulls.NullAt(i) {
				c.builders.binaryBuilder.AppendNull()
				continue
			}
			nanos, months, days, err := intervals[i].Encode()
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint64(scratchIntervalBytes[0:sizeOfInt64], uint64(nanos))
			binary.LittleEndian.PutUint64(scratchIntervalBytes[sizeOfInt64:sizeOfInt64*2], uint64(months))
			binary.LittleEndian.PutUint64(scratchIntervalBytes[sizeOfInt64*2:sizeOfInt64*3], uint64(days))
			c.builders.binaryBuilder.Append(scratchIntervalBytes)
		}
		return c.builders.binaryBuilder.NewBinaryArray().Data(), nil

	case typeconv.DatumVecCanonicalTypeFamily:
		datums := vec.Datum().Slice(0 /* start */, n)
		for i := 0; i < n; i++ {
			if nulls != nil && nulls.NullAt(i) {
				c.builders.binaryBuilder.AppendNull()
				continue
			}
			marshaled, err := datums.MarshalAt(i)
			if err != nil {
				return nil, err
			}
			c.builders.binaryBuilder.Append(marshaled)
		}
		return c.builders.binaryBuilder.NewBinaryArray().Data(), nil
	}

	return nil, nil
}

// ArrowToBatch converts []*array.Data to a coldata.Batch. There must not be
// more than coldata.BatchSize() elements in data. It's safe to call ArrowToBatch
// concurrently.
//
// The passed in batch is overwritten, but after this method returns it stays
// valid as long as `data` stays valid. Callers can use this to control the
// lifetimes of the batches, saving allocations when they can be reused (i.e.
// reused by passing them back into this function).
//
// The passed in data is also mutated (we store nulls differently than arrow and
// the adjustment is done in place).
func (c *ArrowBatchConverter) ArrowToBatch(
	data []*array.Data, batchLength int, b coldata.Batch,
) error {
	if len(data) != len(c.typs) {
		return errors.Errorf("mismatched data and schema length: %d != %d", len(data), len(c.typs))
	}

	for i, typ := range c.typs {
		vec := b.ColVec(i)
		d := data[i]

		// Eagerly release our data references to make sure they can be collected
		// as quickly as possible as we copy each (or simply reference each) by
		// coldata.Vecs below.
		data[i] = nil

		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BoolFamily:
			boolArr := array.NewBooleanData(d)
			handleNulls(boolArr, vec, batchLength)
			vecArr := vec.Bool()
			for i := 0; i < boolArr.Len(); i++ {
				vecArr[i] = boolArr.Value(i)
			}

		case types.BytesFamily:
			bytesArr := array.NewBinaryData(d)
			handleNulls(bytesArr, vec, batchLength)
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			coldata.BytesFromArrowSerializationFormat(vec.Bytes(), bytes, bytesArr.ValueOffsets())

		case types.DecimalFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
			handleNulls(bytesArr, vec, batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Decimal()
			for i := 0; i < len(offsets)-1; i++ {
				if nulls == nil || !nulls.NullAt(i) {
					if err := vecArr[i].UnmarshalText(bytes[offsets[i]:offsets[i+1]]); err != nil {
						return err
					}
				}
			}

		case types.TimestampTZFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
			handleNulls(bytesArr, vec, batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Timestamp()
			for i := 0; i < len(offsets)-1; i++ {
				if nulls == nil || !nulls.NullAt(i) {
					if err := vecArr[i].UnmarshalBinary(bytes[offsets[i]:offsets[i+1]]); err != nil {
						return err
					}
				}
			}

		case types.IntervalFamily:
			// TODO(asubiotto): this serialization is quite inefficient compared to
			//  the direct casts below. Improve it.
			bytesArr := array.NewBinaryData(d)
			handleNulls(bytesArr, vec, batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Interval()
			for i := 0; i < len(offsets)-1; i++ {
				if nulls == nil || !nulls.NullAt(i) {
					intervalBytes := bytes[offsets[i]:offsets[i+1]]
					var err error
					vecArr[i], err = duration.Decode(
						int64(binary.LittleEndian.Uint64(intervalBytes[0:sizeOfInt64])),
						int64(binary.LittleEndian.Uint64(intervalBytes[sizeOfInt64:sizeOfInt64*2])),
						int64(binary.LittleEndian.Uint64(intervalBytes[sizeOfInt64*2:sizeOfInt64*3])),
					)
					if err != nil {
						return err
					}
				}
			}

		case typeconv.DatumVecCanonicalTypeFamily:
			bytesArr := array.NewBinaryData(d)
			handleNulls(bytesArr, vec, batchLength)
			// We need to be paying attention to nulls values so that we don't
			// try to unmarshal invalid values.
			var nulls *coldata.Nulls
			if vec.MaybeHasNulls() {
				nulls = vec.Nulls()
			}
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Datum()
			for i := 0; i < len(offsets)-1; i++ {
				if nulls == nil || !nulls.NullAt(i) {
					if err := vecArr.UnmarshalTo(i, bytes[offsets[i]:offsets[i+1]]); err != nil {
						return err
					}
				}
			}

		default:
			var col interface{}
			switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
			case types.IntFamily:
				switch typ.Width() {
				case 16:
					intArr := array.NewInt16Data(d)
					handleNulls(intArr, vec, batchLength)
					col = coldata.Int16s(intArr.Int16Values())
				case 32:
					intArr := array.NewInt32Data(d)
					handleNulls(intArr, vec, batchLength)
					col = coldata.Int32s(intArr.Int32Values())
				case 0, 64:
					intArr := array.NewInt64Data(d)
					handleNulls(intArr, vec, batchLength)
					col = coldata.Int64s(intArr.Int64Values())
				default:
					panic(fmt.Sprintf("unexpected int width: %d", typ.Width()))
				}
			case types.FloatFamily:
				floatArr := array.NewFloat64Data(d)
				handleNulls(floatArr, vec, batchLength)
				col = coldata.Float64s(floatArr.Float64Values())
			default:
				panic(
					fmt.Sprintf("unsupported type for conversion to column batch %s", d.DataType().Name()),
				)
			}
			vec.SetCol(col)
		}
	}
	b.SetSelection(false)
	b.SetLength(batchLength)
	return nil
}

// handleNulls sets the correct nulls bitmap on vec according to arr.
func handleNulls(arr array.Interface, vec coldata.Vec, batchLength int) {
	arrowBitmap := arr.NullBitmapBytes()
	if len(arrowBitmap) != 0 {
		vec.Nulls().SetNullBitmap(arrowBitmap, batchLength)
	} else {
		// For types with the canonical type family of Bool, Bytes, Int, or
		// Float, when there are no nulls, we have a null bitmap with zero
		// length.
		if vec.Nulls().MaxNumElements() < batchLength {
			// The current null bitmap doesn't have enough space, so we need to
			// allocate a new one.
			//
			// Note that this has likely occurred because on the previous batch
			// there were some nulls and we replaced the null bitmap with the
			// arrowBitmap which happened to be of insufficient capacity for the
			// current batch.
			nulls := coldata.NewNulls(batchLength)
			vec.SetNulls(&nulls)
		} else {
			vec.Nulls().UnsetNulls()
		}
	}
}
