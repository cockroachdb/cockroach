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
	for i, typ := range c.typs {
		vec := batch.ColVec(i)
		canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family())

		var arrowBitmap []byte
		if vec.MaybeHasNulls() {
			n := vec.Nulls()
			// To conform to the Arrow spec, zero out all trailing null values.
			n.Truncate(batch.Length())
			arrowBitmap = n.NullBitmap()
		}

		var data *array.Data
		switch canonicalTypeFamily {
		case types.BoolFamily:
			c.builders.boolBuilder.AppendValues(vec.Bool()[:n], nil /* valid */)
			data = c.builders.boolBuilder.NewBooleanArray().Data()
		case types.DecimalFamily:
			decimals := vec.Decimal()[:n]
			for _, d := range decimals {
				marshaled, err := d.MarshalText()
				if err != nil {
					return nil, err
				}
				c.builders.binaryBuilder.Append(marshaled)
			}
			data = c.builders.binaryBuilder.NewBinaryArray().Data()
		case types.TimestampTZFamily:
			timestamps := vec.Timestamp()[:n]
			for _, ts := range timestamps {
				marshaled, err := ts.MarshalBinary()
				if err != nil {
					return nil, err
				}
				c.builders.binaryBuilder.Append(marshaled)
			}
			data = c.builders.binaryBuilder.NewBinaryArray().Data()
		case types.IntervalFamily:
			intervals := vec.Interval()[:n]
			// Appending to the binary builder will copy the bytes, so it's safe to
			// reuse a scratch bytes to encode the interval into.
			scratchIntervalBytes := make([]byte, sizeOfInt64*3)
			for _, interval := range intervals {
				nanos, months, days, err := interval.Encode()
				if err != nil {
					return nil, err
				}
				binary.LittleEndian.PutUint64(scratchIntervalBytes[0:sizeOfInt64], uint64(nanos))
				binary.LittleEndian.PutUint64(scratchIntervalBytes[sizeOfInt64:sizeOfInt64*2], uint64(months))
				binary.LittleEndian.PutUint64(scratchIntervalBytes[sizeOfInt64*2:sizeOfInt64*3], uint64(days))
				c.builders.binaryBuilder.Append(scratchIntervalBytes)
			}
			data = c.builders.binaryBuilder.NewBinaryArray().Data()
		case typeconv.DatumVecCanonicalTypeFamily:
			datums := vec.Datum().Slice(0 /* start */, n)
			for idx := 0; idx < n; idx++ {
				b, err := datums.MarshalAt(idx)
				if err != nil {
					return nil, err
				}
				c.builders.binaryBuilder.Append(b)
			}
			data = c.builders.binaryBuilder.NewBinaryArray().Data()
		}
		if data != nil {
			if arrowBitmap != nil {
				// Overwrite empty null bitmap with the true bitmap.
				data.Buffers()[0] = memory.NewBufferBytes(arrowBitmap)
			}
			c.scratch.arrowData[i] = data
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

		switch canonicalTypeFamily {
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

		// Construct the underlying arrow buffers.
		// WARNING: The ordering of construction is critical.
		c.scratch.buffers[i] = c.scratch.buffers[i][:0]
		c.scratch.buffers[i] = append(c.scratch.buffers[i], memory.NewBufferBytes(arrowBitmap))
		if offsets != nil {
			c.scratch.buffers[i] = append(c.scratch.buffers[i], memory.NewBufferBytes(offsets))
		}
		c.scratch.buffers[i] = append(c.scratch.buffers[i], memory.NewBufferBytes(values))

		// Create the data from the buffers. It might be surprising that we don't
		// set a type or a null count, but these fields are not used in the way that
		// we're using array.Data. The part we care about are the buffers. Type
		// information is inferred from the ArrowBatchConverter schema, null count
		// is an optimization we can use when working with nulls, and childData is
		// only used for nested types like Lists, Structs, or Unions.
		c.scratch.arrowData[i] = array.NewData(
			nil /* dtype */, n, c.scratch.buffers[i], nil /* childData */, 0 /* nulls */, 0, /* offset */
		)
	}
	return c.scratch.arrowData, nil
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
func (c *ArrowBatchConverter) ArrowToBatch(data []*array.Data, b coldata.Batch) error {
	if len(data) != len(c.typs) {
		return errors.Errorf("mismatched data and schema length: %d != %d", len(data), len(c.typs))
	}
	// Assume > 0 length data.
	n := data[0].Len()

	for i, typ := range c.typs {
		vec := b.ColVec(i)
		d := data[i]

		var arr array.Interface
		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BoolFamily:
			boolArr := array.NewBooleanData(d)
			vecArr := vec.Bool()
			for i := 0; i < boolArr.Len(); i++ {
				vecArr[i] = boolArr.Value(i)
			}
			arr = boolArr
		case types.BytesFamily:
			bytesArr := array.NewBinaryData(d)
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			coldata.BytesFromArrowSerializationFormat(vec.Bytes(), bytes, bytesArr.ValueOffsets())
			arr = bytesArr
		case types.DecimalFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
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
				if err := vecArr[i].UnmarshalText(bytes[offsets[i]:offsets[i+1]]); err != nil {
					return err
				}
			}
			arr = bytesArr
		case types.TimestampTZFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
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
				if err := vecArr[i].UnmarshalBinary(bytes[offsets[i]:offsets[i+1]]); err != nil {
					return err
				}
			}
			arr = bytesArr
		case types.IntervalFamily:
			// TODO(asubiotto): this serialization is quite inefficient compared to
			//  the direct casts below. Improve it.
			bytesArr := array.NewBinaryData(d)
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
			arr = bytesArr
		case typeconv.DatumVecCanonicalTypeFamily:
			bytesArr := array.NewBinaryData(d)
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
				err := vecArr.UnmarshalTo(i, bytes[offsets[i]:offsets[i+1]])
				if err != nil {
					return err
				}
			}
			arr = bytesArr
		default:
			var col interface{}
			switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
			case types.IntFamily:
				switch typ.Width() {
				case 16:
					intArr := array.NewInt16Data(d)
					col = coldata.Int16s(intArr.Int16Values())
					arr = intArr
				case 32:
					intArr := array.NewInt32Data(d)
					col = coldata.Int32s(intArr.Int32Values())
					arr = intArr
				case 0, 64:
					intArr := array.NewInt64Data(d)
					col = coldata.Int64s(intArr.Int64Values())
					arr = intArr
				default:
					panic(fmt.Sprintf("unexpected int width: %d", typ.Width()))
				}
			case types.FloatFamily:
				floatArr := array.NewFloat64Data(d)
				col = coldata.Float64s(floatArr.Float64Values())
				arr = floatArr
			default:
				panic(
					fmt.Sprintf("unsupported type for conversion to column batch %s", d.DataType().Name()),
				)
			}
			vec.SetCol(col)
		}
		arrowBitmap := arr.NullBitmapBytes()
		if len(arrowBitmap) != 0 {
			vec.Nulls().SetNullBitmap(arrowBitmap, n)
		} else {
			vec.Nulls().UnsetNulls()
		}
		b.SetSelection(false)
	}
	b.SetLength(n)
	return nil
}
