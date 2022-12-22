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

// ArrowBatchConverter converts batches to arrow column data
// ([]array.Data) and back again.
type ArrowBatchConverter struct {
	typs []*types.T

	// builders are the set of builders that need to be kept around in order to
	// construct arrow representations of certain types when they cannot be simply
	// cast from our current data representation.
	builders struct {
		// boolBuilder builds arrow bool columns as a bitmap from a bool slice.
		boolBuilder *array.BooleanBuilder
	}

	scratch struct {
		// arrowData is used as scratch space returned as the corresponding
		// conversion result.
		arrowData []array.Data
		// buffers is scratch space for two or three buffers per element in
		// arrowData.
		buffers [][]*memory.Buffer
	}
}

// NewArrowBatchConverter converts coldata.Batches to []array.Data and back
// again according to the schema specified by typs. Converting data that does
// not conform to typs results in undefined behavior.
func NewArrowBatchConverter(typs []*types.T) (*ArrowBatchConverter, error) {
	c := &ArrowBatchConverter{typs: typs}
	c.builders.boolBuilder = array.NewBooleanBuilder(memory.DefaultAllocator)
	c.scratch.arrowData = make([]array.Data, len(typs))
	c.scratch.buffers = make([][]*memory.Buffer, len(typs))
	// Calculate the number of buffers needed for all types to be able to batch
	// allocate them below.
	var numBuffersTotal int
	for _, t := range typs {
		numBuffersTotal += numBuffersForType(t)
	}
	buffers := make([]memory.Buffer, numBuffersTotal)
	var buffersIdx int
	for i, t := range typs {
		c.scratch.buffers[i] = make([]*memory.Buffer, numBuffersForType(t))
		for j := range c.scratch.buffers[i] {
			c.scratch.buffers[i][j] = &buffers[buffersIdx]
			buffersIdx++
		}
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

		// Bools require special handling.
		if typ.Family() == types.BoolFamily {
			c.builders.boolBuilder.AppendValues(vec.Bool()[:n], nil /* valid */)
			c.scratch.arrowData[vecIdx] = *c.builders.boolBuilder.NewBooleanArray().Data()
			// Overwrite incorrect null bitmap (that has all values as "valid")
			// with the actual null bitmap. Note that if we actually don't have
			// any nulls, we use a bitmap with zero length for it in order to
			// reduce the size of serialized representation.
			var arrowBitmap []byte
			if nulls != nil {
				arrowBitmap = nulls.NullBitmap()
			}
			c.scratch.arrowData[vecIdx].Buffers()[0].Reset(arrowBitmap)
			continue
		}

		var (
			values       []byte
			offsetsBytes []byte
		)

		if f := typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()); f == types.IntFamily || f == types.FloatFamily {
			// For ints and floats we have a fast path where we cast the
			// underlying slice directly to the arrow format.
			//
			// dataHeader is the reflect.SliceHeader of the coldata.Vec's underlying
			// data slice that we are casting to bytes.
			var dataHeader *reflect.SliceHeader
			// datumSize is the size of one datum that we are casting to a byte slice.
			var datumSize int64
			if f == types.IntFamily {
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
			} else {
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
			var offsets []int32
			switch f {
			case types.BytesFamily:
				values, offsets = vec.Bytes().ToArrowSerializationFormat(n)
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case types.JsonFamily:
				values, offsets = vec.JSON().Bytes.ToArrowSerializationFormat(n)
				unsafeCastOffsetsArray(offsets, &offsetsBytes)

			case types.DecimalFamily:
				offsets = make([]int32, 0, n+1)
				decimals := vec.Decimal()[:n]
				// We don't know exactly how big a decimal will serialize to. Let's
				// estimate 16 bytes.
				values = make([]byte, 0, n*16)
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
				offsets = make([]int32, 0, n+1)
				intervals := vec.Interval()[:n]
				intervalSize := int(memsize.Int64) * 3
				// TODO(jordan): we could right-size this values slice by counting up the
				// number of nulls in the nulls bitmap first, and subtracting from n.
				values = make([]byte, intervalSize*n)
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
				offsets = make([]int32, 0, n+1)
				timestamps := vec.Timestamp()[:n]
				// See implementation of time.MarshalBinary.
				const timestampSize = 14
				values = make([]byte, 0, n*timestampSize)
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
				offsets = make([]int32, 0, n+1)
				datums := vec.Datum().Window(0 /* start */, n)
				// Make a very very rough estimate of the number of bytes we'll have to
				// allocate for the datums in this vector. This will likely be an
				// undercount, but the estimate is better than nothing.
				size, _ := tree.DatumTypeSize(typ)
				values = make([]byte, 0, n*int(size))
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
		case types.BoolFamily:
			boolArr := array.NewBooleanData(d)
			vec.Nulls().SetNullBitmap(boolArr.NullBitmapBytes(), batchLength)
			vecArr := vec.Bool()
			for j := 0; j < boolArr.Len(); j++ {
				vecArr[j] = boolArr.Value(j)
			}

		case types.BytesFamily:
			deserializeArrowIntoBytes(d, vec.Nulls(), vec.Bytes(), batchLength)

		case types.JsonFamily:
			deserializeArrowIntoBytes(d, vec.Nulls(), &vec.JSON().Bytes, batchLength)

		case types.DecimalFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
			vec.Nulls().SetNullBitmap(bytesArr.NullBitmapBytes(), batchLength)
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
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					if err := vecArr[j].UnmarshalText(bytes[offsets[j]:offsets[j+1]]); err != nil {
						return err
					}
				}
			}

		case types.TimestampTZFamily:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
			vec.Nulls().SetNullBitmap(bytesArr.NullBitmapBytes(), batchLength)
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
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					if err := vecArr[j].UnmarshalBinary(bytes[offsets[j]:offsets[j+1]]); err != nil {
						return err
					}
				}
			}

		case types.IntervalFamily:
			// TODO(asubiotto): this serialization is quite inefficient compared to
			//  the direct casts below. Improve it.
			bytesArr := array.NewBinaryData(d)
			vec.Nulls().SetNullBitmap(bytesArr.NullBitmapBytes(), batchLength)
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
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					intervalBytes := bytes[offsets[j]:offsets[j+1]]
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
			bytesArr := array.NewBinaryData(d)
			vec.Nulls().SetNullBitmap(bytesArr.NullBitmapBytes(), batchLength)
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
			for j := 0; j < len(offsets)-1; j++ {
				if nulls == nil || !nulls.NullAt(j) {
					if err := vecArr.UnmarshalTo(j, bytes[offsets[j]:offsets[j+1]]); err != nil {
						return err
					}
				}
			}

		default:
			// For integers and floats we can just directly cast the slice
			// without performing the copy.
			//
			// However, we have to be careful to set the capacity on each slice
			// explicitly to protect memory regions that come after the slice
			// from corruption in case the slice will be appended to in the
			// future. See an example in deserializeArrowIntoBytes.
			var col coldata.Column
			switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
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

// deserializeArrowIntoBytes deserializes some arrow data into the given
// Bytes structure, adding any nulls to the given null bitmap.
func deserializeArrowIntoBytes(
	d *array.Data, nulls *coldata.Nulls, bytes *coldata.Bytes, batchLength int,
) {
	bytesArr := array.NewBinaryData(d)
	nulls.SetNullBitmap(bytesArr.NullBitmapBytes(), batchLength)
	b := bytesArr.ValueBytes()
	if b == nil {
		// All bytes values are empty, so the representation is solely with the
		// offsets slice, so create an empty slice so that the conversion
		// corresponds.
		b = make([]byte, 0)
	}
	// Cap the data and offsets slices explicitly to protect against possible
	// corruption of the memory region that is after the arrow data for this
	// Bytes vector.
	//
	// Consider the following scenario: a batch with two Bytes vectors is
	// serialized. Say
	// - the first vector is {data:[foo], offsets:[0, 3]}
	// - the second vector is {data:[bar], offsets:[0, 3]}.
	// After serializing both of them we will have a flat buffer with something
	// like:
	//   buf = {1foo031bar03} (ones represent the lengths of each vector).
	// Now, when the first vector is being deserialized, it's data slice will be
	// something like:
	//   data = [foo031bar03], len(data) = 3, cap(data) > 3.
	// If we don't explicitly cap the slice and deserialize it into a Bytes
	// vector, then later when we append to that vector, we will overwrite the
	// data that is actually a part of the second serialized vector, thus,
	// corrupting it (or the next batch).
	offsets := bytesArr.ValueOffsets()
	b = b[:len(b):len(b)]
	offsets = offsets[:len(offsets):len(offsets)]
	coldata.BytesFromArrowSerializationFormat(bytes, b, offsets)
}
