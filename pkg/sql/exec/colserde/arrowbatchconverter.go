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
	"fmt"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/errors"
)

// ArrowBatchConverter converts batches to arrow column data
// ([]*array.Data) and back again.
type ArrowBatchConverter struct {
	typs []types.T

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
func NewArrowBatchConverter(typs []types.T) (*ArrowBatchConverter, error) {
	for _, t := range typs {
		if _, supported := supportedTypes[t]; !supported {
			return nil, errors.Errorf("unsupported type %v", t.String())
		}
	}
	c := &ArrowBatchConverter{typs: typs}
	c.builders.boolBuilder = array.NewBooleanBuilder(memory.DefaultAllocator)
	c.builders.binaryBuilder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	c.scratch.arrowData = make([]*array.Data, len(typs))
	c.scratch.buffers = make([][]*memory.Buffer, len(typs))
	for i := range c.scratch.buffers {
		// Only primitive types are directly constructed, therefore we only need
		// two buffers: one for the nulls, the other for the values.
		c.scratch.buffers[i] = make([]*memory.Buffer, 2)
	}
	return c, nil
}

const (
	sizeOfInt8    = int(unsafe.Sizeof(int8(0)))
	sizeOfInt16   = int(unsafe.Sizeof(int16(0)))
	sizeOfInt32   = int(unsafe.Sizeof(int32(0)))
	sizeOfInt64   = int(unsafe.Sizeof(int64(0)))
	sizeOfFloat32 = int(unsafe.Sizeof(float32(0)))
	sizeOfFloat64 = int(unsafe.Sizeof(float64(0)))
)

var supportedTypes = func() map[types.T]struct{} {
	typs := make(map[types.T]struct{})
	for _, t := range []types.T{
		types.Bool,
		types.Bytes,
		types.Int8,
		types.Int16,
		types.Int32,
		types.Int64,
		types.Float32,
		types.Float64,
	} {
		typs[t] = struct{}{}
	}
	return typs
}()

// BatchToArrow converts the first batch.Length elements of the batch into an
// arrow []*array.Data. It is assumed that the batch is not larger than
// coldata.BatchSize. The returned []*array.Data may only be used until the
// next call to BatchToArrow.
func (c *ArrowBatchConverter) BatchToArrow(batch coldata.Batch) ([]*array.Data, error) {
	if batch.Width() != len(c.typs) {
		return nil, errors.AssertionFailedf("mismatched batch width and schema length: %d != %d", batch.Width(), len(c.typs))
	}
	n := int(batch.Length())
	for i, typ := range c.typs {
		vec := batch.ColVec(i)

		var arrowBitmap []byte
		if vec.MaybeHasNulls() {
			n := vec.Nulls()
			// To conform to the Arrow spec, zero out all trailing null values.
			n.Truncate(batch.Length())
			arrowBitmap = n.NullBitmap()
		}

		if typ == types.Bool || typ == types.Bytes {
			// Bools and Bytes are handled differently from other types. Refer to the
			// comment on ArrowBatchConverter.builders for more information.
			var data *array.Data
			switch typ {
			case types.Bool:
				c.builders.boolBuilder.AppendValues(vec.Bool()[:n], nil /* valid */)
				data = c.builders.boolBuilder.NewBooleanArray().Data()
			case types.Bytes:
				c.builders.binaryBuilder.AppendValues(vec.Bytes()[:n], nil /* valid */)
				data = c.builders.binaryBuilder.NewBinaryArray().Data()
			default:
				panic(fmt.Sprintf("unexpected type %s", typ))
			}
			if arrowBitmap != nil {
				// Overwrite empty null bitmap with the true bitmap.
				data.Buffers()[0] = memory.NewBufferBytes(arrowBitmap)
			}
			c.scratch.arrowData[i] = data
			continue
		}

		var (
			values []byte
			// dataHeader is the reflect.SliceHeader of the coldata.Vec's underlying
			// data slice that we are casting to bytes.
			dataHeader *reflect.SliceHeader
			// datumSize is the size of one datum that we are casting to a byte slice.
			datumSize int
		)

		switch typ {
		case types.Int8:
			ints := vec.Int8()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt8
		case types.Int16:
			ints := vec.Int16()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt16
		case types.Int32:
			ints := vec.Int32()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt32
		case types.Int64:
			ints := vec.Int64()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt64
		case types.Float32:
			floats := vec.Float32()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&floats))
			datumSize = sizeOfFloat32
		case types.Float64:
			floats := vec.Float64()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&floats))
			datumSize = sizeOfFloat64
		default:
			panic(fmt.Sprintf("unsupported type for conversion to arrow data %s", typ))
		}

		// Cast values.
		valuesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&values))
		valuesHeader.Data = dataHeader.Data
		valuesHeader.Len = dataHeader.Len * datumSize
		valuesHeader.Cap = dataHeader.Cap * datumSize

		// Construct the underlying arrow buffers.
		// WARNING: The ordering of construction is critical.
		c.scratch.buffers[i][0] = memory.NewBufferBytes(arrowBitmap)
		c.scratch.buffers[i][1] = memory.NewBufferBytes(values)

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
// more than coldata.BatchSize elements in data. It's safe to call ArrowToBatch
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
	// Reset reuses the passed-in Batch when possible, saving allocations but
	// overwriting it. If the passed-in Batch is not suitable for use, a new one
	// is allocated.
	b.Reset(c.typs, n)
	b.SetLength(uint16(n))
	// No selection, all values are valid.
	b.SetSelection(false)

	for i, typ := range c.typs {
		vec := b.ColVec(i)
		d := data[i]

		var arr array.Interface
		if typ == types.Bool || typ == types.Bytes {
			switch typ {
			case types.Bool:
				boolArr := array.NewBooleanData(d)
				vecArr := vec.Bool()
				for i := 0; i < boolArr.Len(); i++ {
					vecArr[i] = boolArr.Value(i)
				}
				arr = boolArr
			case types.Bytes:
				bytesArr := array.NewBinaryData(d)
				bytes := bytesArr.ValueBytes()
				if bytes == nil {
					// All bytes values are empty, so the representation is solely with the
					// offsets slice, so create an empty slice so that the conversion
					// corresponds.
					bytes = make([]byte, 0)
				}
				offsets := bytesArr.ValueOffsets()
				vecArr := vec.Bytes()
				for i := 0; i < len(offsets)-1; i++ {
					vecArr[i] = bytes[offsets[i]:offsets[i+1]]
				}
				arr = bytesArr
			default:
				panic(fmt.Sprintf("unexpected type %s", typ))
			}
		} else {
			var col interface{}
			switch typ {
			case types.Int8:
				intArr := array.NewInt8Data(d)
				col = intArr.Int8Values()
				arr = intArr
			case types.Int16:
				intArr := array.NewInt16Data(d)
				col = intArr.Int16Values()
				arr = intArr
			case types.Int32:
				intArr := array.NewInt32Data(d)
				col = intArr.Int32Values()
				arr = intArr
			case types.Int64:
				intArr := array.NewInt64Data(d)
				col = intArr.Int64Values()
				arr = intArr
			case types.Float32:
				floatArr := array.NewFloat32Data(d)
				col = floatArr.Float32Values()
				arr = floatArr
			case types.Float64:
				floatArr := array.NewFloat64Data(d)
				col = floatArr.Float64Values()
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
		}
	}
	return nil
}
