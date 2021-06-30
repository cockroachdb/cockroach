// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colserde_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// randomDataFromType creates an *array.Data of length n and type t, filling it
// with random values and inserting nulls with probability nullProbability.
func randomDataFromType(rng *rand.Rand, t *types.T, n int, nullProbability float64) *array.Data {
	if nullProbability < 0 || nullProbability > 1 {
		panic(fmt.Sprintf("expected a value between 0 and 1 for nullProbability but got %f", nullProbability))
	}
	const (
		// maxVarLen is the maximum length we allow variable length datatypes (e.g.
		// strings) to be.
		maxVarLen = 1024
		charset   = "㪊㪋㪌㪍㪎𢽙啟敍敎敏敚敐救敒敓敔敕敖敗敘教敏敖abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ😈💜╯‵Д′)╯彡┻━┻"
	)
	// valid represents the null bitmap.
	valid := make([]bool, n)
	for i := range valid {
		if rng.Float64() >= nullProbability {
			valid[i] = true
		}
	}

	var builder array.Builder
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	case types.BoolFamily:
		builder = array.NewBooleanBuilder(memory.DefaultAllocator)
		data := make([]bool, n)
		for i := range data {
			if rng.Float64() < 0.5 {
				data[i] = true
			}
		}
		builder.(*array.BooleanBuilder).AppendValues(data, valid)
	case types.IntFamily:
		switch t.Width() {
		case 16:
			builder = array.NewInt16Builder(memory.DefaultAllocator)
			data := make([]int16, n)
			for i := range data {
				data[i] = int16(rng.Uint64())
			}
			builder.(*array.Int16Builder).AppendValues(data, valid)
		case 32:
			builder = array.NewInt32Builder(memory.DefaultAllocator)
			data := make([]int32, n)
			for i := range data {
				data[i] = int32(rng.Uint64())
			}
			builder.(*array.Int32Builder).AppendValues(data, valid)
		case 0, 64:
			builder = array.NewInt64Builder(memory.DefaultAllocator)
			data := make([]int64, n)
			for i := range data {
				data[i] = int64(rng.Uint64())
			}
			builder.(*array.Int64Builder).AppendValues(data, valid)
		default:
			panic(fmt.Sprintf("unexpected int width: %d", t.Width()))
		}
	case types.FloatFamily:
		builder = array.NewFloat64Builder(memory.DefaultAllocator)
		data := make([]float64, n)
		for i := range data {
			data[i] = rng.Float64() * math.MaxFloat64
		}
		builder.(*array.Float64Builder).AppendValues(data, valid)
	case types.BytesFamily:
		// Bytes can be represented 3 different ways. As variable-length bytes,
		// variable-length strings, or fixed-width bytes.
		representation := rng.Intn(2)
		switch representation {
		case 0:
			builder = array.NewStringBuilder(memory.DefaultAllocator)
			data := make([]string, n)
			stringBuilder := &strings.Builder{}
			for i := range data {
				stringBuilder.Reset()
				if valid[i] {
					for j := 0; j < rng.Intn(maxVarLen)+1; j++ {
						stringBuilder.WriteRune(rune(charset[rng.Intn(len(charset))]))
					}
				}
				data[i] = stringBuilder.String()
			}
			builder.(*array.StringBuilder).AppendValues(data, valid)
		case 1:
			builder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			data := make([][]byte, n)
			for i := range data {
				slice := make([]byte, rng.Intn(maxVarLen))
				if valid[i] {
					// Read always returns len(slice) and nil error.
					_, _ = rng.Read(slice)
				}
				data[i] = slice
			}
			builder.(*array.BinaryBuilder).AppendValues(data, valid)
		case 2:
			// NOTE: We currently do not generate fixed-width bytes in this test due to
			// the different buffer layout (no offsets). The serialization code assumes
			// 3 buffers for all types.BytesFamily types.
			/*
				width := rng.Intn(maxVarLen) + 1
				  builder = array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: width})
				  data := make([][]byte, n)
				  for i := range data {
				  	slice := make([]byte, width)
				  	if valid[i] {
				  		_, _ = rng.Read(slice)
				  	}
				  	data[i] = slice
				  }
				  builder.(*array.FixedSizeBinaryBuilder).AppendValues(data, valid)
			*/
		}
	case types.DecimalFamily:
		var err error
		builder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		data := make([][]byte, n)
		for i := range data {
			var d apd.Decimal
			// int64(rng.Uint64()) to get negative numbers, too.
			d.SetFinite(int64(rng.Uint64()), int32(rng.Intn(40)-20))
			data[i], err = d.MarshalText()
			if err != nil {
				panic(err)
			}
		}
		builder.(*array.BinaryBuilder).AppendValues(data, valid)
	case types.TimestampTZFamily:
		var err error
		now := timeutil.Now()
		builder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		data := make([][]byte, n)
		for i := range data {
			delta := rng.Int63()
			ts := now.Add(time.Duration(delta))
			data[i], err = ts.MarshalBinary()
			if err != nil {
				panic(err)
			}
		}
		builder.(*array.BinaryBuilder).AppendValues(data, valid)
	case types.IntervalFamily:
		builder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		data := make([][]byte, n)
		sizeOfInt64 := int(unsafe.Sizeof(int64(0)))
		for i := range data {
			data[i] = make([]byte, sizeOfInt64*3)
			binary.LittleEndian.PutUint64(data[i][0:sizeOfInt64], rng.Uint64())
			binary.LittleEndian.PutUint64(data[i][sizeOfInt64:sizeOfInt64*2], rng.Uint64())
			binary.LittleEndian.PutUint64(data[i][sizeOfInt64*2:sizeOfInt64*3], rng.Uint64())
		}
		builder.(*array.BinaryBuilder).AppendValues(data, valid)
	case types.JsonFamily:
		builder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		data := make([][]byte, n)
		for i := range data {
			j, err := json.Random(20, rng)
			if err != nil {
				panic(err)
			}
			bytes, err := json.EncodeJSON(nil, j)
			if err != nil {
				panic(err)
			}
			data[i] = bytes
		}
		builder.(*array.BinaryBuilder).AppendValues(data, valid)
	case typeconv.DatumVecCanonicalTypeFamily:
		builder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		data := make([][]byte, n)
		var (
			scratch []byte
			err     error
		)
		for i := range data {
			d := randgen.RandDatum(rng, t, false /* nullOk */)
			data[i], err = rowenc.EncodeTableValue(data[i], descpb.ColumnID(encoding.NoColumnID), d, scratch)
			if err != nil {
				panic(err)
			}
		}
		builder.(*array.BinaryBuilder).AppendValues(data, valid)
	default:
		panic(fmt.Sprintf("unsupported type %s", t))
	}
	return builder.NewArray().Data()
}

func TestRecordBatchSerializer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Serializing and Deserializing an invalid schema is undefined.

	t.Run("SerializeDifferentColumnLengths", func(t *testing.T) {
		s, err := colserde.NewRecordBatchSerializer([]*types.T{types.Int, types.Int})
		require.NoError(t, err)
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.AppendValues([]int64{1, 2}, nil /* valid */)
		firstCol := b.NewArray().Data()
		b.AppendValues([]int64{3}, nil /* valid */)
		secondCol := b.NewArray().Data()
		_, _, err = s.Serialize(&bytes.Buffer{}, []*array.Data{firstCol, secondCol}, firstCol.Len())
		require.True(t, testutils.IsError(err, "mismatched data lengths"), err)
	})
}

func TestRecordBatchSerializerSerializeDeserializeRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	const (
		maxTypes   = 16
		maxDataLen = 2048
	)

	var (
		typs            = make([]*types.T, rng.Intn(maxTypes)+1)
		data            = make([]*array.Data, len(typs))
		dataLen         = rng.Intn(maxDataLen) + 1
		nullProbability = rng.Float64()
		buf             = bytes.Buffer{}
	)

	for i := range typs {
		typs[i] = randgen.RandType(rng)
		data[i] = randomDataFromType(rng, typs[i], dataLen, nullProbability)
	}

	s, err := colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		t.Fatal(err)
	}

	// Run Serialize/Deserialize in a loop to test reuse.
	for i := 0; i < 2; i++ {
		buf.Reset()
		dataCopy := append([]*array.Data{}, data...)
		_, _, err := s.Serialize(&buf, dataCopy, dataLen)
		require.NoError(t, err)
		if buf.Len()%8 != 0 {
			t.Fatal("message length must align to 8 byte boundary")
		}
		var deserializedData []*array.Data
		_, err = s.Deserialize(&deserializedData, buf.Bytes())
		require.NoError(t, err)

		// Check the fields we care most about. We can't use require.Equal directly
		// due to some unimportant differences (e.g. mutability of underlying
		// buffers).
		require.Equal(t, len(data), len(deserializedData))
		for i := range data {
			require.Equal(t, data[i].Len(), deserializedData[i].Len())
			require.Equal(t, len(data[i].Buffers()), len(deserializedData[i].Buffers()))
			require.Equal(t, data[i].NullN(), deserializedData[i].NullN())
			require.Equal(t, data[i].Offset(), deserializedData[i].Offset())
			decBuffers := deserializedData[i].Buffers()
			for j, buf := range data[i].Buffers() {
				if buf == nil {
					if decBuffers[j].Len() != 0 {
						t.Fatal("expected zero length serialization of nil buffer")
					}
					continue
				}
				require.Equal(t, buf.Len(), decBuffers[j].Len())
				require.Equal(t, buf.Bytes(), decBuffers[j].Bytes())
			}
		}
	}
}

func TestRecordBatchSerializerDeserializeMemoryEstimate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var err error
	rng, _ := randutil.NewPseudoRand()

	typs := []*types.T{types.Bytes}
	src := testAllocator.NewMemBatchWithMaxCapacity(typs)
	dest := testAllocator.NewMemBatchWithMaxCapacity(typs)
	bytesVec := src.ColVec(0).Bytes()
	maxValueLen := coldata.BytesInitialAllocationFactor * 8
	value := make([]byte, maxValueLen)
	for i := 0; i < coldata.BatchSize(); i++ {
		value = value[:rng.Intn(maxValueLen)]
		_, err = rng.Read(value)
		require.NoError(t, err)
		bytesVec.Set(i, value)
	}
	src.SetLength(coldata.BatchSize())

	c, err := colserde.NewArrowBatchConverter(typs)
	require.NoError(t, err)
	r, err := colserde.NewRecordBatchSerializer(typs)
	require.NoError(t, err)
	require.NoError(t, roundTripBatch(src, dest, c, r))

	originalMemorySize := colmem.GetBatchMemSize(src)
	newMemorySize := colmem.GetBatchMemSize(dest)

	// We expect that the original and the new memory sizes are relatively close
	// to each other (do not differ by more than a third). We cannot guarantee
	// more precise bound here because the capacities of the underlying []byte
	// slices is unpredictable. However, this check is sufficient to ensure that
	// we don't double count memory under `Bytes.data`.
	const maxDeviation = float64(0.33)
	deviation := math.Abs(float64(originalMemorySize-newMemorySize) / (float64(originalMemorySize)))
	require.GreaterOrEqualf(t, maxDeviation, deviation,
		"new memory size %d is too far away from original %d", newMemorySize, originalMemorySize)
}

func BenchmarkRecordBatchSerializerInt64(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	var (
		typs             = []*types.T{types.Int}
		buf              = bytes.Buffer{}
		deserializedData []*array.Data
	)

	s, err := colserde.NewRecordBatchSerializer(typs)
	require.NoError(b, err)

	for _, dataLen := range []int{1, 16, 256, 2048, 4096} {
		// Only calculate useful bytes.
		numBytes := int64(dataLen * 8)
		data := []*array.Data{randomDataFromType(rng, typs[0], dataLen, 0 /* nullProbability */)}
		b.Run(fmt.Sprintf("Serialize/dataLen=%d", dataLen), func(b *testing.B) {
			b.SetBytes(numBytes)
			for i := 0; i < b.N; i++ {
				buf.Reset()
				if _, _, err := s.Serialize(&buf, data, dataLen); err != nil {
					b.Fatal(err)
				}
			}
		})

		// buf should still have the result of the last serialization. It is still
		// empty in cases in which we run only the Deserialize benchmarks.
		if buf.Len() == 0 {
			if _, _, err := s.Serialize(&buf, data, dataLen); err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprintf("Deserialize/dataLen=%d", dataLen), func(b *testing.B) {
			b.SetBytes(numBytes)
			for i := 0; i < b.N; i++ {
				if _, err := s.Deserialize(&deserializedData, buf.Bytes()); err != nil {
					b.Fatal(err)
				}
				deserializedData = deserializedData[:0]
			}
		})
	}
}
