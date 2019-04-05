// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package arrow_test

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	goarrow "github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/arrow"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// randomDataFromType creates an *array.Data of length n and type dataType,
// filling it with random values and inserting nulls with probability
// nullProbability.
func randomDataFromType(
	rng *rand.Rand, dataType goarrow.DataType, n int, nullProbability float64,
) *array.Data {
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
	switch dataType.ID() {
	case goarrow.BOOL:
		builder = array.NewBooleanBuilder(memory.DefaultAllocator)
		data := make([]bool, n)
		for i := range data {
			if rng.Float64() < 0.5 {
				data[i] = true
			}
		}
		builder.(*array.BooleanBuilder).AppendValues(data, valid)
	case goarrow.INT8:
		builder = array.NewInt8Builder(memory.DefaultAllocator)
		data := make([]int8, n)
		for i := range data {
			data[i] = int8(rng.Uint64())
		}
		builder.(*array.Int8Builder).AppendValues(data, valid)
	case goarrow.INT16:
		builder = array.NewInt16Builder(memory.DefaultAllocator)
		data := make([]int16, n)
		for i := range data {
			data[i] = int16(rng.Uint64())
		}
		builder.(*array.Int16Builder).AppendValues(data, valid)
	case goarrow.INT32:
		builder = array.NewInt32Builder(memory.DefaultAllocator)
		data := make([]int32, n)
		for i := range data {
			data[i] = int32(rng.Uint64())
		}
		builder.(*array.Int32Builder).AppendValues(data, valid)
	case goarrow.INT64:
		builder = array.NewInt64Builder(memory.DefaultAllocator)
		data := make([]int64, n)
		for i := range data {
			data[i] = int64(rng.Uint64())
		}
		builder.(*array.Int64Builder).AppendValues(data, valid)
	case goarrow.FLOAT32:
		builder = array.NewFloat32Builder(memory.DefaultAllocator)
		data := make([]float32, n)
		for i := range data {
			data[i] = rng.Float32() * math.MaxFloat32
		}
		builder.(*array.Float32Builder).AppendValues(data, valid)
	case goarrow.FLOAT64:
		builder = array.NewFloat64Builder(memory.DefaultAllocator)
		data := make([]float64, n)
		for i := range data {
			data[i] = rng.Float64() * math.MaxFloat64
		}
		builder.(*array.Float64Builder).AppendValues(data, valid)
	case goarrow.STRING:
		builder = array.NewStringBuilder(memory.DefaultAllocator)
		data := make([]string, n)
		stringBuilder := &strings.Builder{}
		for i := range data {
			stringBuilder.Reset()
			for j := 0; j < rng.Intn(maxVarLen)+1; j++ {
				stringBuilder.WriteRune(rune(charset[rng.Intn(len(charset))]))
			}
			data[i] = stringBuilder.String()
		}
		builder.(*array.StringBuilder).AppendValues(data, valid)
	case goarrow.BINARY:
		builder = array.NewBinaryBuilder(memory.DefaultAllocator, goarrow.BinaryTypes.Binary)
		data := make([][]byte, n)
		for i := range data {
			slice := make([]byte, rng.Intn(maxVarLen))
			// Read always returns len(slice) and nil error.
			_, _ = rng.Read(slice)
			data[i] = slice
		}
		builder.(*array.BinaryBuilder).AppendValues(data, valid)
	case goarrow.FIXED_SIZE_BINARY:
		width := rng.Intn(maxVarLen) + 1
		builder = array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &goarrow.FixedSizeBinaryType{ByteWidth: width})
		data := make([][]byte, n)
		for i := range data {
			slice := make([]byte, width)
			_, _ = rng.Read(slice)
			data[i] = slice
		}
		builder.(*array.FixedSizeBinaryBuilder).AppendValues(data, valid)
	default:
		panic(fmt.Sprintf("unsupported type %s", dataType.Name()))
	}
	return builder.NewArray().Data()
}

func TestRecordBatchSerializer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typs := []goarrow.DataType{goarrow.PrimitiveTypes.Int64, goarrow.PrimitiveTypes.Int64}

	t.Run("UnsupportedSchema", func(t *testing.T) {
		_, err := arrow.NewRecordBatchSerializer([]goarrow.DataType{})
		require.True(t, testutils.IsError(err, "zero length"), err)
		_, err = arrow.NewRecordBatchSerializer([]goarrow.DataType{goarrow.FixedWidthTypes.Time32ms})
		require.True(t, testutils.IsError(err, "unsupported arrow type"), err)
	})

	t.Run("SerializeInvalidSchema", func(t *testing.T) {
		s, err := arrow.NewRecordBatchSerializer(typs)
		require.NoError(t, err)
		// Different length.
		err = s.Serialize(&bytes.Buffer{}, []*array.Data{})
		require.True(t, testutils.IsError(err, "mismatched schema length"), err)
		// Same length, different types.
		b := array.NewStringBuilder(memory.DefaultAllocator)
		err = s.Serialize(&bytes.Buffer{}, []*array.Data{b.NewArray().Data(), b.NewArray().Data()})
		require.True(t, testutils.IsError(err, "unsupported"), err)
	})

	// Deserializing an invalid schema is undefined.

	t.Run("SerializeDifferentColumnLengths", func(t *testing.T) {
		s, err := arrow.NewRecordBatchSerializer(typs)
		require.NoError(t, err)
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.AppendValues([]int64{1, 2}, nil /* valid */)
		firstCol := b.NewArray().Data()
		b.AppendValues([]int64{3}, nil /* valid */)
		secondCol := b.NewArray().Data()
		err = s.Serialize(&bytes.Buffer{}, []*array.Data{firstCol, secondCol})
		require.True(t, testutils.IsError(err, "mismatched data lengths"), err)
	})
}

func TestRecordBatchSerializerSerializeDeserializeRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	const (
		maxTypes   = 16
		maxDataLen = 2056
	)

	var (
		supportedTypes  = arrow.SupportedTypes()
		typs            = make([]goarrow.DataType, rng.Intn(maxTypes)+1)
		data            = make([]*array.Data, len(typs))
		dataLen         = rng.Intn(maxDataLen) + 1
		nullProbability = rng.Float64()
		buf             = bytes.Buffer{}
	)
	for i := range typs {
		typs[i] = supportedTypes[rng.Intn(len(supportedTypes))]
		data[i] = randomDataFromType(rng, typs[i], dataLen, nullProbability)
	}

	s, err := arrow.NewRecordBatchSerializer(typs)
	if err != nil {
		t.Fatal(err)
	}

	// Run Serialize/Deserialize in a loop to test reuse.
	for i := 0; i < 2; i++ {
		buf.Reset()
		require.NoError(t, s.Serialize(&buf, data))
		if buf.Len()%8 != 0 {
			t.Fatal("message length must align to 8 byte boundary")
		}
		var deserializedData []*array.Data
		require.NoError(t, s.Deserialize(&deserializedData, buf.Bytes()))

		// Check the fields we care most about. We can't use require.Equal directly
		// due to some unimportant differences (e.g. mutability of
		// underlying buffers).
		require.Equal(t, len(data), len(deserializedData))
		for i := range data {
			require.Equal(t, data[i].Len(), deserializedData[i].Len())
			require.Equal(t, len(data[i].Buffers()), len(deserializedData[i].Buffers()))
			require.Equal(t, data[i].NullN(), deserializedData[i].NullN())
			require.Equal(t, data[i].Offset(), deserializedData[i].Offset())
			decBuffers := deserializedData[i].Buffers()
			for j, buf := range data[i].Buffers() {
				require.Equal(t, buf.Len(), decBuffers[j].Len())
				require.Equal(t, buf.Bytes(), decBuffers[j].Bytes())
			}
		}
	}
}

func BenchmarkRecordBatchSerializer(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	var (
		typs             = []goarrow.DataType{goarrow.PrimitiveTypes.Int64}
		buf              = bytes.Buffer{}
		deserializedData []*array.Data
	)

	s, err := arrow.NewRecordBatchSerializer(typs)
	require.NoError(b, err)

	for _, dataLen := range []int{1, 16, 256, 2048, 4096} {
		// Only calculate useful bytes.
		numBytes := int64(dataLen * 8)
		data := []*array.Data{randomDataFromType(rng, typs[0], dataLen, 0 /* nullProbability */)}
		b.Run(fmt.Sprintf("Serialize/dataLen=%d", dataLen), func(b *testing.B) {
			b.SetBytes(numBytes)
			for i := 0; i < b.N; i++ {
				buf.Reset()
				if err := s.Serialize(&buf, data); err != nil {
					b.Fatal(err)
				}
			}
		})

		// buf should still have the result of the last serialization. It is still
		// empty in cases in which we run only the Deserialize benchmarks.
		if buf.Len() == 0 {
			if err := s.Serialize(&buf, data); err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprintf("Deserialize/dataLen=%d", dataLen), func(b *testing.B) {
			b.SetBytes(numBytes)
			for i := 0; i < b.N; i++ {
				if err := s.Deserialize(&deserializedData, buf.Bytes()); err != nil {
					b.Fatal(err)
				}
				deserializedData = deserializedData[:0]
			}
		})
	}
}
