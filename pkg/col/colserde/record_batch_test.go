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
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// randomDataFromType creates an *array.Data of length n and type t, filling it
// with random values and inserting nulls with probability nullProbability.
func randomDataFromType(rng *rand.Rand, t coltypes.T, n int, nullProbability float64) *array.Data {
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
	switch t {
	case coltypes.Bool:
		builder = array.NewBooleanBuilder(memory.DefaultAllocator)
		data := make([]bool, n)
		for i := range data {
			if rng.Float64() < 0.5 {
				data[i] = true
			}
		}
		builder.(*array.BooleanBuilder).AppendValues(data, valid)
	case coltypes.Int16:
		builder = array.NewInt16Builder(memory.DefaultAllocator)
		data := make([]int16, n)
		for i := range data {
			data[i] = int16(rng.Uint64())
		}
		builder.(*array.Int16Builder).AppendValues(data, valid)
	case coltypes.Int32:
		builder = array.NewInt32Builder(memory.DefaultAllocator)
		data := make([]int32, n)
		for i := range data {
			data[i] = int32(rng.Uint64())
		}
		builder.(*array.Int32Builder).AppendValues(data, valid)
	case coltypes.Int64:
		builder = array.NewInt64Builder(memory.DefaultAllocator)
		data := make([]int64, n)
		for i := range data {
			data[i] = int64(rng.Uint64())
		}
		builder.(*array.Int64Builder).AppendValues(data, valid)
	case coltypes.Float64:
		builder = array.NewFloat64Builder(memory.DefaultAllocator)
		data := make([]float64, n)
		for i := range data {
			data[i] = rng.Float64() * math.MaxFloat64
		}
		builder.(*array.Float64Builder).AppendValues(data, valid)
	case coltypes.Bytes:
		// Bytes can be represented 3 different ways. As variable-length bytes,
		// variable-length strings, or fixed-width bytes.
		representation := rng.Intn(3)
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
		}
	case coltypes.Decimal:
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
	case coltypes.Timestamp:
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
	case coltypes.Interval:
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
	default:
		panic(fmt.Sprintf("unsupported type %s", t))
	}
	return builder.NewArray().Data()
}

func TestRecordBatchSerializer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("UnsupportedSchema", func(t *testing.T) {
		_, err := colserde.NewRecordBatchSerializer([]coltypes.T{})
		require.True(t, testutils.IsError(err, "zero length"), err)
	})

	// Serializing and Deserializing an invalid schema is undefined.

	t.Run("SerializeDifferentColumnLengths", func(t *testing.T) {
		s, err := colserde.NewRecordBatchSerializer([]coltypes.T{coltypes.Int64, coltypes.Int64})
		require.NoError(t, err)
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.AppendValues([]int64{1, 2}, nil /* valid */)
		firstCol := b.NewArray().Data()
		b.AppendValues([]int64{3}, nil /* valid */)
		secondCol := b.NewArray().Data()
		_, _, err = s.Serialize(&bytes.Buffer{}, []*array.Data{firstCol, secondCol})
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
		typs            = make([]coltypes.T, rng.Intn(maxTypes)+1)
		data            = make([]*array.Data, len(typs))
		dataLen         = rng.Intn(maxDataLen) + 1
		nullProbability = rng.Float64()
		buf             = bytes.Buffer{}
	)

	for i := range typs {
		typs[i] = coltypes.AllTypes[rng.Intn(len(coltypes.AllTypes))]
		data[i] = randomDataFromType(rng, typs[i], dataLen, nullProbability)
	}

	s, err := colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		t.Fatal(err)
	}

	// Run Serialize/Deserialize in a loop to test reuse.
	for i := 0; i < 2; i++ {
		buf.Reset()
		_, _, err := s.Serialize(&buf, data)
		require.NoError(t, err)
		if buf.Len()%8 != 0 {
			t.Fatal("message length must align to 8 byte boundary")
		}
		var deserializedData []*array.Data
		require.NoError(t, s.Deserialize(&deserializedData, buf.Bytes()))

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

func BenchmarkRecordBatchSerializerInt64(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	var (
		typs             = []coltypes.T{coltypes.Int64}
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
				if _, _, err := s.Serialize(&buf, data); err != nil {
					b.Fatal(err)
				}
			}
		})

		// buf should still have the result of the last serialization. It is still
		// empty in cases in which we run only the Deserialize benchmarks.
		if buf.Len() == 0 {
			if _, _, err := s.Serialize(&buf, data); err != nil {
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
