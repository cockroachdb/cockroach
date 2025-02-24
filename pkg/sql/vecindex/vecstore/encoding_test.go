// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rnd, seed := randutil.NewTestRand()
	t.Logf("random seed: %v", seed)

	dims := rnd.Intn(100) + 1
	count := rnd.Intn(128)

	set := vector.MakeSet(dims)
	set.AddUndefined(count)
	for i := range count {
		vecDatum := randgen.RandDatum(rnd, types.MakePGVector(int32(dims)), false /* nullOk */)
		copy(set.At(i), vecDatum.(*tree.DPGVector).T)
	}
	testEncodeDecodeRoundTripImpl(t, rnd, set)
}

func testEncodeDecodeRoundTripImpl(t *testing.T, rnd *rand.Rand, set vector.Set) {
	var workspace workspace.T
	for _, quantizer := range []quantize.Quantizer{
		quantize.NewUnQuantizer(set.Dims),
		quantize.NewRaBitQuantizer(set.Dims, rnd.Int63()),
	} {
		name := strings.TrimPrefix(fmt.Sprintf("%T", quantizer), "*quantize.")
		t.Run(name, func(t *testing.T) {
			for _, level := range []cspann.Level{cspann.LeafLevel, cspann.Level(rnd.Intn(10)) + cspann.SecondLevel} {
				t.Run(fmt.Sprintf("level=%d", level), func(t *testing.T) {
					// Build the partition.
					quantizedSet := quantizer.Quantize(&workspace, set)
					childKeys := make([]cspann.ChildKey, set.Count)
					valueBytes := make([]cspann.ValueBytes, set.Count)
					for i := range childKeys {
						if level == cspann.LeafLevel {
							pkSize := rnd.Intn(32) + 1
							childKeys[i] = cspann.ChildKey{KeyBytes: randutil.RandBytes(rnd, pkSize)}
						} else {
							childKeys[i] = cspann.ChildKey{PartitionKey: cspann.PartitionKey(rnd.Uint64())}
						}
						valueBytes[i] = randutil.RandBytes(rnd, 10)
					}
					originalPartition := cspann.NewPartition(quantizer, quantizedSet, childKeys, valueBytes, level)

					// Encode the partition.
					encMetadata, err := EncodePartitionMetadata(level, quantizedSet.GetCentroid())
					require.NoError(t, err)

					// Create a single buffer containing all vectors.
					var buf []byte
					for i := range set.Count {
						switch quantizedSet := quantizedSet.(type) {
						case *quantize.UnQuantizedVectorSet:
							buf, err = EncodeUnquantizedVector(buf,
								quantizedSet.GetCentroidDistances()[i], set.At(i),
							)
							require.NoError(t, err)
						case *quantize.RaBitQuantizedVectorSet:
							buf = EncodeRaBitQVector(buf,
								quantizedSet.CodeCounts[i], quantizedSet.CentroidDistances[i],
								quantizedSet.DotProducts[i], quantizedSet.Codes.At(i),
							)
						}
					}

					// Add some trailing data that should not be processed.
					trailingData := testutils.NormalizeSlice(randutil.RandBytes(rnd, rnd.Intn(32)))
					buf = append(buf, trailingData...)

					// Decode the encoded partition.
					decodedLevel, decodedCentroid, err := DecodePartitionMetadata(encMetadata)
					require.NoError(t, err)
					var decodedSet quantize.QuantizedVectorSet
					remainder := buf

					switch quantizedSet.(type) {
					case *quantize.UnQuantizedVectorSet:
						decodedSet = quantizer.NewQuantizedVectorSet(set.Count, decodedCentroid)
						for range set.Count {
							remainder, err = DecodeUnquantizedVectorToSet(
								remainder, decodedSet.(*quantize.UnQuantizedVectorSet),
							)
							require.NoError(t, err)
						}
						// Verify remaining bytes match trailing data
						require.Equal(t, trailingData, testutils.NormalizeSlice(remainder))
					case *quantize.RaBitQuantizedVectorSet:
						decodedSet = quantizer.NewQuantizedVectorSet(set.Count, decodedCentroid)
						for range set.Count {
							remainder, err = DecodeRaBitQVectorToSet(
								remainder, decodedSet.(*quantize.RaBitQuantizedVectorSet),
							)
							require.NoError(t, err)
						}
						// Verify remaining bytes match trailing data
						require.Equal(t, trailingData, testutils.NormalizeSlice(remainder))
					}

					decodedPartition := cspann.NewPartition(
						quantizer, decodedSet, childKeys, valueBytes, decodedLevel)
					testingAssertPartitionsEqual(t, originalPartition, decodedPartition)
				})
			}
		})
	}
}

func testingAssertPartitionsEqual(t *testing.T, l, r *cspann.Partition) {
	q1, q2 := l.QuantizedSet(), r.QuantizedSet()
	require.Equal(t, l.Level(), r.Level(), "levels do not match")
	require.Equal(t, l.ChildKeys(), r.ChildKeys(), "childKeys do not match")
	require.Equal(t, l.ValueBytes(), r.ValueBytes(), "valueBytes do not match")
	require.Equal(t, q1.GetCentroid(), q2.GetCentroid(), "centroids do not match")
	require.Equal(t, q1.GetCount(), q2.GetCount(), "counts do not match")
	require.Equal(t, q1.GetCentroidDistances(), q2.GetCentroidDistances(), "distances do not match")
	switch leftSet := q1.(type) {
	case *quantize.UnQuantizedVectorSet:
		rightSet, ok := q2.(*quantize.UnQuantizedVectorSet)
		require.True(t, ok, "quantized set types do not match")
		require.True(t, leftSet.Vectors.Equal(&rightSet.Vectors), "vectors do not match")
	case *quantize.RaBitQuantizedVectorSet:
		rightSet, ok := q2.(*quantize.RaBitQuantizedVectorSet)
		require.True(t, ok, "quantized set types do not match")
		require.Equal(t, leftSet.CodeCounts, rightSet.CodeCounts, "code counts do not match")
		require.Equal(t, leftSet.Codes, rightSet.Codes, "codes do not match")
		require.Equal(t, leftSet.DotProducts, rightSet.DotProducts, "dot products do not match")
	default:
		t.Fatalf("unexpected type %T", q1)
	}
}
