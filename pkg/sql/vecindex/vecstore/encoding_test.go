// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
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
	testEncodeDecodeRoundTripImpl(t, rnd, &set)
}

func testEncodeDecodeRoundTripImpl(t *testing.T, rnd *rand.Rand, set *vector.Set) {
	ctx := internal.WithWorkspace(context.Background(), &internal.Workspace{})
	for _, quantizer := range []quantize.Quantizer{
		quantize.NewUnQuantizer(set.Dims),
		quantize.NewRaBitQuantizer(set.Dims, rnd.Int63()),
	} {
		name := strings.TrimPrefix(fmt.Sprintf("%T", quantizer), "*quantize.")
		t.Run(name, func(t *testing.T) {
			for _, level := range []Level{LeafLevel, Level(rnd.Intn(10)) + SecondLevel} {
				t.Run(fmt.Sprintf("level=%d", level), func(t *testing.T) {
					// Build the partition.
					quantizedSet := quantizer.Quantize(ctx, set)
					childKeys := make([]ChildKey, set.Count)
					for i := range childKeys {
						if level == LeafLevel {
							pkSize := rnd.Intn(32) + 1
							childKeys[i] = ChildKey{PrimaryKey: randutil.RandBytes(rnd, pkSize)}
						} else {
							childKeys[i] = ChildKey{PartitionKey: PartitionKey(rnd.Uint64())}
						}
					}
					originalPartition := NewPartition(quantizer, quantizedSet, childKeys, level)

					// Encode the partition.
					encMetadata, err := EncodePartitionMetadata(level, quantizedSet.GetCentroid())
					require.NoError(t, err)
					encVectors := make([][]byte, set.Count)
					encChildren := make([][]byte, len(childKeys))
					for i := range set.Count {
						switch quantizedSet := quantizedSet.(type) {
						case *quantize.UnQuantizedVectorSet:
							encVectors[i], err = EncodeUnquantizedVector([]byte(nil),
								quantizedSet.GetCentroidDistances()[i], set.At(i),
							)
							require.NoError(t, err)
						case *quantize.RaBitQuantizedVectorSet:
							encVectors[i] = EncodeRaBitQVector([]byte(nil),
								quantizedSet.CodeCounts[i], quantizedSet.CentroidDistances[i],
								quantizedSet.DotProducts[i], quantizedSet.Codes.At(i),
							)
						}
						encChildren[i] = EncodeChildKey([]byte(nil), childKeys[i])
					}

					// Decode the encoded partition.
					decodedLevel, decodedCentroid, err := DecodePartitionMetadata(encMetadata)
					require.NoError(t, err)
					decodedChildKeys := make([]ChildKey, len(encVectors))
					var decodedSet quantize.QuantizedVectorSet
					switch quantizedSet.(type) {
					case *quantize.UnQuantizedVectorSet:
						decodedSet = &quantize.UnQuantizedVectorSet{
							Centroid: decodedCentroid,
							Vectors:  vector.MakeSet(set.Dims),
						}
						for i := range encVectors {
							err = DecodeUnquantizedVectorToSet(
								encVectors[i], decodedSet.(*quantize.UnQuantizedVectorSet),
							)
							require.NoError(t, err)
						}
					case *quantize.RaBitQuantizedVectorSet:
						decodedSet = &quantize.RaBitQuantizedVectorSet{
							Centroid: decodedCentroid,
							Codes:    quantize.MakeRaBitQCodeSet(set.Dims),
						}
						for i := range encVectors {
							err = DecodeRaBitQVectorToSet(
								encVectors[i], decodedSet.(*quantize.RaBitQuantizedVectorSet),
							)
							require.NoError(t, err)
						}
					}
					for i := range encChildren {
						decodedChildKeys[i], err = DecodeChildKey(encChildren[i], decodedLevel)
						require.NoError(t, err)
					}
					decodedPartition := NewPartition(quantizer, decodedSet, decodedChildKeys, decodedLevel)
					testingAssertPartitionsEqual(t, originalPartition, decodedPartition)
				})
			}
		})
	}
}

func testingAssertPartitionsEqual(t *testing.T, l, r *Partition) {
	q1, q2 := l.quantizedSet, r.quantizedSet
	require.Equal(t, l.level, r.level, "levels do not match")
	require.Equal(t, l.ChildKeys(), r.ChildKeys(), "childKeys do not match")
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
