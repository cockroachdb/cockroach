// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecencoding_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
					metadata := cspann.PartitionMetadata{
						Level:        level,
						Centroid:     quantizedSet.GetCentroid(),
						StateDetails: cspann.MakeSplittingDetails(10, 20),
					}
					originalPartition := cspann.NewPartition(metadata, quantizer, quantizedSet, childKeys, valueBytes)

					// Encode the partition.
					encMetadata := vecencoding.EncodeMetadataValue(metadata)

					// Create a single buffer containing all vectors.
					var buf []byte
					for i := range set.Count {
						switch quantizedSet := quantizedSet.(type) {
						case *quantize.UnQuantizedVectorSet:
							var err error
							buf, err = vecencoding.EncodeUnquantizerVector(buf,
								quantizedSet.GetCentroidDistances()[i], set.At(i),
							)
							require.NoError(t, err)
						case *quantize.RaBitQuantizedVectorSet:
							buf = vecencoding.EncodeRaBitQVector(buf,
								quantizedSet.CodeCounts[i], quantizedSet.CentroidDistances[i],
								quantizedSet.DotProducts[i], quantizedSet.Codes.At(i),
							)
						}
					}

					// Add some trailing data that should not be processed.
					trailingData := testutils.NormalizeSlice(randutil.RandBytes(rnd, rnd.Intn(32)))
					buf = append(buf, trailingData...)

					// Decode the encoded partition.
					decodedMetadata, err := vecencoding.DecodeMetadataValue(encMetadata)
					require.NoError(t, err)
					var decodedSet quantize.QuantizedVectorSet
					remainder := buf

					switch quantizedSet.(type) {
					case *quantize.UnQuantizedVectorSet:
						decodedSet = quantizer.NewQuantizedVectorSet(set.Count, decodedMetadata.Centroid)
						for range set.Count {
							remainder, err = vecencoding.DecodeUnquantizedVectorToSet(
								remainder, decodedSet.(*quantize.UnQuantizedVectorSet),
							)
							require.NoError(t, err)
						}
						// Verify remaining bytes match trailing data
						require.Equal(t, trailingData, testutils.NormalizeSlice(remainder))
					case *quantize.RaBitQuantizedVectorSet:
						decodedSet = quantizer.NewQuantizedVectorSet(set.Count, decodedMetadata.Centroid)
						for range set.Count {
							remainder, err = vecencoding.DecodeRaBitQVectorToSet(
								remainder, decodedSet.(*quantize.RaBitQuantizedVectorSet),
							)
							require.NoError(t, err)
						}
						// Verify remaining bytes match trailing data
						require.Equal(t, trailingData, testutils.NormalizeSlice(remainder))
					}

					metadata = cspann.PartitionMetadata{
						Level:        decodedMetadata.Level,
						Centroid:     decodedMetadata.Centroid,
						StateDetails: decodedMetadata.StateDetails,
					}
					decodedPartition := cspann.NewPartition(
						metadata, quantizer, decodedSet, childKeys, valueBytes)
					testingAssertPartitionsEqual(t, originalPartition, decodedPartition)
				})
			}
		})
	}
}

func testingAssertPartitionsEqual(t *testing.T, l, r *cspann.Partition) {
	m1, m2 := l.Metadata(), r.Metadata()
	require.True(t, m1.Equal(m2), "metadata does not match\n%+v\n\n%+v", m1, m2)
	require.Equal(t, l.ChildKeys(), r.ChildKeys(), "childKeys do not match")
	require.Equal(t, l.ValueBytes(), r.ValueBytes(), "valueBytes do not match")
	q1, q2 := l.QuantizedSet(), r.QuantizedSet()
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

func TestEncodeKeys(t *testing.T) {
	// None of the encoding routines should disturb the input bytes.
	input := roachpb.Key{1, 2, 3}

	// EncodeMetadataKey.
	encodedMeta := vecencoding.EncodeMetadataKey(input, input, 10)
	require.Equal(t, roachpb.Key{1, 2, 3, 1, 2, 3, 146, 136}, encodedMeta)

	// EncodeStartVectorKey.
	encodedStart := vecencoding.EncodeStartVectorKey(encodedMeta)
	require.Equal(t, roachpb.Key{1, 2, 3, 1, 2, 3, 146, 137}, encodedStart)
	require.Negative(t, bytes.Compare(encodedMeta, encodedStart))

	// EncodeEndVectorKey.
	encodedEnd := vecencoding.EncodeEndVectorKey(encodedMeta)
	require.Equal(t, roachpb.Key{1, 2, 3, 1, 2, 3, 147}, encodedEnd)
	require.Negative(t, bytes.Compare(encodedMeta, encodedEnd))
	require.Negative(t, bytes.Compare(encodedStart, encodedEnd))

	// EncodePrefixVectorKey and EncodedPrefixVectorKeyLen.
	encodedPrefix := vecencoding.EncodePrefixVectorKey(encodedMeta, cspann.SecondLevel)
	require.Equal(t, roachpb.Key{1, 2, 3, 1, 2, 3, 146, 138}, encodedPrefix)
	require.Negative(t, bytes.Compare(encodedStart, encodedPrefix))
	require.Negative(t, bytes.Compare(encodedPrefix, encodedEnd))
	require.Equal(t, 3, vecencoding.EncodedPrefixVectorKeyLen(input, cspann.SecondLevel))

	// EncodeMetadataValue and DecodeMetadataValue.
	metadata1 := cspann.PartitionMetadata{
		Level:        cspann.LeafLevel,
		Centroid:     vector.T{4, 3},
		StateDetails: cspann.MakeDrainingForMergeDetails(10),
	}
	encoded := vecencoding.EncodeMetadataValue(metadata1)
	metadata2, err := vecencoding.DecodeMetadataValue(encoded)
	require.NoError(t, err)
	require.True(t, metadata1.Equal(&metadata2),
		"metadata does not match\n%+v\n\n%+v", metadata1, metadata2)
}

func TestDecodeVectorKey(t *testing.T) {
	// Build an encoded key with no prefix columns.
	var buf []byte
	// Encode a partition key (e.g. 456).
	partitionKey := cspann.PartitionKey(456)
	buf = vecencoding.EncodePartitionKey(buf, partitionKey)
	// Encode a partition level (e.g. 2).
	level := cspann.Level(2)
	buf = vecencoding.EncodePartitionLevel(buf, level)
	// Add some suffix bytes.
	expectedSuffix := []byte("mySuffix")
	buf = append(buf, expectedSuffix...)

	// Call DecodeKey with numPrefixColumns=0.
	indexKey, err := vecencoding.DecodeVectorKey(buf, 0)
	require.NoError(t, err)
	// No prefix since numPrefixColumns=0.
	require.Empty(t, indexKey.Prefix)
	// Verify partition key and level.
	require.Equal(t, partitionKey, indexKey.PartitionKey)
	require.Equal(t, level, indexKey.Level)
	// Suffix should match.
	require.Equal(t, expectedSuffix, indexKey.Suffix)
}

func TestDecodeKeyPrefixColumns(t *testing.T) {
	var buf []byte

	// Encode a prefix column.
	// Here we simulate a prefix column by encoding a byte slice.
	prefixVal := []byte("prefixColumnValue")
	buf = encoding.EncodeBytesAscending(buf, prefixVal)

	// Encode another prefix column.
	prefixVal2 := []byte("anotherPrefixColumnValue")
	buf = encoding.EncodeBytesAscending(buf, prefixVal2)

	// Capture the prefix bytes to compare later.
	prefixEncoded := make([]byte, len(buf))
	copy(prefixEncoded, buf)

	// Append a partition key.
	partitionKey := cspann.PartitionKey(1234)
	buf = vecencoding.EncodePartitionKey(buf, partitionKey)

	// Append a partition level.
	level := cspann.Level(5)
	buf = vecencoding.EncodePartitionLevel(buf, level)

	// Append some suffix bytes.
	expectedSuffix := []byte("suffixData")
	buf = append(buf, expectedSuffix...)

	// Extract the key with one prefix column.
	key, err := vecencoding.DecodeVectorKey(buf, 2)
	require.NoError(t, err)

	// Verify that the extracted prefix matches.
	require.Equal(t, prefixEncoded, key.Prefix)
	// Verify partition key and level.
	require.Equal(t, partitionKey, key.PartitionKey)
	require.Equal(t, level, key.Level)
	// Verify that the remaining bytes form the suffix.
	require.Equal(t, expectedSuffix, key.Suffix)
}
