// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create test vectors and partitions
	dims := 4
	rootQuantizer := quantize.NewUnQuantizer(dims, vecdist.L2Squared)
	nonRootQuantizer := quantize.NewRaBitQuantizer(dims, 42, vecdist.L2Squared)

	// Create sample vectors
	vectors := []vector.T{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
	}

	// Create centroids
	rootCentroid := vector.T{0.0, 0.0, 0.0, 0.0}
	nonRootCentroid := vector.T{5.0, 6.0, 7.0, 8.0}

	t.Run("encode and decode the root partition", func(t *testing.T) {
		testEncodeDecode(t, rootQuantizer, nonRootQuantizer, cspann.RootKey, vectors, rootCentroid)
	})

	t.Run("encode and decode a non-root partition", func(t *testing.T) {
		testEncodeDecode(t, rootQuantizer, nonRootQuantizer, cspann.PartitionKey(42), vectors, nonRootCentroid)
	})
}

func testEncodeDecode(
	t *testing.T,
	rootQuantizer quantize.Quantizer,
	nonRootQuantizer quantize.Quantizer,
	partitionKey cspann.PartitionKey,
	vectors []vector.T,
	centroid vector.T,
) {
	codec := makePartitionCodec(rootQuantizer, nonRootQuantizer)
	metadata := cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, centroid)

	// Encode vectors and child keys.
	encodedVectors := make([][]byte, len(vectors))
	encodedChildKeys := make([][]byte, len(vectors))
	childKeys := make([]cspann.ChildKey, len(vectors))

	// Create child keys based on whether this is a leaf partition
	for i := range vectors {
		// Encode the child key.
		childKeys[i] = cspann.ChildKey{KeyBytes: fmt.Appendf(nil, "vec%d", i)}
		encodedChildKeys[i] = vecencoding.EncodeChildKey(nil, childKeys[i])

		// Encode the vector.
		var err error
		encodedVectors[i], err = codec.EncodeVector(partitionKey, vectors[i], centroid)
		require.NoError(t, err)

		// Add some value bytes.
		encodedVectors[i] = append(encodedVectors[i], byte(i))
		encodedVectors[i] = append(encodedVectors[i], byte(i+1))
	}

	// Initialize decoder and decode all vectors.
	codec.InitForDecoding(partitionKey, metadata, len(vectors))
	for i := range vectors {
		err := codec.DecodePartitionData(encodedChildKeys[i], encodedVectors[i])
		require.NoError(t, err)
	}

	// Get the partition and verify its content.
	partition := codec.GetPartition()
	require.True(t, partition.Metadata().Equal(&metadata))
	require.Equal(t, len(vectors), partition.Count())
	partitionChildKeys := partition.ChildKeys()
	partitionValueBytes := partition.ValueBytes()
	require.Equal(t, len(childKeys), len(partitionChildKeys))
	for i, expected := range childKeys {
		require.Equal(t, expected.KeyBytes, partitionChildKeys[i].KeyBytes)
		require.Equal(t, cspann.ValueBytes{byte(i), byte(i + 1)}, partitionValueBytes[i])
	}
}
