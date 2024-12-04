// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// EncodePartitionMetadata encodes the metadata for a partition.
func EncodePartitionMetadata(level Level, centroid vector.T) ([]byte, error) {
	// The encoding consists of 8 bytes for the level, and a 4-byte length,
	// followed by 4 bytes for each dimension in the vector.
	encMetadataSize := 8 + (4 * (len(centroid) + 1))
	buf := make([]byte, 0, encMetadataSize)
	buf = encoding.EncodeUint64Ascending(buf, uint64(level))
	return vector.Encode(buf, centroid)
}

// EncodeRaBitQVector encodes a RaBitQ vector into the given byte slice.
func EncodeRaBitQVector(
	appendTo []byte, codeCount uint32, centroidDistance, dotProduct float32, code quantize.RaBitQCode,
) []byte {
	appendTo = encoding.EncodeUint32Ascending(appendTo, codeCount)
	appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, centroidDistance)
	appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, dotProduct)
	for _, c := range code {
		appendTo = encoding.EncodeUint64Ascending(appendTo, c)
	}
	return appendTo
}

// EncodeFullVector encodes a full vector into the given byte slice.
func EncodeFullVector(appendTo []byte, centroidDistance float32, v vector.T) ([]byte, error) {
	appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, centroidDistance)
	return vector.Encode(appendTo, v)
}

// EncodeChildKey encodes a child key into the given byte slice.
func EncodeChildKey(appendTo []byte, key ChildKey) []byte {
	if key.PrimaryKey != nil {
		return encoding.EncodeBytesAscending(appendTo, key.PrimaryKey)
	}
	return encoding.EncodeUint64Ascending(appendTo, uint64(key.PartitionKey))
}

// DecodePartitionMetadata decodes the metadata for a partition.
func DecodePartitionMetadata(encMetadata []byte) (Level, vector.T, error) {
	encMetadata, level, err := encoding.DecodeUint64Ascending(encMetadata)
	if err != nil {
		return 0, nil, err
	}
	centroid, err := vector.Decode(encMetadata)
	return Level(level), centroid, nil
}

// DecodeRaBitQVectorToSet decodes a RaBitQ vector entry into the given
// RaBitQuantizedVectorSet. The vector set must have been initialized with the
// correct number of dimensions.
func DecodeRaBitQVectorToSet(encVector []byte, vectorSet *quantize.RaBitQuantizedVectorSet) error {
	encVector, codeCount, err := encoding.DecodeUint32Ascending(encVector)
	if err != nil {
		return err
	}
	encVector, centroidDistance, err := encoding.DecodeUntaggedFloat32Value(encVector)
	if err != nil {
		return err
	}
	encVector, dotProduct, err := encoding.DecodeUntaggedFloat32Value(encVector)
	if err != nil {
		return err
	}
	vectorSet.CodeCounts = append(vectorSet.CodeCounts, codeCount)
	vectorSet.CentroidDistances = append(vectorSet.CentroidDistances, centroidDistance)
	vectorSet.DotProducts = append(vectorSet.DotProducts, dotProduct)
	for i := 0; i < vectorSet.Codes.Width; i++ {
		var codeWord uint64
		encVector, codeWord, err = encoding.DecodeUint64Ascending(encVector)
		if err != nil {
			return err
		}
		vectorSet.Codes.Data = append(vectorSet.Codes.Data, codeWord)
	}
	vectorSet.Codes.Count++
	return nil
}

// DecodeFullVectorToSet decodes a full vector entry into the given
// UnQuantizedVectorSet. The vector set must have been initialized with the
// correct number of dimensions.
func DecodeFullVectorToSet(encVector []byte, vectorSet *quantize.UnQuantizedVectorSet) error {
	encVector, centroidDistance, err := encoding.DecodeUntaggedFloat32Value(encVector)
	if err != nil {
		return err
	}
	v, err := vector.Decode(encVector)
	if err != nil {
		return err
	}
	vectorSet.CentroidDistances = append(vectorSet.CentroidDistances, centroidDistance)
	vectorSet.Vectors.Add(v)
	return nil
}

// DecodeChildKey decodes a child key from the given byte slice.
func DecodeChildKey(encChildKey []byte, level Level) (ChildKey, error) {
	if level == LeafLevel {
		// Leaf vectors point to the primary index.
		_, decodedPrimaryKey, err := encoding.DecodeBytesAscending(encChildKey, []byte(nil))
		if err != nil {
			return ChildKey{}, err
		}
		return ChildKey{PrimaryKey: decodedPrimaryKey}, nil
	} else {
		// Non-leaf vectors point to the partition key.
		_, childPartitionKey, err := encoding.DecodeUint64Ascending(encChildKey)
		if err != nil {
			return ChildKey{}, err
		}
		return ChildKey{PartitionKey: PartitionKey(childPartitionKey)}, nil
	}
}

// GetRaBitQChildKeyOffset returns the offset of the child key in an encoded
// RaBitQ vector entry.
func GetRaBitQChildKeyOffset(dims int) int {
	// RaBitQ vector encodings include a code count, a centroid distance, a dot
	// product, and a variable-length code.
	const (
		codeCountEncSize        = 4
		centroidDistanceEncSize = 4
		dotProductEncSize       = 4
		codeWordEncSize         = 8
	)
	codeEncSize := codeWordEncSize * quantize.RaBitQCodeSetWidth(dims)
	return codeCountEncSize + centroidDistanceEncSize + dotProductEncSize + codeEncSize
}

// GetFullVectorChildKeyOffset returns the offset of the child key in an encoded
// full vector entry.
func GetFullVectorChildKeyOffset(dims int) int {
	// Full vectors encodings include a centroid distance as well as the encoded
	// vector. The encoded vector is prefixed by a 4-byte length.
	const (
		centroidDistanceEncSize = 4
		vectorLengthEncSize     = 4
		vectorDimEncSize        = 4
	)
	return centroidDistanceEncSize + vectorLengthEncSize + (vectorDimEncSize * dims)
}
