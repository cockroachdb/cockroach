// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecencoding

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

/* Vector indexes are encoded as follows:

Interior (non-leaf) partition KV key:
  ┌────────────┬──────────────┬───────────┬─────┬─────────────────┐
  │Index Prefix│Prefix Columns│PartitionID│Level│Child PartitionID│
  └────────────┴──────────────┴───────────┴─────┴─────────────────┘
Leaf partition KV key:
  ┌────────────┬──────────────┬───────────┬─────┬───────────┬────────────────────┐
  │Index Prefix│Prefix Columns│PartitionID│Level│Primary Key│Sentinel Family ID 0│
  └────────────┴──────────────┴───────────┴─────┴───────────┴────────────────────┘
Value:
  ┌────────────────────────┬────────────────────────┐
  │Quantized+Encoded Vector│Composite+Stored Columns│
  └────────────────────────┴────────────────────────┘
*/

// VectorIndexKey is a deconstructed key value, as described above, minus the
// index prefix. Any suffix bytes (child partition, primary key) are left in the
// Suffix value.
type VectorIndexKey struct {
	Prefix       cspann.TreeKey
	PartitionKey cspann.PartitionKey
	Level        cspann.Level
	Suffix       []byte
}

// DecodeKey takes an encoded key value, minus the /Tenant/Table/Index prefix,
// and extracts the vector index specific portions of it in an VectorIndexKey
// struct.
func DecodeKey(keyBytes []byte, numPrefixColumns int) (vecIndexKey VectorIndexKey, err error) {
	prefixLen := 0
	for i := 0; i < numPrefixColumns; i++ {
		columnWidth, err := encoding.PeekLength(keyBytes[prefixLen:])
		if err != nil {
			return vecIndexKey, err
		}
		prefixLen += columnWidth
	}
	if prefixLen > 0 {
		vecIndexKey.Prefix = keyBytes[:prefixLen]
		keyBytes = keyBytes[prefixLen:]
	}

	partitionKey, keyBytes, err := DecodePartitionKey(keyBytes)
	if err != nil {
		return vecIndexKey, err
	}
	vecIndexKey.PartitionKey = partitionKey

	level, keyBytes, err := DecodePartitionLevel(keyBytes)
	if err != nil {
		return vecIndexKey, err
	}
	vecIndexKey.Level = level

	if len(keyBytes) > 0 {
		vecIndexKey.Suffix = keyBytes
	}

	return vecIndexKey, nil
}

// Encode takes a VectorIndexKey and turns it back into encoded bytes that can be
// appended to the index's prefix to form a key value.
func (vik *VectorIndexKey) Encode(appendTo []byte) []byte {
	appendTo = append(appendTo, vik.Prefix...)
	appendTo = EncodePartitionKey(appendTo, vik.PartitionKey)
	appendTo = EncodePartitionLevel(appendTo, vik.Level)
	return append(appendTo, vik.Suffix...)
}

// EncodedVectorIndexValueLen returns the number of bytes needed to encode the
// value side of a vector index entry.
func EncodedVectorIndexValueLen(vectorData []byte, compositeData []byte) int {
	return len(vectorData) + len(compositeData)
}

// EncodeVectorIndexValue takes a quantized vector entry and any composite key
// data and returns the byte slice encoding the value side of the vector index
// entry. This value will still need to be further encoded as Bytes in the
// valueside.Value
func EncodeVectorIndexValue(appendTo []byte, vectorData []byte, compositeData []byte) []byte {
	// The value side is encoded as a concatenation of the vector data and the
	// composite data.
	appendTo = append(appendTo, vectorData...)
	return append(appendTo, compositeData...)
}

// EncodePartitionMetadata encodes the metadata for a partition.
func EncodePartitionMetadata(level cspann.Level, centroid vector.T) ([]byte, error) {
	// The encoding consists of 8 bytes for the level, and a 4-byte length,
	// followed by 4 bytes for each dimension in the vector.
	encMetadataSize := 8 + (4 * (len(centroid) + 1))
	buf := make([]byte, 0, encMetadataSize)
	buf = encoding.EncodeUint32Ascending(buf, uint32(level))
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

// EncodeUnquantizedVector encodes a full vector into the given byte slice.
func EncodeUnquantizedVector(
	appendTo []byte, centroidDistance float32, v vector.T,
) ([]byte, error) {
	appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, centroidDistance)
	return vector.Encode(appendTo, v)
}

// EncodePartitionKey encodes a partition key into the given byte slice.
func EncodePartitionKey(appendTo []byte, key cspann.PartitionKey) []byte {
	return encoding.EncodeUvarintAscending(appendTo, uint64(key))
}

// EncodedPartitionKeyLen returns the number of bytes needed to encode the
// partition key.
func EncodedPartitionKeyLen(key cspann.PartitionKey) int {
	return encoding.EncLenUvarintAscending(uint64(key))
}

// EncodePartitionLevel encodes a partition's level into the given byte slice.
// The level can be used to filter leaf vectors when scanning the partition.
func EncodePartitionLevel(appendTo []byte, level cspann.Level) []byte {
	return encoding.EncodeUvarintAscending(appendTo, uint64(level))
}

// EncodedPartitionLevelLen returns the number of bytes needed to encode the
// partition level.
func EncodedPartitionLevelLen(level cspann.Level) int {
	return encoding.EncLenUvarintAscending(uint64(level))
}

// EncodeChildKey encodes a child key into the given byte slice. The "appendTo"
// slice is expected to be the prefix shared between all KV entries for a
// partition.
func EncodeChildKey(appendTo []byte, key cspann.ChildKey) []byte {
	if key.KeyBytes != nil {
		// The primary key is already in encoded form.
		return append(appendTo, key.KeyBytes...)
	}
	return EncodePartitionKey(appendTo, key.PartitionKey)
}

// DecodePartitionMetadata decodes the metadata for a partition.
func DecodePartitionMetadata(
	encMetadata []byte,
) (level cspann.Level, centroid vector.T, err error) {
	encMetadata, decodedLevel, err := encoding.DecodeUint32Ascending(encMetadata)
	if err != nil {
		return 0, nil, err
	}
	_, centroid, err = vector.Decode(encMetadata)
	if err != nil {
		return 0, nil, err
	}
	return cspann.Level(decodedLevel), centroid, nil
}

// DecodeRaBitQVectorToSet decodes a RaBitQ vector entry into the given
// RaBitQuantizedVectorSet. The vector set must have been initialized with the
// correct number of dimensions. It returns the remainder of the input buffer.
func DecodeRaBitQVectorToSet(
	encVector []byte, vectorSet *quantize.RaBitQuantizedVectorSet,
) ([]byte, error) {
	encVector, codeCount, err := encoding.DecodeUint32Ascending(encVector)
	if err != nil {
		return nil, err
	}
	encVector, centroidDistance, err := encoding.DecodeUntaggedFloat32Value(encVector)
	if err != nil {
		return nil, err
	}
	encVector, dotProduct, err := encoding.DecodeUntaggedFloat32Value(encVector)
	if err != nil {
		return nil, err
	}
	vectorSet.CodeCounts = append(vectorSet.CodeCounts, codeCount)
	vectorSet.CentroidDistances = append(vectorSet.CentroidDistances, centroidDistance)
	vectorSet.DotProducts = append(vectorSet.DotProducts, dotProduct)
	vectorSet.Codes.Data = slices.Grow(vectorSet.Codes.Data, vectorSet.Codes.Width)
	for i := 0; i < vectorSet.Codes.Width; i++ {
		var codeWord uint64
		encVector, codeWord, err = encoding.DecodeUint64Ascending(encVector)
		if err != nil {
			return nil, err
		}
		vectorSet.Codes.Data = append(vectorSet.Codes.Data, codeWord)
	}
	vectorSet.Codes.Count++
	return encVector, nil
}

// DecodeUnquantizedVectorToSet decodes a full vector entry into the given
// UnQuantizedVectorSet. The vector set must have been initialized with the
// correct number of dimensions. It returns the remainder of the input buffer.
func DecodeUnquantizedVectorToSet(
	encVector []byte, vectorSet *quantize.UnQuantizedVectorSet,
) ([]byte, error) {
	encVector, centroidDistance, err := encoding.DecodeUntaggedFloat32Value(encVector)
	if err != nil {
		return nil, err
	}
	encVector, v, err := vector.Decode(encVector)
	if err != nil {
		return nil, err
	}
	vectorSet.CentroidDistances = append(vectorSet.CentroidDistances, centroidDistance)
	vectorSet.Vectors.Add(v)
	return encVector, nil
}

// DecodeChildKey decodes a child key from the given byte slice.
// NOTE: the returned ChildKey may reference the input slice.
func DecodeChildKey(encChildKey []byte, level cspann.Level) (cspann.ChildKey, error) {
	if level == cspann.LeafLevel {
		// Leaf vectors point to the primary index. The primary key is already in
		// encoded form, so just use it as-is.
		return cspann.ChildKey{KeyBytes: encChildKey}, nil
	} else {
		// Non-leaf vectors point to the partition key.
		_, childPartitionKey, err := encoding.DecodeUvarintAscending(encChildKey)
		if err != nil {
			return cspann.ChildKey{}, err
		}
		return cspann.ChildKey{PartitionKey: cspann.PartitionKey(childPartitionKey)}, nil
	}
}

func DecodePartitionKey(encodedPartitionKey []byte) (cspann.PartitionKey, []byte, error) {
	remainingBytes, partitionKey, err := encoding.DecodeUvarintAscending(encodedPartitionKey)
	if err != nil {
		return 0, nil, err
	}
	return cspann.PartitionKey(partitionKey), remainingBytes, nil
}

func DecodePartitionLevel(encodedLevel []byte) (cspann.Level, []byte, error) {
	remainingBytes, level, err := encoding.DecodeUvarintAscending(encodedLevel)
	if err != nil {
		return 0, nil, err
	}

	return cspann.Level(level), remainingBytes, nil
}
