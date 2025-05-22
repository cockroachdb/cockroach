// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecencoding

import (
	"encoding/binary"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

/* Vector indexes are encoded as shown below.

NOTE: Key formats always are always suffixed by a Family ID byte of 0. This is
necessary to include so that the key can be parsed by the KV range split code.
That code uses the family ID to check that it doesn't split column families for
the same row across ranges.

Metadata KV Key:
  Metadata keys always sort before vector keys, since Level 0 is InvalidLevel,
  and vector keys always have Level >= 1. Also, using Level=0 prevents the
  metadata key from ever being a prefix of vector keys. The KV split logic does
  not support one KV key being a prefix of another KV key (after chopping the
  family ID, which it also does).
  ┌────────────┬──────────────┬────────────┬───────┬───────────┐
  │Index Prefix│Prefix Columns│PartitionKey│Level 0│Family ID 0│
  └────────────┴──────────────┴────────────┴───────┴───────────┘
Metadata KV Value:
  ┌─────┬─────┬───────┬───────┬──────┬─────────┬────────┐
  │Level│State│Target1│Target2│Source|Timestamp│Centroid|
  └─────┴─────┴───────┴───────┴──────┴─────────┴────────┘
Vector KV Key (interior, non-leaf partition):
  ┌────────────┬──────────────┬────────────┬─────┬──────────────────┬───────────┐
  │Index Prefix│Prefix Columns│PartitionKey│Level│Child PartitionKey│Family ID 0│
  └────────────┴──────────────┴────────────┴─────┴──────────────────┴───────────┘
Vector KV Key (leaf partition):
  ┌────────────┬──────────────┬────────────┬─────┬──────────┬───────────┐
  │Index Prefix│Prefix Columns│PartitionKey│Level│PrimaryKey│Family ID 0│
  └────────────┴──────────────┴────────────┴─────┴──────────┴───────────┘
Vector KV Value:
  ┌────────────────────────┬────────────────────────┐
  │Quantized+Encoded Vector│Composite+Stored Columns│
  └────────────────────────┴────────────────────────┘
*/

// EncodeMetadataKey constructs the KV key for the metadata record in a
// partition. All vector keys in the partition sort after it.
func EncodeMetadataKey(
	indexPrefix []byte, encodedPrefixCols []byte, partitionKey cspann.PartitionKey,
) roachpb.Key {
	capacity := len(indexPrefix) + len(encodedPrefixCols) + EncodedPartitionKeyLen(partitionKey) + 1
	keyBuffer := make([]byte, 0, capacity)
	keyBuffer = append(keyBuffer, indexPrefix...)
	keyBuffer = append(keyBuffer, encodedPrefixCols...)
	keyBuffer = EncodePartitionKey(keyBuffer, partitionKey)
	keyBuffer = EncodePartitionLevel(keyBuffer, cspann.InvalidLevel)
	return keys.MakeFamilyKey(keyBuffer, 0)
}

// EncodeStartVectorKey constructs the KV key that precedes all the KV keys for
// vector data records in the partition.
func EncodeStartVectorKey(metadataKey roachpb.Key) roachpb.Key {
	// The last two bytes of the metadata key are the Level (always 0) and the
	// Family ID (always 0). Chop the Family ID value and increment the Level in
	// order to get the start key.
	n := len(metadataKey) - 1
	keyBuffer := make(roachpb.Key, n)
	copy(keyBuffer, metadataKey[:n])
	keyBuffer[n-1]++
	return keyBuffer
}

// EncodeEndVectorKey constructs the KV key that succeeds all the KV keys for
// vector data records in the partition.
func EncodeEndVectorKey(metadataKey roachpb.Key) roachpb.Key {
	// Chop the last two bytes, which are the Level (always 0) and the Family ID
	// (always 0).
	n := len(metadataKey) - 2
	return metadataKey[:n:n].PrefixEnd()
}

// EncodePrefixVectorKey constructs the prefix that is shared by all KV keys for
// vector data records in the partition.
func EncodePrefixVectorKey(metadataKey roachpb.Key, level cspann.Level) roachpb.Key {
	// Chop the last two bytes, which are the Level (always 0) and the Family ID
	// (always 0).
	n := len(metadataKey) - 2
	return EncodePartitionLevel(metadataKey[:n:n], level)
}

// EncodedPrefixVectorKeyLen returns the number of bytes needed to encode the
// prefix for vector data records in the partition.
func EncodedPrefixVectorKeyLen(metadataKey roachpb.Key, level cspann.Level) int {
	return len(metadataKey) - 2 + EncodedPartitionLevelLen(level)
}

// DecodedVectorKey is a deconstructed key value, as described above, minus the
// index prefix. Any suffix bytes (child partition, primary key) are left in the
// Suffix value.
type DecodedVectorKey struct {
	Prefix       []byte
	PartitionKey cspann.PartitionKey
	Level        cspann.Level
	Suffix       []byte
}

// DecodeVectorKey takes an encoded key value, minus the /Tenant/Table/Index prefix,
// and extracts the vector index specific portions of it in an VectorIndexKey
// struct.
func DecodeVectorKey(
	keyBytes []byte, numPrefixColumns int,
) (vecIndexKey DecodedVectorKey, err error) {
	prefixLen := 0
	for range numPrefixColumns {
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

// Encode takes a VectorKey and turns it back into encoded bytes that can be
// appended to the index's prefix to form a key value.
func (vik *DecodedVectorKey) Encode(appendTo []byte) []byte {
	appendTo = append(appendTo, vik.Prefix...)
	appendTo = EncodePartitionKey(appendTo, vik.PartitionKey)
	appendTo = EncodePartitionLevel(appendTo, vik.Level)
	return append(appendTo, vik.Suffix...)
}

// EncodeVectorValue takes a quantized vector entry and any composite key data
// and returns the byte slice encoding the value of the vector index entry. This
// value will still need to be further encoded as Bytes in valueside.Value.
func EncodeVectorValue(appendTo []byte, vectorData []byte, compositeData []byte) []byte {
	// The value is encoded as a concatenation of the vector data and the
	// composite data.
	appendTo = append(appendTo, vectorData...)
	return append(appendTo, compositeData...)
}

// EncodedVectorValueLen returns the number of bytes needed to encode the value
// of a vector index entry.
func EncodedVectorValueLen(vectorData []byte, compositeData []byte) int {
	return len(vectorData) + len(compositeData)
}

// EncodeMetadataValue encodes the metadata KV value for a partition.
func EncodeMetadataValue(metadata cspann.PartitionMetadata) []byte {
	// The encoding consists of:
	// - 4 bytes for the level
	// - 4 bytes for the state
	// - 8 bytes for first target partition key (0 if InvalidKey)
	// - 8 bytes for second target partition key (0 if InvalidKey)
	// - 8 bytes for source partition key (0 if InvalidKey)
	// - 0-20 bytes for timestamp (variable encoding)
	// - 4 bytes count of dimensions
	// - 4 bytes for each dimension in the vector
	encMetadataSize := 4 + 8 + 8 + 8 + binary.MaxVarintLen64*2 + 4 + 4*len(metadata.Centroid)
	buf := make([]byte, 0, encMetadataSize)
	buf = encoding.EncodeUint32Ascending(buf, uint32(metadata.Level))
	buf = encoding.EncodeUint32Ascending(buf, uint32(metadata.StateDetails.State))
	buf = encoding.EncodeUint64Ascending(buf, uint64(metadata.StateDetails.Target1))
	buf = encoding.EncodeUint64Ascending(buf, uint64(metadata.StateDetails.Target2))
	buf = encoding.EncodeUint64Ascending(buf, uint64(metadata.StateDetails.Source))
	buf = encoding.EncodeUntaggedTimeValue(buf, metadata.StateDetails.Timestamp)

	// vector.Encode never returns a non-nil error, so suppress return value.
	buf, _ = vector.Encode(buf, metadata.Centroid)
	return buf
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

// EncodeUnquantizerVector encodes an Unquantizer vector into the given byte
// slice.
func EncodeUnquantizerVector(appendTo []byte, v vector.T) ([]byte, error) {
	// For backwards compatibility, encode a zero float32. Previously, the
	// distance of the vector to the centroid was encoded, but that is no longer
	// necessary.
	appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, 0)
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
		// The primary key is already in encoded form. That encoded form always
		// already contains the final family ID 0 value.
		return append(appendTo, key.KeyBytes...)
	}

	// Encode the partition key, along with the final family ID 0 value.
	appendTo = EncodePartitionKey(appendTo, key.PartitionKey)
	return keys.MakeFamilyKey(appendTo, 0)
}

// DecodeMetadataValue decodes the metadata KV value for a partition.
func DecodeMetadataValue(encMetadata []byte) (metadata cspann.PartitionMetadata, err error) {
	encMetadata, decodedLevel, err := encoding.DecodeUint32Ascending(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	encMetadata, decodedState, err := encoding.DecodeUint32Ascending(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	encMetadata, decodedTarget1, err := encoding.DecodeUint64Ascending(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	encMetadata, decodedTarget2, err := encoding.DecodeUint64Ascending(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	encMetadata, decodedSource, err := encoding.DecodeUint64Ascending(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	encMetadata, decodedTime, err := encoding.DecodeUntaggedTimeValue(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	_, centroid, err := vector.Decode(encMetadata)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	return cspann.PartitionMetadata{
		Level:    cspann.Level(decodedLevel),
		Centroid: centroid,
		StateDetails: cspann.PartitionStateDetails{
			State:     cspann.PartitionState(decodedState),
			Target1:   cspann.PartitionKey(decodedTarget1),
			Target2:   cspann.PartitionKey(decodedTarget2),
			Source:    cspann.PartitionKey(decodedSource),
			Timestamp: decodedTime,
		},
	}, nil
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
	// TODO(andyk): Also decode CentroidDotProducts once we support other distance
	// metrics.
	vectorSet.CodeCounts = append(vectorSet.CodeCounts, codeCount)
	vectorSet.CentroidDistances = append(vectorSet.CentroidDistances, centroidDistance)
	vectorSet.QuantizedDotProducts = append(vectorSet.QuantizedDotProducts, dotProduct)
	vectorSet.Codes.Data = slices.Grow(vectorSet.Codes.Data, vectorSet.Codes.Width)
	for range vectorSet.Codes.Width {
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

// DecodeUnquantizerVectorToSet decodes an Unquantizer vector entry into the
// given UnQuantizedVectorSet. The vector set must have been initialized with
// the correct number of dimensions. It returns the remainder of the input
// buffer.
func DecodeUnquantizerVectorToSet(
	encVector []byte, vectorSet *quantize.UnQuantizedVectorSet,
) ([]byte, error) {
	// Skip past the centroid distance, which was encoded as a 4-byte float32
	// value in a previous version.
	encVector = encVector[4:]
	encVector, v, err := vector.Decode(encVector)
	if err != nil {
		return nil, err
	}
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
		// Non-leaf vectors point to the partition key. Note that the Family ID
		// value at the end of the encoding is ignored.
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
