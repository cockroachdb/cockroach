// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// storeCodec abstracts quantizer specific encode/decode operations from the
// rest of the store.
type storeCodec struct {
	quantizer    quantize.Quantizer
	tmpVectorSet quantize.QuantizedVectorSet
}

// makeStoreCodec creates a new StoreCodec wrapping the provided quantizer.
func makeStoreCodec(quantizer quantize.Quantizer) storeCodec {
	return storeCodec{
		quantizer: quantizer,
	}
}

// Init sets up the codec's state for an encode/decode operation for vectors in
// a partition with the given centroid. It reuses temporary memory where
// possible.
func (sc *storeCodec) Init(centroid vector.T, minCapacity int) {
	if sc.tmpVectorSet == nil {
		sc.tmpVectorSet = sc.quantizer.NewQuantizedVectorSet(minCapacity, centroid)
	} else {
		sc.tmpVectorSet.Clear(centroid)
	}
}

// GetVectorSet returns the internal vector set cache.
// NOTE: This will be invalidated when the Init or EncodeVector method is
// called.
func (sc *storeCodec) GetVectorSet() quantize.QuantizedVectorSet {
	return sc.tmpVectorSet
}

// DecodeVector decodes a single vector to the codec's internal vector set. It
// returns the remainder of the input buffer.
func (sc *storeCodec) DecodeVector(encodedVector []byte) ([]byte, error) {
	switch sc.quantizer.(type) {
	case *quantize.UnQuantizer:
		return vecencoding.DecodeUnquantizerVectorToSet(
			encodedVector, sc.tmpVectorSet.(*quantize.UnQuantizedVectorSet))
	case *quantize.RaBitQuantizer:
		return vecencoding.DecodeRaBitQVectorToSet(
			encodedVector, sc.tmpVectorSet.(*quantize.RaBitQuantizedVectorSet))
	}
	return nil, errors.Errorf("unknown quantizer type %T", sc.quantizer)
}

// EncodeVector encodes a single vector and returns the encoded bytes.
// NOTE: This method invalidates the internal vector set.
func (sc *storeCodec) EncodeVector(w *workspace.T, v vector.T, centroid vector.T) ([]byte, error) {
	sc.Init(centroid, 1)
	input := v.AsSet()
	sc.quantizer.QuantizeInSet(w, sc.tmpVectorSet, input)
	return encodeVectorFromSet(sc.tmpVectorSet, 0 /* idx */)
}

// partitionCodec abstracts the encoding and decoding of partition data,
// including vectors, child keys, and value bytes. The rest of the store does
// not need to concern itself with which quantizer to use or how to efficiently
// reuse memory.
//
// To use for decoding, call InitForDecoding to set up needed context. Then,
// repeatedly call DecodeVectorData to decode partition data, followed by a call
// to GetPartition to construct the resulting Partition.
//
// To use for encoding, call EncodeVector.
//
// NOTE: Decoding and encoding operations reuse the same state and cannot be
// interleaved.
type partitionCodec struct {
	// workspace allocates temporary memory that is used when quantizing.
	workspace workspace.T
	// metadata is the metadata for the partition.
	metadata cspann.PartitionMetadata
	// rootCodec encodes and decodes for the root partition.
	rootCodec storeCodec
	// nonRootCodec encodes and decodes for non-root partitions.
	nonRootCodec storeCodec
	// codec points to rootCodec for the root partition, or nonRootCodec
	// otherwise.
	codec *storeCodec
	// tmpPartition is reused for partition storage after every call to Init.
	tmpPartition cspann.Partition
	// tmpChildKeys is reused for child keys after every call to Init.
	tmpChildKeys []cspann.ChildKey
	// tmpValueBytes is reused for value bytes after every call to Init.
	tmpValueBytes []cspann.ValueBytes
}

// makePartitionCodec initializes the codec with the quantizer used for the root
// partition and the quantizer used for all other partitions.
func makePartitionCodec(
	rootQuantizer quantize.Quantizer, quantizer quantize.Quantizer,
) partitionCodec {
	return partitionCodec{
		rootCodec:    makeStoreCodec(rootQuantizer),
		nonRootCodec: makeStoreCodec(quantizer),
	}
}

// EncodeVector encodes a single vector and returns the encoded bytes.
func (pc *partitionCodec) EncodeVector(
	partitionKey cspann.PartitionKey, v vector.T, centroid vector.T,
) ([]byte, error) {
	pc.setStoreCodec(partitionKey)
	return pc.codec.EncodeVector(&pc.workspace, v, centroid)
}

// InitForDecoding sets up the codec's state for decoding partition child keys,
// vectors, and value bytes. It reuses temporary memory where possible.
func (pc *partitionCodec) InitForDecoding(
	partitionKey cspann.PartitionKey, metadata cspann.PartitionMetadata, minCapacity int,
) {
	// Set up the right codec.
	pc.metadata = metadata
	pc.setStoreCodec(partitionKey)
	pc.codec.Init(metadata.Centroid, minCapacity)

	// Reuse temporary memory.
	if cap(pc.tmpChildKeys) < minCapacity {
		pc.tmpChildKeys = make([]cspann.ChildKey, 0, minCapacity)
	} else {
		pc.tmpChildKeys = pc.tmpChildKeys[:0]
	}
	if cap(pc.tmpValueBytes) < minCapacity {
		pc.tmpValueBytes = make([]cspann.ValueBytes, 0, minCapacity)
	} else {
		pc.tmpValueBytes = pc.tmpValueBytes[:0]
	}
}

// DecodePartitionData decodes the provided partition data and adds it to the
// internal partition data cache. After zero or more calls to this method, the
// caller can call GetPartition to get the resulting partition.
func (pc *partitionCodec) DecodePartitionData(encodedChildKey, encodedVector []byte) error {
	childKey, err := vecencoding.DecodeChildKey(encodedChildKey, pc.metadata.Level)
	if err != nil {
		return err
	}
	pc.tmpChildKeys = append(pc.tmpChildKeys, childKey)

	valueBytes, err := pc.codec.DecodeVector(encodedVector)
	if err != nil {
		return err
	}
	pc.tmpValueBytes = append(pc.tmpValueBytes, valueBytes)

	return nil
}

// GetPartition returns the internal partition instance that has been built via
// repeated calls to DecodePartitionData.
// NOTE: The returned partition is only valid until the next call to this codec.
func (pc *partitionCodec) GetPartition() *cspann.Partition {
	pc.tmpPartition.Init(
		pc.metadata, pc.codec.quantizer, pc.codec.GetVectorSet(), pc.tmpChildKeys, pc.tmpValueBytes)
	return &pc.tmpPartition
}

// setStoreCodec sets the storeCodec that can encode or decode for a partition
// with the given key.
func (pc *partitionCodec) setStoreCodec(partitionKey cspann.PartitionKey) {
	if partitionKey == cspann.RootKey {
		pc.codec = &pc.rootCodec
	} else {
		pc.codec = &pc.nonRootCodec
	}
}

// encodeVectorFromSet encodes the vector indicated by 'idx' from an external
// vector set.
func encodeVectorFromSet(vs quantize.QuantizedVectorSet, idx int) ([]byte, error) {
	switch t := vs.(type) {
	case *quantize.UnQuantizedVectorSet:
		return vecencoding.EncodeUnquantizerVector(
			[]byte{}, t.CentroidDistances[idx], t.Vectors.At(idx))
	case *quantize.RaBitQuantizedVectorSet:
		return vecencoding.EncodeRaBitQVector(
			[]byte{}, t.CodeCounts[idx], t.CentroidDistances[idx], t.QuantizedDotProducts[idx], t.Codes.At(idx),
		), nil
	}
	return nil, errors.Errorf("unknown quantizer type %T", vs)
}
