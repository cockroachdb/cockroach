// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// PersistentStoreTxn provides a context to make transactional changes to a
// vector index. Calling methods here will use the wrapped KV Txn to update the
// vector index's internal data. Committing changes is the responsibility of the
// caller.
type PersistentStoreTxn struct {
	kv    *kv.Txn
	store *PersistentStore

	lockDurability kvpb.KeyLockingDurabilityType

	rootCodec    *PersistentStoreCodec
	codec        *PersistentStoreCodec
	tmpChildKeys []ChildKey
}

var _ Txn = (*PersistentStoreTxn)(nil)

// PersistentStoreCodec abstracts quantizer-specific aspects of reading and writing vector sets.
type PersistentStoreCodec struct {
	quantizer quantize.Quantizer

	encoder func(quantize.QuantizedVectorSet, []byte, int) ([]byte, error)
	decoder func(quantize.QuantizedVectorSet, []byte) error

	tmpVectorSet quantize.QuantizedVectorSet
}

// NewPersistentStoreCodec creates a new PersistentStoreCodec wrapping the
// provided quantizer.
func NewPersistentStoreCodec(quantizer quantize.Quantizer) *PersistentStoreCodec {
	codec := PersistentStoreCodec{
		quantizer: quantizer,
	}
	switch quantizer.(type) {
	case *quantize.UnQuantizer:
		codec.encoder = func(vs quantize.QuantizedVectorSet, appendTo []byte, idx int) ([]byte, error) {
			dist := vs.GetCentroidDistances()
			return EncodeUnquantizedVector(appendTo, dist[idx], vs.(*quantize.UnQuantizedVectorSet).Vectors.At(idx))
		}
		codec.decoder = func(vs quantize.QuantizedVectorSet, encodedVector []byte) error {
			return DecodeUnquantizedVectorToSet(encodedVector, vs.(*quantize.UnQuantizedVectorSet))
		}
	case *quantize.RaBitQuantizer:
		codec.encoder = func(vectorSet quantize.QuantizedVectorSet, appendTo []byte, idx int) ([]byte, error) {
			dist := vectorSet.GetCentroidDistances()
			qs := vectorSet.(*quantize.RaBitQuantizedVectorSet)
			return EncodeRaBitQVector(
				appendTo,
				qs.CodeCounts[idx],
				dist[idx],
				qs.DotProducts[idx],
				qs.Codes.At(idx),
			), nil
		}
		codec.decoder = func(vs quantize.QuantizedVectorSet, encodedVector []byte) error {
			return DecodeRaBitQVectorToSet(encodedVector, vs.(*quantize.RaBitQuantizedVectorSet))
		}
	}

	return &codec
}

// Clear resets the codec's internal vector set to start a new encode / decode
// operation.
func (psc *PersistentStoreCodec) Clear(minCapacity int, centroid vector.T) {
	if psc.tmpVectorSet == nil {
		psc.tmpVectorSet = psc.quantizer.NewQuantizedVectorSet(minCapacity, centroid)
	} else {
		psc.tmpVectorSet.Clear(centroid)
	}
}

// GetVectorSet returns the internal vector set cache. These will be invalidated
// when the Clear method is called.
func (psc *PersistentStoreCodec) GetVectorSet() quantize.QuantizedVectorSet {
	return psc.tmpVectorSet
}

// DecodeVector decodes a single vector to the codec's internal vector set.
func (psc *PersistentStoreCodec) DecodeVector(encodedVector []byte) error {
	return psc.decoder(psc.tmpVectorSet, encodedVector)
}

// EncodeVector encodes a single vector. This method invalidates the internal
// vector set.
func (psc *PersistentStoreCodec) EncodeVector(
	ctx context.Context, v vector.T, centroid vector.T,
) ([]byte, error) {
	psc.Clear(1, centroid)
	input := v.AsSet()
	psc.quantizer.QuantizeInSet(ctx, psc.tmpVectorSet, &input)
	return psc.EncodeVectorFromSet(psc.tmpVectorSet, []byte{}, 0 /* idx */)
}

// EncodeVectorFromSet encodes the vector indicated by 'idx' from an external
// vector set.
func (psc *PersistentStoreCodec) EncodeVectorFromSet(
	vs quantize.QuantizedVectorSet, encodedBytes []byte, idx int,
) ([]byte, error) {
	return psc.encoder(vs, encodedBytes, idx)
}

// NewPersistentStoreTxn wraps a PersistentStore transaction around a kv
// transaction for use with the vecstore API.
func NewPersistentStoreTxn(store *PersistentStore, kv *kv.Txn) *PersistentStoreTxn {
	psTxn := PersistentStoreTxn{
		kv:        kv,
		store:     store,
		codec:     NewPersistentStoreCodec(store.quantizer),
		rootCodec: NewPersistentStoreCodec(store.rootQuantizer),
	}
	// TODO (mw5h): This doesn't take into account session variables that control
	// lock durability. This doesn't matter for partition maintenance operations that
	// don't have a session, but may lead to unexpected behavior for CRUD operations.
	// The logic for determining what to do there is in optBuilder, so there may be
	// some plumbing involved to get it down here.
	if kv.IsoLevel() == isolation.Serializable {
		psTxn.lockDurability = kvpb.BestEffort
	} else {
		psTxn.lockDurability = kvpb.GuaranteedDurability
	}

	return &psTxn
}

// getCodecForPartitionKey returns the correct codec to use for interacting with
// the partition indicated. This will be the unquantized codec for the root
// partition, the codec for the quantizer indicated when the store was created
// otherwise.
func (psTxn *PersistentStoreTxn) getCodecForPartitionKey(
	partitionKey PartitionKey,
) *PersistentStoreCodec {
	// We always store the full sized vectors in the root partition
	if partitionKey == RootKey {
		return psTxn.rootCodec
	} else {
		return psTxn.codec
	}
}

// decodePartition decodes the KV row set into an ephemeral partition. This
// partition will become invalid when the codec is next reset, so it needs to be
// cloned if it will be used outside of the persistent store.
func (psTxn *PersistentStoreTxn) decodePartition(
	codec *PersistentStoreCodec, result *kv.Result,
) (*Partition, error) {
	if result.Err != nil {
		return nil, result.Err
	}
	if len(result.Rows) == 0 {
		return nil, ErrPartitionNotFound
	}

	// Partition metadata is stored in /Prefix/PartitionID, with vector data
	// following in /Prefix/PartitionID/ChildKey.
	level, centroid, err := DecodePartitionMetadata(result.Rows[0].ValueBytes())
	if err != nil {
		return nil, err
	}
	metaKeyLen := len(result.Rows[0].Key)
	vectorEntries := result.Rows[1:]

	// Clear and ensure storage for the vector entries and child keys.
	codec.Clear(len(vectorEntries), centroid)
	if cap(psTxn.tmpChildKeys) < len(vectorEntries) {
		psTxn.tmpChildKeys = slices.Grow(psTxn.tmpChildKeys, len(vectorEntries)-cap(psTxn.tmpChildKeys))
	}
	psTxn.tmpChildKeys = psTxn.tmpChildKeys[:len(vectorEntries)]

	for i, entry := range vectorEntries {
		childKey, err := DecodeChildKey(entry.Key[metaKeyLen:], level)
		if err != nil {
			return nil, err
		}
		psTxn.tmpChildKeys[i] = childKey

		if err = codec.DecodeVector(entry.ValueBytes()); err != nil {
			return nil, err
		}
	}

	return NewPartition(codec.quantizer, codec.GetVectorSet(), psTxn.tmpChildKeys, level), nil
}

// GetPartition is part of the vecstore.Txn interface. Read the partition
// indicated by `partitionKey` and build a Partition data structure, which is
// returned.
func (psTxn *PersistentStoreTxn) GetPartition(
	ctx context.Context, partitionKey PartitionKey,
) (*Partition, error) {
	b := psTxn.kv.NewBatch()

	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	// GetPartition is used by fixup to split and merge partitions, so we want to
	// block concurrent writes.
	b.ScanForUpdate(startKey, endKey, psTxn.lockDurability)
	err := psTxn.kv.Run(ctx, b)
	if err != nil {
		return nil, err
	}

	codec := psTxn.getCodecForPartitionKey(partitionKey)
	partition, err := psTxn.decodePartition(codec, &b.Results[0])
	if err != nil {
		return nil, err
	}
	return partition.Clone(), nil
}

// Insert a partition with the given partition key into the store. If the
// partition already exists, the new partition's metadata will overwrite the
// existing metadata, but existing vectors will not be deleted. Vectors in the
// new partition will overwrite existing vectors if child keys collide, but
// otherwise the resulting partition will be a union of the two partitions.
func (psTxn *PersistentStoreTxn) insertPartition(
	ctx context.Context, partitionKey PartitionKey, partition *Partition,
) error {
	b := psTxn.kv.NewBatch()

	key := psTxn.encodePartitionKey(partitionKey)
	meta, err := EncodePartitionMetadata(partition.Level(), partition.quantizedSet.GetCentroid())
	if err != nil {
		return err
	}
	b.Put(key, meta)

	codec := psTxn.getCodecForPartitionKey(partitionKey)
	childKeys := partition.ChildKeys()
	for i := 0; i < partition.quantizedSet.GetCount(); i++ {
		// The child key gets appended to 'key' here.
		// TODO(mw5h): preallocate memory for keys and encoded vectors.
		k := make(roachpb.Key, len(key))
		copy(k, key)
		k = EncodeChildKey(k, childKeys[i])
		encodedVector, err := codec.EncodeVectorFromSet(partition.QuantizedSet(), []byte{}, i)
		if err != nil {
			return err
		}
		b.Put(k, encodedVector)
	}

	return psTxn.kv.Run(ctx, b)
}

// SetRootPartition implements the vecstore.Txn interface.
func (psTxn *PersistentStoreTxn) SetRootPartition(ctx context.Context, partition *Partition) error {
	if err := psTxn.DeletePartition(ctx, RootKey); err != nil {
		return err
	}
	return psTxn.insertPartition(ctx, RootKey, partition)
}

// InsertPartition implements the vecstore.Txn interface.
func (psTxn *PersistentStoreTxn) InsertPartition(
	ctx context.Context, partition *Partition,
) (PartitionKey, error) {
	instanceID := psTxn.store.db.Context().NodeID.SQLInstanceID()
	partitionID := PartitionKey(builtins.GenerateUniqueInt(builtins.ProcessUniqueID(instanceID)))
	return partitionID, psTxn.insertPartition(ctx, partitionID, partition)
}

// DeletePartition implements the vecstore.Txn interface.
func (psTxn *PersistentStoreTxn) DeletePartition(
	ctx context.Context, partitionKey PartitionKey,
) error {
	b := psTxn.kv.NewBatch()

	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	b.DelRange(startKey, endKey, false /* returnKeys */)
	return psTxn.kv.Run(ctx, b)
}

// AddToPartition implements the vecstore.Txn interface.
func (psTxn *PersistentStoreTxn) AddToPartition(
	ctx context.Context, partitionKey PartitionKey, vector vector.T, childKey ChildKey,
) (int, error) {
	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := psTxn.kv.NewBatch()

	metadataKey := psTxn.encodePartitionKey(partitionKey)

	b.GetForShare(metadataKey, psTxn.lockDurability)
	err := psTxn.kv.Run(ctx, b)
	if err != nil {
		return -1, err
	}
	if len(b.Results[0].Rows) == 0 {
		return -1, ErrPartitionNotFound
	}
	_, centroid, err := DecodePartitionMetadata(b.Results[0].Rows[0].ValueBytes())
	if err != nil {
		return -1, err
	}

	entryKey := make([]byte, len(metadataKey))
	copy(entryKey, metadataKey)
	entryKey = EncodeChildKey(entryKey, childKey)

	codec := psTxn.getCodecForPartitionKey(partitionKey)
	b = psTxn.kv.NewBatch()
	encodedVector, err := codec.EncodeVector(ctx, vector, centroid)
	if err != nil {
		return -1, err
	}
	b.Put(entryKey, encodedVector)

	// This scan is purely for returning partition cardinality.
	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()
	b.Scan(startKey, endKey)

	if err = psTxn.kv.Run(ctx, b); err != nil {
		return -1, err
	}
	return len(b.Results[1].Rows) - 1, nil
}

// RemoveFromPartition implements the vecstore.Txn interface.
func (psTxn *PersistentStoreTxn) RemoveFromPartition(
	ctx context.Context, partitionKey PartitionKey, childKey ChildKey,
) (int, error) {
	b := psTxn.kv.NewBatch()

	// Lock metadata for partition in shared mode to block concurrent fixups.
	metadataKey := psTxn.encodePartitionKey(partitionKey)
	b.GetForShare(metadataKey, psTxn.lockDurability)

	// Delete the child.
	entryKey := make([]byte, len(metadataKey))
	copy(entryKey, metadataKey)
	entryKey = EncodeChildKey(entryKey, childKey)
	b.Del(entryKey)

	// Scan to get current cardinality.
	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()
	b.Scan(startKey, endKey)

	if err := psTxn.kv.Run(ctx, b); err != nil {
		return -1, err
	}
	if len(b.Results[0].Rows) == 0 {
		return -1, ErrPartitionNotFound
	}
	// We ignore key not found for the deleted child.

	return len(b.Results[2].Rows) - 1, nil
}

// SearchPartitions implements the vecstore.Txn interface.
func (psTxn *PersistentStoreTxn) SearchPartitions(
	ctx context.Context,
	partitionKeys []PartitionKey,
	queryVector vector.T,
	searchSet *SearchSet,
	partitionCounts []int,
) (Level, error) {
	b := psTxn.kv.NewBatch()

	for _, pk := range partitionKeys {
		startKey := psTxn.encodePartitionKey(pk)
		endKey := startKey.PrefixEnd()
		b.Scan(startKey, endKey)
	}

	if err := psTxn.kv.Run(ctx, b); err != nil {
		return InvalidLevel, err
	}

	level := InvalidLevel
	codec := psTxn.getCodecForPartitionKey(partitionKeys[0])
	for i, result := range b.Results {
		partition, err := psTxn.decodePartition(codec, &result)
		if err != nil {
			return InvalidLevel, err
		}
		searchLevel, partitionCount := partition.Search(ctx, partitionKeys[i], queryVector, searchSet)
		if i == 0 {
			level = searchLevel
		} else if level != searchLevel {
			// Callers should only search for partitions at the same level.
			panic(errors.AssertionFailedf(
				"caller already searched a partition at level %d, cannot search at level %d",
				level, searchLevel))
		}
		partitionCounts[i] = partitionCount
	}

	return level, nil
}

func (psTxn *PersistentStoreTxn) GetFullVectors(ctx context.Context, refs []VectorWithKey) error {
	panic("GetFullVectors() unimplemented")
}

// encodePartitionKey takes a partition key and creates a KV key to read that
// partition's metadata. Vector data can be read by scanning from the metadata to
// the next partition's metadata.
func (psTxn *PersistentStoreTxn) encodePartitionKey(partitionKey PartitionKey) roachpb.Key {
	keyBuffer := make(roachpb.Key, len(psTxn.store.prefix)+EncodedPartitionKeyLen(partitionKey))
	copy(keyBuffer, psTxn.store.prefix)
	keyBuffer = EncodePartitionKey(keyBuffer, partitionKey)
	return keyBuffer
}
