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

// persistentStoreTxn provides a context to make transactional changes to a
// vector index. Calling methods here will use the wrapped KV Txn to update the
// vector index's internal data. Committing changes is the responsibility of the
// caller.
type persistentStoreTxn struct {
	kv    *kv.Txn
	store *PersistentStore

	lockDurability kvpb.KeyLockingDurabilityType
	tmpChildKeys   []ChildKey

	codec     persistentStoreCodec
	rootCodec persistentStoreCodec
}

var _ Txn = (*persistentStoreTxn)(nil)

// persistentStoreCodec abstracts quantizer specific encode/decode operations
// from the rest of the persistent store.
type persistentStoreCodec struct {
	quantizer    quantize.Quantizer
	tmpVectorSet quantize.QuantizedVectorSet
}

// newPersistentStoreCodec creates a new PersistentStoreCodec wrapping the
// provided quantizer.
func newPersistentStoreCodec(quantizer quantize.Quantizer) persistentStoreCodec {
	return persistentStoreCodec{
		quantizer: quantizer,
	}
}

// clear resets the codec's internal vector set to start a new encode / decode
// operation.
func (psc *persistentStoreCodec) clear(minCapacity int, centroid vector.T) {
	if psc.tmpVectorSet == nil {
		psc.tmpVectorSet = psc.quantizer.NewQuantizedVectorSet(minCapacity, centroid)
	} else {
		psc.tmpVectorSet.Clear(centroid)
	}
}

// getVectorSet returns the internal vector set cache. These will be invalidated
// when the clear method is called.
func (psc *persistentStoreCodec) getVectorSet() quantize.QuantizedVectorSet {
	return psc.tmpVectorSet
}

// decodeVector decodes a single vector to the codec's internal vector set.
func (psc *persistentStoreCodec) decodeVector(encodedVector []byte) error {
	switch psc.quantizer.(type) {
	case *quantize.UnQuantizer:
		return DecodeUnquantizedVectorToSet(encodedVector, psc.tmpVectorSet.(*quantize.UnQuantizedVectorSet))
	case *quantize.RaBitQuantizer:
		return DecodeRaBitQVectorToSet(encodedVector, psc.tmpVectorSet.(*quantize.RaBitQuantizedVectorSet))
	}
	return errors.Errorf("unknown quantizer type %T", psc.quantizer)
}

// encodeVector encodes a single vector. This method invalidates the internal
// vector set.
func (psc *persistentStoreCodec) encodeVector(
	ctx context.Context, v vector.T, centroid vector.T,
) ([]byte, error) {
	psc.clear(1, centroid)
	input := v.AsSet()
	psc.quantizer.QuantizeInSet(ctx, psc.tmpVectorSet, &input)
	return psc.encodeVectorFromSet(psc.tmpVectorSet, 0 /* idx */)
}

// encodeVectorFromSet encodes the vector indicated by 'idx' from an external
// vector set.
func (psc *persistentStoreCodec) encodeVectorFromSet(
	vs quantize.QuantizedVectorSet, idx int,
) ([]byte, error) {
	switch psc.quantizer.(type) {
	case *quantize.UnQuantizer:
		dist := vs.GetCentroidDistances()
		return EncodeUnquantizedVector([]byte{}, dist[idx], vs.(*quantize.UnQuantizedVectorSet).Vectors.At(idx))
	case *quantize.RaBitQuantizer:
		dist := psc.tmpVectorSet.GetCentroidDistances()
		qs := psc.tmpVectorSet.(*quantize.RaBitQuantizedVectorSet)
		return EncodeRaBitQVector(
			[]byte{},
			qs.CodeCounts[idx],
			dist[idx],
			qs.DotProducts[idx],
			qs.Codes.At(idx),
		), nil
	}
	return nil, errors.Errorf("unknown quantizer type %T", psc.quantizer)
}

// NewPersistentStoreTxn wraps a PersistentStore transaction around a kv
// transaction for use with the vecstore API.
func NewPersistentStoreTxn(store *PersistentStore, kv *kv.Txn) *persistentStoreTxn {
	psTxn := persistentStoreTxn{
		kv:        kv,
		store:     store,
		codec:     newPersistentStoreCodec(store.quantizer),
		rootCodec: newPersistentStoreCodec(store.rootQuantizer),
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
func (psTxn *persistentStoreTxn) getCodecForPartitionKey(
	partitionKey PartitionKey,
) *persistentStoreCodec {
	// We always store the full sized vectors in the root partition
	if partitionKey == RootKey {
		return &psTxn.rootCodec
	} else {
		return &psTxn.codec
	}
}

// decodePartition decodes the KV row set into an ephemeral partition. This
// partition will become invalid when the codec is next reset, so it needs to be
// cloned if it will be used outside of the persistent store.
func (psTxn *persistentStoreTxn) decodePartition(
	codec *persistentStoreCodec, result *kv.Result,
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
	codec.clear(len(vectorEntries), centroid)
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

		if err = codec.decodeVector(entry.ValueBytes()); err != nil {
			return nil, err
		}
	}

	return NewPartition(codec.quantizer, codec.getVectorSet(), psTxn.tmpChildKeys, level), nil
}

// GetPartition is part of the vecstore.Txn interface. Read the partition
// indicated by `partitionKey` and build a Partition data structure, which is
// returned.
func (psTxn *persistentStoreTxn) GetPartition(
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
func (psTxn *persistentStoreTxn) insertPartition(
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
		// Cap the metadata key so that the append allocates a new slice for the child key.
		key = key[:len(key):len(key)]
		k := EncodeChildKey(key, childKeys[i])
		encodedVector, err := codec.encodeVectorFromSet(partition.QuantizedSet(), i)
		if err != nil {
			return err
		}
		b.Put(k, encodedVector)
	}

	return psTxn.kv.Run(ctx, b)
}

// SetRootPartition implements the vecstore.Txn interface.
func (psTxn *persistentStoreTxn) SetRootPartition(ctx context.Context, partition *Partition) error {
	if err := psTxn.DeletePartition(ctx, RootKey); err != nil {
		return err
	}
	return psTxn.insertPartition(ctx, RootKey, partition)
}

// InsertPartition implements the vecstore.Txn interface.
func (psTxn *persistentStoreTxn) InsertPartition(
	ctx context.Context, partition *Partition,
) (PartitionKey, error) {
	instanceID := psTxn.store.db.Context().NodeID.SQLInstanceID()
	partitionID := PartitionKey(builtins.GenerateUniqueInt(builtins.ProcessUniqueID(instanceID)))
	return partitionID, psTxn.insertPartition(ctx, partitionID, partition)
}

// DeletePartition implements the vecstore.Txn interface.
func (psTxn *persistentStoreTxn) DeletePartition(
	ctx context.Context, partitionKey PartitionKey,
) error {
	b := psTxn.kv.NewBatch()

	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	b.DelRange(startKey, endKey, false /* returnKeys */)
	return psTxn.kv.Run(ctx, b)
}

// AddToPartition implements the vecstore.Txn interface.
func (psTxn *persistentStoreTxn) AddToPartition(
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

	// Cap the metadata key so that the append allocates a new slice for the child key.
	prefix := metadataKey[:len(metadataKey):len(metadataKey)]
	entryKey := EncodeChildKey(prefix, childKey)

	codec := psTxn.getCodecForPartitionKey(partitionKey)
	b = psTxn.kv.NewBatch()
	encodedVector, err := codec.encodeVector(ctx, vector, centroid)
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
func (psTxn *persistentStoreTxn) RemoveFromPartition(
	ctx context.Context, partitionKey PartitionKey, childKey ChildKey,
) (int, error) {
	b := psTxn.kv.NewBatch()

	// Lock metadata for partition in shared mode to block concurrent fixups.
	metadataKey := psTxn.encodePartitionKey(partitionKey)
	b.GetForShare(metadataKey, psTxn.lockDurability)

	// Cap the metadata key so that the append allocates a new slice for the child key.
	prefix := metadataKey[:len(metadataKey):len(metadataKey)]
	entryKey := EncodeChildKey(prefix, childKey)
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
func (psTxn *persistentStoreTxn) SearchPartitions(
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

func (psTxn *persistentStoreTxn) GetFullVectors(ctx context.Context, refs []VectorWithKey) error {
	panic("GetFullVectors() unimplemented")
}

// encodePartitionKey takes a partition key and creates a KV key to read that
// partition's metadata. Vector data can be read by scanning from the metadata to
// the next partition's metadata.
func (psTxn *persistentStoreTxn) encodePartitionKey(partitionKey PartitionKey) roachpb.Key {
	keyBuffer := make(roachpb.Key, len(psTxn.store.prefix)+EncodedPartitionKeyLen(partitionKey))
	copy(keyBuffer, psTxn.store.prefix)
	keyBuffer = EncodePartitionKey(keyBuffer, partitionKey)
	return keyBuffer
}
