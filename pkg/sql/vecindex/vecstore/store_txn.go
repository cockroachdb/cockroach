// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/veclib"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// storeTxn provides a context to make transactional changes to a vector index.
// Calling methods here will use the wrapped KV Txn to update the vector index's
// internal data. Committing changes is the responsibility of the caller.
type storeTxn struct {
	workspace *veclib.Workspace
	kv        *kv.Txn
	store     *Store

	// Locking durability required by transaction isolation level.
	lockDurability kvpb.KeyLockingDurabilityType

	// Quantizer specific encoding and decoding for the root partition and everyone
	// else.
	rootCodec storeCodec
	codec     storeCodec

	// Retained allocations to prevent excessive reallocation.
	tmpChildKeys  []cspann.ChildKey
	tmpValueBytes []cspann.ValueBytes
	tmpSpans      []roachpb.Span
	tmpSpanIDs    []int
}

var _ cspann.Txn = (*storeTxn)(nil)

// storeCodec abstracts quantizer specific encode/decode operations from the
// rest of the store.
type storeCodec struct {
	quantizer    quantize.Quantizer
	tmpVectorSet quantize.QuantizedVectorSet
}

// newStoreCodec creates a new StoreCodec wrapping the provided quantizer.
func newStoreCodec(quantizer quantize.Quantizer) storeCodec {
	return storeCodec{
		quantizer: quantizer,
	}
}

// clear resets the codec's internal vector set to start a new encode / decode
// operation.
func (cs *storeCodec) clear(minCapacity int, centroid vector.T) {
	if cs.tmpVectorSet == nil {
		cs.tmpVectorSet = cs.quantizer.NewQuantizedVectorSet(minCapacity, centroid)
	} else {
		cs.tmpVectorSet.Clear(centroid)
	}
}

// getVectorSet returns the internal vector set cache. These will be invalidated
// when the clear method is called.
func (cs *storeCodec) getVectorSet() quantize.QuantizedVectorSet {
	return cs.tmpVectorSet
}

// decodeVector decodes a single vector to the codec's internal vector set. It
// returns the remainder of the input buffer.
func (cs *storeCodec) decodeVector(encodedVector []byte) ([]byte, error) {
	switch cs.quantizer.(type) {
	case *quantize.UnQuantizer:
		return DecodeUnquantizedVectorToSet(encodedVector, cs.tmpVectorSet.(*quantize.UnQuantizedVectorSet))
	case *quantize.RaBitQuantizer:
		return DecodeRaBitQVectorToSet(encodedVector, cs.tmpVectorSet.(*quantize.RaBitQuantizedVectorSet))
	}
	return nil, errors.Errorf("unknown quantizer type %T", cs.quantizer)
}

// encodeVector encodes a single vector. This method invalidates the internal
// vector set.
func (cs *storeCodec) encodeVector(
	w *veclib.Workspace, v vector.T, centroid vector.T,
) ([]byte, error) {
	cs.clear(1, centroid)
	input := v.AsSet()
	cs.quantizer.QuantizeInSet(w, cs.tmpVectorSet, input)
	return cs.encodeVectorFromSet(cs.tmpVectorSet, 0 /* idx */)
}

// encodeVectorFromSet encodes the vector indicated by 'idx' from an external
// vector set.
func (sc *storeCodec) encodeVectorFromSet(vs quantize.QuantizedVectorSet, idx int) ([]byte, error) {
	switch sc.quantizer.(type) {
	case *quantize.UnQuantizer:
		dist := vs.GetCentroidDistances()
		return EncodeUnquantizedVector([]byte{}, dist[idx], vs.(*quantize.UnQuantizedVectorSet).Vectors.At(idx))
	case *quantize.RaBitQuantizer:
		dist := sc.tmpVectorSet.GetCentroidDistances()
		qs := sc.tmpVectorSet.(*quantize.RaBitQuantizedVectorSet)
		return EncodeRaBitQVector(
			[]byte{},
			qs.CodeCounts[idx],
			dist[idx],
			qs.DotProducts[idx],
			qs.Codes.At(idx),
		), nil
	}
	return nil, errors.Errorf("unknown quantizer type %T", sc.quantizer)
}

// newTxn wraps a Store transaction around a kv transaction for use with the
// cspann.Store API.
func newTxn(w *veclib.Workspace, store *Store, kv *kv.Txn) *storeTxn {
	tx := storeTxn{
		workspace: w,
		kv:        kv,
		store:     store,
		codec:     newStoreCodec(store.quantizer),
		rootCodec: newStoreCodec(store.rootQuantizer),
	}
	// TODO (mw5h): This doesn't take into account session variables that control
	// lock durability. This doesn't matter for partition maintenance operations that
	// don't have a session, but may lead to unexpected behavior for CRUD operations.
	// The logic for determining what to do there is in optBuilder, so there may be
	// some plumbing involved to get it down here.
	if kv.IsoLevel() == isolation.Serializable {
		tx.lockDurability = kvpb.BestEffort
	} else {
		tx.lockDurability = kvpb.GuaranteedDurability
	}

	return &tx
}

// getCodecForPartitionKey returns the correct codec to use for interacting with
// the partition indicated. This will be the unquantized codec for the root
// partition, the codec for the quantizer indicated when the store was created
// otherwise.
func (tx *storeTxn) getCodecForPartitionKey(partitionKey cspann.PartitionKey) *storeCodec {
	// We always store the full sized vectors in the root partition
	if partitionKey == cspann.RootKey {
		return &tx.rootCodec
	} else {
		return &tx.codec
	}
}

// decodePartition decodes the KV row set into an ephemeral partition. This
// partition will become invalid when the codec is next reset, so it needs to be
// cloned if it will be used outside of the store.
func (tx *storeTxn) decodePartition(
	codec *storeCodec, result *kv.Result,
) (*cspann.Partition, error) {
	if result.Err != nil {
		return nil, result.Err
	}
	if len(result.Rows) == 0 {
		return nil, cspann.ErrPartitionNotFound
	}

	// Partition metadata is stored in /Prefix/PartitionID, with vector data
	// following in /Prefix/PartitionID/ChildKey.
	level, centroid, err := DecodePartitionMetadata(result.Rows[0].ValueBytes())
	if err != nil {
		return nil, err
	}
	metaKeyLen := len(result.Rows[0].Key)
	vectorEntries := result.Rows[1:]

	// Clear and ensure storage for the vector entries, child keys, and value
	// bytes.
	codec.clear(len(vectorEntries), centroid)
	if cap(tx.tmpChildKeys) < len(vectorEntries) {
		tx.tmpChildKeys = make([]cspann.ChildKey, len(vectorEntries))
	}
	tx.tmpChildKeys = tx.tmpChildKeys[:len(vectorEntries)]
	if cap(tx.tmpValueBytes) < len(vectorEntries) {
		tx.tmpValueBytes = make([]cspann.ValueBytes, len(vectorEntries))
	}
	tx.tmpValueBytes = tx.tmpValueBytes[:len(vectorEntries)]

	for i, entry := range vectorEntries {
		childKey, err := DecodeChildKey(entry.Key[metaKeyLen:], level)
		if err != nil {
			return nil, err
		}
		tx.tmpChildKeys[i] = childKey

		tx.tmpValueBytes[i], err = codec.decodeVector(entry.ValueBytes())
		if err != nil {
			return nil, err
		}
	}

	return cspann.NewPartition(
		codec.quantizer, codec.getVectorSet(), tx.tmpChildKeys, tx.tmpValueBytes, level), nil
}

// GetPartition is part of the vecstore.Txn interface. Read the partition
// indicated by `partitionKey` and build a Partition data structure, which is
// returned.
func (tx *storeTxn) GetPartition(
	ctx context.Context, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	b := tx.kv.NewBatch()

	startKey := tx.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	// GetPartition is used by fixup to split and merge partitions, so we want to
	// block concurrent writes.
	b.ScanForUpdate(startKey, endKey, tx.lockDurability)
	err := tx.kv.Run(ctx, b)
	if err != nil {
		return nil, err
	}

	codec := tx.getCodecForPartitionKey(partitionKey)
	partition, err := tx.decodePartition(codec, &b.Results[0])
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
func (tx *storeTxn) insertPartition(
	ctx context.Context, partitionKey cspann.PartitionKey, partition *cspann.Partition,
) error {
	b := tx.kv.NewBatch()

	key := tx.encodePartitionKey(partitionKey)
	meta, err := EncodePartitionMetadata(partition.Level(), partition.QuantizedSet().GetCentroid())
	if err != nil {
		return err
	}
	b.Put(key, meta)

	codec := tx.getCodecForPartitionKey(partitionKey)
	childKeys := partition.ChildKeys()
	valueBytes := partition.ValueBytes()
	for i := 0; i < partition.QuantizedSet().GetCount(); i++ {
		// The child key gets appended to 'key' here.
		// Cap the metadata key so that the append allocates a new slice for the child key.
		key = key[:len(key):len(key)]
		k := EncodeChildKey(key, childKeys[i])
		encodedValue, err := codec.encodeVectorFromSet(partition.QuantizedSet(), i)
		if err != nil {
			return err
		}
		encodedValue = append(encodedValue, valueBytes[i]...)
		b.Put(k, encodedValue)
	}

	return tx.kv.Run(ctx, b)
}

// SetRootPartition implements the vecstore.Txn interface.
func (tx *storeTxn) SetRootPartition(ctx context.Context, partition *cspann.Partition) error {
	if err := tx.DeletePartition(ctx, cspann.RootKey); err != nil {
		return err
	}
	return tx.insertPartition(ctx, cspann.RootKey, partition)
}

// InsertPartition implements the vecstore.Txn interface.
func (tx *storeTxn) InsertPartition(
	ctx context.Context, partition *cspann.Partition,
) (cspann.PartitionKey, error) {
	instanceID := tx.store.db.KV().Context().NodeID.SQLInstanceID()
	partitionID := cspann.PartitionKey(unique.GenerateUniqueInt(unique.ProcessUniqueID(instanceID)))
	return partitionID, tx.insertPartition(ctx, partitionID, partition)
}

// DeletePartition implements the vecstore.Txn interface.
func (tx *storeTxn) DeletePartition(ctx context.Context, partitionKey cspann.PartitionKey) error {
	b := tx.kv.NewBatch()

	startKey := tx.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	b.DelRange(startKey, endKey, false /* returnKeys */)
	return tx.kv.Run(ctx, b)
}

// GetPartitionMetadata implements the vecstore.Txn interface.
func (tx *storeTxn) GetPartitionMetadata(
	ctx context.Context, partitionKey cspann.PartitionKey, forUpdate bool,
) (cspann.PartitionMetadata, error) {
	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := tx.kv.NewBatch()

	metadataKey := tx.encodePartitionKey(partitionKey)
	if forUpdate {
		// By acquiring a shared lock on metadata key, we prevent splits/merges of
		// this partition from conflicting with the add operation.
		b.GetForShare(metadataKey, tx.lockDurability)
	} else {
		b.Get(metadataKey)
	}

	// This scan is purely for returning partition cardinality.
	b.Scan(metadataKey, metadataKey.PrefixEnd())

	// Run the batch and extract the partition metadata from results.
	if err := tx.kv.Run(ctx, b); err != nil {
		return cspann.PartitionMetadata{},
			errors.Wrapf(err, "getting partition metadata for %d", partitionKey)
	}
	metadata, err := getMetadataFromKVResult(&b.Results[0])
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	metadata.Count = len(b.Results[1].Rows) - 1
	return metadata, nil
}

// AddToPartition implements the vecstore.Txn interface.
func (tx *storeTxn) AddToPartition(
	ctx context.Context,
	partitionKey cspann.PartitionKey,
	vec vector.T,
	childKey cspann.ChildKey,
	valueBytes cspann.ValueBytes,
) (cspann.PartitionMetadata, error) {
	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := tx.kv.NewBatch()

	metadataKey := tx.encodePartitionKey(partitionKey)

	// By acquiring a shared lock on metadata key, we prevent splits/merges of
	// this partition from conflicting with the add operation.
	b.GetForShare(metadataKey, tx.lockDurability)

	// Run the batch and extract the partition metadata from results.
	err := tx.kv.Run(ctx, b)
	if err != nil {
		return cspann.PartitionMetadata{},
			errors.Wrapf(err, "locking partition %d for add", partitionKey)
	}
	metadata, err := getMetadataFromKVResult(&b.Results[0])
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// Cap the metadata key so that the append allocates a new slice for the
	// child key.
	prefix := metadataKey[:len(metadataKey):len(metadataKey)]
	entryKey := EncodeChildKey(prefix, childKey)

	b = tx.kv.NewBatch()

	// Add the Put command to the batch.
	codec := tx.getCodecForPartitionKey(partitionKey)
	encodedValue, err := codec.encodeVector(tx.workspace, vec, metadata.Centroid)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	encodedValue = append(encodedValue, valueBytes...)
	b.Put(entryKey, encodedValue)

	// This scan is purely for returning partition cardinality.
	startKey := tx.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()
	b.Scan(startKey, endKey)

	// Run the batch and set the metadata Count field from the results.
	if err = tx.kv.Run(ctx, b); err != nil {
		return cspann.PartitionMetadata{},
			errors.Wrapf(err, "adding vector to partition %d", partitionKey)
	}
	metadata.Count = len(b.Results[1].Rows) - 1
	return metadata, nil
}

// RemoveFromPartition implements the vecstore.Txn interface.
func (tx *storeTxn) RemoveFromPartition(
	ctx context.Context, partitionKey cspann.PartitionKey, childKey cspann.ChildKey,
) (cspann.PartitionMetadata, error) {
	b := tx.kv.NewBatch()

	// Lock metadata for partition in shared mode to block concurrent fixups.
	metadataKey := tx.encodePartitionKey(partitionKey)
	b.GetForShare(metadataKey, tx.lockDurability)

	// Cap the metadata key so that the append allocates a new slice for the child key.
	prefix := metadataKey[:len(metadataKey):len(metadataKey)]
	entryKey := EncodeChildKey(prefix, childKey)
	b.Del(entryKey)

	// Scan to get current cardinality.
	startKey := tx.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()
	b.Scan(startKey, endKey)

	if err := tx.kv.Run(ctx, b); err != nil {
		return cspann.PartitionMetadata{}, err
	}
	if len(b.Results[0].Rows) == 0 {
		return cspann.PartitionMetadata{}, cspann.ErrPartitionNotFound
	}
	// We ignore key not found for the deleted child.

	// Extract the partition metadata from the results.
	metadata, err := getMetadataFromKVResult(&b.Results[0])
	if err != nil {
		return cspann.PartitionMetadata{},
			errors.Wrapf(err, "removing vector from partition %d", partitionKey)
	}
	metadata.Count = len(b.Results[2].Rows) - 1
	return metadata, nil
}

// SearchPartitions implements the vecstore.Txn interface.
func (tx *storeTxn) SearchPartitions(
	ctx context.Context,
	partitionKeys []cspann.PartitionKey,
	queryVector vector.T,
	searchSet *cspann.SearchSet,
	partitionCounts []int,
) (cspann.Level, error) {
	b := tx.kv.NewBatch()

	for _, pk := range partitionKeys {
		startKey := tx.encodePartitionKey(pk)
		endKey := startKey.PrefixEnd()
		b.Scan(startKey, endKey)
	}

	if err := tx.kv.Run(ctx, b); err != nil {
		return cspann.InvalidLevel, err
	}

	level := cspann.InvalidLevel
	codec := tx.getCodecForPartitionKey(partitionKeys[0])
	for i, result := range b.Results {
		partition, err := tx.decodePartition(codec, &result)
		if err != nil {
			return cspann.InvalidLevel, err
		}
		searchLevel, partitionCount := partition.Search(
			tx.workspace, partitionKeys[i], queryVector, searchSet)
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

// getFullVectorsFromPK fills in refs that are specified by primary key. Refs
// that specify a partition ID are ignored. The values are returned in-line in
// the refs slice.
func (tx *storeTxn) getFullVectorsFromPK(
	ctx context.Context, refs []cspann.VectorWithKey, numPKLookups int,
) (err error) {
	if cap(tx.tmpSpans) >= numPKLookups {
		tx.tmpSpans = tx.tmpSpans[:0]
		tx.tmpSpanIDs = tx.tmpSpanIDs[:0]
	} else {
		tx.tmpSpans = make([]roachpb.Span, 0, numPKLookups)
		tx.tmpSpanIDs = make([]int, 0, numPKLookups)
	}

	for refIdx, ref := range refs {
		if ref.Key.PartitionKey != cspann.InvalidKey {
			continue
		}

		key := make(roachpb.Key, len(tx.store.pkPrefix)+len(ref.Key.KeyBytes))
		copy(key, tx.store.pkPrefix)
		copy(key[len(tx.store.pkPrefix):], ref.Key.KeyBytes)
		tx.tmpSpans = append(tx.tmpSpans, roachpb.Span{Key: key})
		tx.tmpSpanIDs = append(tx.tmpSpanIDs, refIdx)
	}

	if len(tx.tmpSpans) > 0 {
		var fetcher row.Fetcher
		var alloc tree.DatumAlloc
		err = fetcher.Init(ctx, row.FetcherInitArgs{
			Txn:             tx.kv,
			Alloc:           &alloc,
			Spec:            &tx.store.fetchSpec,
			SpansCanOverlap: true,
		})
		if err != nil {
			return err
		}
		defer fetcher.Close(ctx)

		err = fetcher.StartScan(
			ctx,
			tx.tmpSpans,
			tx.tmpSpanIDs,
			rowinfra.GetDefaultBatchBytesLimit(false /* forceProductionValue */),
			rowinfra.RowLimit(len(tx.tmpSpans)),
		)
		if err != nil {
			return err
		}

		var data [1]tree.Datum
		for {
			ok, refIdx, err := fetcher.NextRowDecodedInto(ctx, data[0:], tx.store.colIdxMap)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if v, ok := tree.AsDPGVector(data[0]); ok {
				refs[refIdx].Vector = v.T
			}
		}
	}
	return err
}

// getFullVectorsFromPartitionMetadata traverses the refs list and fills in refs
// specified by partition ID. Primary key references are ignored.
func (tx *storeTxn) getFullVectorsFromPartitionMetadata(
	ctx context.Context, refs []cspann.VectorWithKey,
) (numPKLookups int, err error) {
	b := tx.kv.NewBatch()

	for _, ref := range refs {
		if ref.Key.PartitionKey == cspann.InvalidKey {
			numPKLookups++
			continue
		}
		key := tx.encodePartitionKey(ref.Key.PartitionKey)
		b.Get(key)
	}

	if numPKLookups == len(refs) {
		return numPKLookups, nil
	}
	if err := tx.kv.Run(ctx, b); err != nil {
		return 0, err
	}

	idx := 0
	for _, result := range b.Results {
		if len(result.Rows) == 0 {
			return 0, cspann.ErrPartitionNotFound
		}
		_, centroid, err := DecodePartitionMetadata(result.Rows[0].ValueBytes())
		if err != nil {
			return 0, err
		}
		for ; refs[idx].Key.PartitionKey == cspann.InvalidKey; idx++ {
		}
		refs[idx].Vector = centroid
		idx++
	}
	return numPKLookups, nil
}

// GetFullVectors implements the Store interface.
func (tx *storeTxn) GetFullVectors(ctx context.Context, refs []cspann.VectorWithKey) error {
	numPKLookups, err := tx.getFullVectorsFromPartitionMetadata(ctx, refs)
	if err != nil {
		return err
	}
	if numPKLookups > 0 {
		err = tx.getFullVectorsFromPK(ctx, refs, numPKLookups)
	}
	return err
}

// encodePartitionKey takes a partition key and creates a KV key to read that
// partition's metadata. Vector data can be read by scanning from the metadata to
// the next partition's metadata.
func (tx *storeTxn) encodePartitionKey(partitionKey cspann.PartitionKey) roachpb.Key {
	keyBuffer := make(roachpb.Key, len(tx.store.prefix)+EncodedPartitionKeyLen(partitionKey))
	copy(keyBuffer, tx.store.prefix)
	keyBuffer = EncodePartitionKey(keyBuffer, partitionKey)
	return keyBuffer
}

// getMetadataFromKVResult extracts the K-means tree level and centroid from the
// results of a query of the partition metadata record.
func getMetadataFromKVResult(result *kv.Result) (cspann.PartitionMetadata, error) {
	if len(result.Rows) == 0 {
		return cspann.PartitionMetadata{}, cspann.ErrPartitionNotFound
	}
	level, centroid, err := DecodePartitionMetadata(result.Rows[0].ValueBytes())
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	return cspann.PartitionMetadata{
		Level:    level,
		Centroid: centroid,
		Count:    len(result.Rows) - 1,
	}, nil
}
