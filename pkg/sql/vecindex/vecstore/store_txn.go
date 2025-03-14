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
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Txn provides a context to make transactional changes to a vector index.
// Calling methods here will use the wrapped KV Txn to update the vector index's
// internal data. Committing changes is the responsibility of the caller.
type Txn struct {
	kv    *kv.Txn
	store *Store

	// Locking durability required by transaction isolation level.
	lockDurability kvpb.KeyLockingDurabilityType

	// Quantizer specific encoding and decoding for the root partition and everyone
	// else.
	rootCodec storeCodec
	codec     storeCodec

	// Retained allocations to prevent excessive reallocation.
	workspace     workspace.T
	tmpChildKeys  []cspann.ChildKey
	tmpValueBytes []cspann.ValueBytes
	tmpSpans      []roachpb.Span
	tmpSpanIDs    []int
}

var _ cspann.Txn = (*Txn)(nil)

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

// clear resets the codec's internal vector set to start a new encode / decode
// operation.
func (sc *storeCodec) clear(minCapacity int, centroid vector.T) {
	if sc.tmpVectorSet == nil {
		sc.tmpVectorSet = sc.quantizer.NewQuantizedVectorSet(minCapacity, centroid)
	} else {
		sc.tmpVectorSet.Clear(centroid)
	}
}

// getVectorSet returns the internal vector set cache. These will be invalidated
// when the clear method is called.
func (sc *storeCodec) getVectorSet() quantize.QuantizedVectorSet {
	return sc.tmpVectorSet
}

// decodeVector decodes a single vector to the codec's internal vector set. It
// returns the remainder of the input buffer.
func (sc *storeCodec) decodeVector(encodedVector []byte) ([]byte, error) {
	switch sc.quantizer.(type) {
	case *quantize.UnQuantizer:
		return vecencoding.DecodeUnquantizedVectorToSet(
			encodedVector, sc.tmpVectorSet.(*quantize.UnQuantizedVectorSet))
	case *quantize.RaBitQuantizer:
		return vecencoding.DecodeRaBitQVectorToSet(
			encodedVector, sc.tmpVectorSet.(*quantize.RaBitQuantizedVectorSet))
	}
	return nil, errors.Errorf("unknown quantizer type %T", sc.quantizer)
}

// encodeVector encodes a single vector. This method invalidates the internal
// vector set.
func (sc *storeCodec) encodeVector(w *workspace.T, v vector.T, centroid vector.T) ([]byte, error) {
	sc.clear(1, centroid)
	input := v.AsSet()
	sc.quantizer.QuantizeInSet(w, sc.tmpVectorSet, input)
	return sc.encodeVectorFromSet(sc.tmpVectorSet, 0 /* idx */)
}

// encodeVectorFromSet encodes the vector indicated by 'idx' from an external
// vector set.
func (sc *storeCodec) encodeVectorFromSet(vs quantize.QuantizedVectorSet, idx int) ([]byte, error) {
	switch t := vs.(type) {
	case *quantize.UnQuantizedVectorSet:
		return vecencoding.EncodeUnquantizedVector(
			[]byte{}, t.CentroidDistances[idx], t.Vectors.At(idx))
	case *quantize.RaBitQuantizedVectorSet:
		return vecencoding.EncodeRaBitQVector(
			[]byte{},
			t.CodeCounts[idx],
			t.CentroidDistances[idx],
			t.DotProducts[idx],
			t.Codes.At(idx),
		), nil
	}
	return nil, errors.Errorf("unknown quantizer type %T", sc.quantizer)
}

// Init sets initial values for the transaction, wrapping it around a kv
// transaction for use with the cspann.Store API. The Init pattern is used
// rather than New so that Txn can be embedded within larger structs and so that
// temporary state can be reused.
func (tx *Txn) Init(store *Store, kv *kv.Txn) {
	tx.kv = kv
	tx.store = store
	tx.codec = makeStoreCodec(store.quantizer)
	tx.rootCodec = makeStoreCodec(store.rootQuantizer)

	// TODO (mw5h): This doesn't take into account session variables that control
	// lock durability. This doesn't matter for partition maintenance operations
	// that don't have a session, but may lead to unexpected behavior for CRUD
	// operations. The logic for determining what to do there is in optBuilder,
	// so there may be some plumbing involved to get it down here.
	if kv.IsoLevel() == isolation.Serializable {
		tx.lockDurability = kvpb.BestEffort
	} else {
		tx.lockDurability = kvpb.GuaranteedDurability
	}
}

// getCodecForPartitionKey returns the correct codec to use for interacting with
// the partition indicated. This will be the unquantized codec for the root
// partition, the codec for the quantizer indicated when the store was created
// otherwise.
func (tx *Txn) getCodecForPartitionKey(partitionKey cspann.PartitionKey) *storeCodec {
	// We always store the full sized vectors in the root partition
	if partitionKey == cspann.RootKey {
		return &tx.rootCodec
	} else {
		return &tx.codec
	}
}

// decodePartition decodes the metadata and data KV results into an ephemeral
// partition. This partition will become invalid when the codec is next reset,
// so it needs to be cloned if it will be used outside of the store.
func (tx *Txn) decodePartition(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, metaResult, dataResult *kv.Result,
) (*cspann.Partition, error) {
	metadata, err := tx.getMetadataFromKVResult(partitionKey, metaResult)
	if err != nil {
		return nil, err
	}

	if dataResult.Err != nil {
		return nil, dataResult.Err
	}
	vectorEntries := dataResult.Rows

	// Clear and ensure storage for the vector entries, child keys, and value
	// bytes.
	codec := tx.getCodecForPartitionKey(partitionKey)
	codec.clear(len(vectorEntries), metadata.Centroid)
	if cap(tx.tmpChildKeys) < len(vectorEntries) {
		tx.tmpChildKeys = make([]cspann.ChildKey, len(vectorEntries))
	}
	tx.tmpChildKeys = tx.tmpChildKeys[:len(vectorEntries)]
	if cap(tx.tmpValueBytes) < len(vectorEntries) {
		tx.tmpValueBytes = make([]cspann.ValueBytes, len(vectorEntries))
	}
	tx.tmpValueBytes = tx.tmpValueBytes[:len(vectorEntries)]

	// Vector entries add the encoded partition level to the metadata key.
	metaKeyLen := calculateMetaKeyLen(tx.store, treeKey, partitionKey)
	metaKeyLen += vecencoding.EncodedPartitionLevelLen(metadata.Level)
	for i, entry := range vectorEntries {
		childKey, err := vecencoding.DecodeChildKey(entry.Key[metaKeyLen:], metadata.Level)
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
		codec.quantizer, codec.getVectorSet(), tx.tmpChildKeys, tx.tmpValueBytes, metadata.Level), nil
}

// GetPartition is part of the cspann.Txn interface. Read the partition
// indicated by `partitionKey` and build a Partition data structure, which is
// returned.
func (tx *Txn) GetPartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	b := tx.kv.NewBatch()

	// GetPartition is used by fixup to split and merge partitions, so we want to
	// block concurrent writes.
	metadataKey := tx.store.encodePartitionKey(treeKey, partitionKey)
	metadataKey = slices.Clip(metadataKey)
	b.GetForUpdate(metadataKey, tx.lockDurability)
	b.Scan(metadataKey.Next(), metadataKey.PrefixEnd())
	err := tx.kv.Run(ctx, b)
	if err != nil {
		return nil, err
	}

	partition, err := tx.decodePartition(treeKey, partitionKey, &b.Results[0], &b.Results[1])
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
func (tx *Txn) insertPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	partition *cspann.Partition,
) error {
	b := tx.kv.NewBatch()

	metadataKey := tx.store.encodePartitionKey(treeKey, partitionKey)
	metadataKey = slices.Clip(metadataKey)
	meta, err := vecencoding.EncodePartitionMetadata(partition.Level(), partition.QuantizedSet().GetCentroid())
	if err != nil {
		return err
	}
	b.Put(metadataKey, meta)

	// Cap the key so that any append allocates a new slice.
	key := vecencoding.EncodePartitionLevel(metadataKey, partition.Level())
	key = slices.Clip(key)
	codec := tx.getCodecForPartitionKey(partitionKey)
	childKeys := partition.ChildKeys()
	valueBytes := partition.ValueBytes()
	for i := 0; i < partition.QuantizedSet().GetCount(); i++ {
		// The child key gets appended to 'key' here.
		k := vecencoding.EncodeChildKey(key, childKeys[i])
		encodedValue, err := codec.encodeVectorFromSet(partition.QuantizedSet(), i)
		if err != nil {
			return err
		}
		encodedValue = append(encodedValue, valueBytes[i]...)
		b.Put(k, encodedValue)
	}

	return tx.kv.Run(ctx, b)
}

// SetRootPartition implements the cspann.Txn interface.
func (tx *Txn) SetRootPartition(
	ctx context.Context, treeKey cspann.TreeKey, partition *cspann.Partition,
) error {
	if err := tx.DeletePartition(ctx, treeKey, cspann.RootKey); err != nil {
		return err
	}
	return tx.insertPartition(ctx, treeKey, cspann.RootKey, partition)
}

// InsertPartition implements the cspann.Txn interface.
func (tx *Txn) InsertPartition(
	ctx context.Context, treeKey cspann.TreeKey, partition *cspann.Partition,
) (cspann.PartitionKey, error) {
	instanceID := tx.store.db.KV().Context().NodeID.SQLInstanceID()
	partitionID := cspann.PartitionKey(unique.GenerateUniqueInt(unique.ProcessUniqueID(instanceID)))
	return partitionID, tx.insertPartition(ctx, treeKey, partitionID, partition)
}

// DeletePartition implements the cspann.Txn interface.
func (tx *Txn) DeletePartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) error {
	b := tx.kv.NewBatch()

	startKey := tx.store.encodePartitionKey(treeKey, partitionKey)
	endKey := startKey.PrefixEnd()

	b.DelRange(startKey, endKey, false /* returnKeys */)
	return tx.kv.Run(ctx, b)
}

// GetPartitionMetadata implements the cspann.Txn interface.
func (tx *Txn) GetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, forUpdate bool,
) (cspann.PartitionMetadata, error) {
	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := tx.kv.NewBatch()

	// Cap the metadata key so that any append allocates a new slice.
	metadataKey := tx.store.encodePartitionKey(treeKey, partitionKey)
	metadataKey = slices.Clip(metadataKey)

	if forUpdate {
		// By acquiring a shared lock on metadata key, we prevent splits/merges of
		// this partition from conflicting with the add operation.
		b.GetForShare(metadataKey, tx.lockDurability)
	} else {
		b.Get(metadataKey)
	}

	// Run the batch and get the partition metadata from results.
	if err := tx.kv.Run(ctx, b); err != nil {
		return cspann.PartitionMetadata{},
			errors.Wrapf(err, "getting partition metadata for %d", partitionKey)
	}

	// If we're preparing to update the root partition, then lazily create its
	// metadata if it does not yet exist.
	if forUpdate && partitionKey == cspann.RootKey && b.Results[0].Rows[0].Value == nil {
		return tx.createRootPartition(ctx, metadataKey)
	}

	return tx.getMetadataFromKVResult(partitionKey, &b.Results[0])
}

// AddToPartition implements the cspann.Txn interface.
func (tx *Txn) AddToPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	level cspann.Level,
	vec vector.T,
	childKey cspann.ChildKey,
	valueBytes cspann.ValueBytes,
) error {
	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := tx.kv.NewBatch()

	// Cap the metadata key so that any append allocates a new slice.
	metadataKey := tx.store.encodePartitionKey(treeKey, partitionKey)
	metadataKey = slices.Clip(metadataKey)

	// Get partition metadata, needed to quantize the vector.
	b.Get(metadataKey)
	err := tx.kv.Run(ctx, b)
	if err != nil {
		return errors.Wrapf(err, "locking partition %d for add", partitionKey)
	}
	metadata, err := tx.getMetadataFromKVResult(partitionKey, &b.Results[0])
	if err != nil {
		return err
	}

	entryKey := vecencoding.EncodePartitionLevel(metadataKey, level)
	entryKey = vecencoding.EncodeChildKey(entryKey, childKey)

	// Quantize the vector and add it to the partition with a Put command.
	b = tx.kv.NewBatch()
	codec := tx.getCodecForPartitionKey(partitionKey)
	encodedValue, err := codec.encodeVector(&tx.workspace, vec, metadata.Centroid)
	if err != nil {
		return err
	}
	encodedValue = append(encodedValue, valueBytes...)
	b.Put(entryKey, encodedValue)

	// Run the batch.
	if err = tx.kv.Run(ctx, b); err != nil {
		return errors.Wrapf(err, "adding vector to partition %d", partitionKey)
	}
	return nil
}

// RemoveFromPartition implements the cspann.Txn interface.
func (tx *Txn) RemoveFromPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	level cspann.Level,
	childKey cspann.ChildKey,
) error {
	b := tx.kv.NewBatch()

	// Cap the metadata key so that the append allocates a new slice for the child
	// key.
	metadataKey := tx.store.encodePartitionKey(treeKey, partitionKey)
	metadataKey = slices.Clip(metadataKey)
	entryKey := vecencoding.EncodePartitionLevel(metadataKey, level)
	entryKey = vecencoding.EncodeChildKey(entryKey, childKey)
	b.Del(entryKey)
	if err := tx.kv.Run(ctx, b); err != nil {
		return err
	}
	// We ignore key not found for the deleted child.

	return nil
}

// SearchPartitions implements the cspann.Txn interface.
func (tx *Txn) SearchPartitions(
	ctx context.Context,
	treeKey cspann.TreeKey,
	toSearch []cspann.PartitionToSearch,
	queryVector vector.T,
	searchSet *cspann.SearchSet,
) (cspann.Level, error) {
	b := tx.kv.NewBatch()

	for i := range toSearch {
		// Cap the metadata key so that any append allocates a new slice.
		metadataKey := tx.store.encodePartitionKey(treeKey, toSearch[i].Key)
		metadataKey = slices.Clip(metadataKey)
		b.Get(metadataKey)
		if toSearch[i].ExcludeLeafVectors {
			// Skip past vectors at the leaf level.
			startKey := vecencoding.EncodePartitionLevel(metadataKey, cspann.SecondLevel)
			b.Scan(startKey, metadataKey.PrefixEnd())
		} else {
			b.Scan(metadataKey.Next(), metadataKey.PrefixEnd())
		}
	}

	if err := tx.kv.Run(ctx, b); err != nil {
		return cspann.InvalidLevel, err
	}

	level := cspann.InvalidLevel
	for i := range toSearch {
		partition, err := tx.decodePartition(
			treeKey, toSearch[i].Key, &b.Results[i*2], &b.Results[i*2+1])
		if err != nil {
			return cspann.InvalidLevel, err
		}

		searchLevel, partitionCount := partition.Search(
			&tx.workspace, toSearch[i].Key, queryVector, searchSet)
		if i == 0 {
			level = searchLevel
		} else if level != searchLevel {
			// Callers should only search for partitions at the same level.
			panic(errors.AssertionFailedf(
				"caller already searched a partition at level %d, cannot search at level %d",
				level, searchLevel))
		}
		toSearch[i].Count = partitionCount
	}

	return level, nil
}

// getFullVectorsFromPK fills in refs that are specified by primary key. Refs
// that specify a partition ID are ignored. The values are returned in-line in
// the refs slice.
func (tx *Txn) getFullVectorsFromPK(
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
			ok, refIdx, err := fetcher.NextRowDecodedInto(ctx, data[:], tx.store.colIdxMap)
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
func (tx *Txn) getFullVectorsFromPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, refs []cspann.VectorWithKey,
) (numPKLookups int, err error) {
	var b *kv.Batch

	for _, ref := range refs {
		if ref.Key.PartitionKey == cspann.InvalidKey {
			numPKLookups++
			continue
		}
		metadataKey := tx.store.encodePartitionKey(treeKey, ref.Key.PartitionKey)
		if b == nil {
			b = tx.kv.NewBatch()
		}
		b.Get(metadataKey)
	}

	if numPKLookups == len(refs) {
		// All of the lookups are for leaf vectors, so don't fetch any centroids.
		return numPKLookups, nil
	}
	if err := tx.kv.Run(ctx, b); err != nil {
		return 0, err
	}

	idx := 0
	for _, result := range b.Results {
		// Skip past primary key references.
		for ; refs[idx].Key.PartitionKey == cspann.InvalidKey; idx++ {
		}

		if result.Rows[0].ValueBytes() == nil {
			// If this is the root partition, then the metadata row is missing;
			// it is only created when the first split of the root happens.
			if refs[idx].Key.PartitionKey != cspann.RootKey {
				return 0, cspann.ErrPartitionNotFound
			}
			refs[idx].Vector = tx.store.emptyVec
		} else {
			// Get the centroid from the partition metadata.
			_, refs[idx].Vector, err = vecencoding.DecodePartitionMetadata(result.Rows[0].ValueBytes())
			if err != nil {
				return 0, err
			}
		}
		idx++
	}
	return numPKLookups, nil
}

// GetFullVectors implements the cspann.Txn interface.
func (tx *Txn) GetFullVectors(
	ctx context.Context, treeKey cspann.TreeKey, refs []cspann.VectorWithKey,
) error {
	numPKLookups, err := tx.getFullVectorsFromPartitionMetadata(ctx, treeKey, refs)
	if err != nil {
		return err
	}
	if numPKLookups > 0 {
		err = tx.getFullVectorsFromPK(ctx, refs, numPKLookups)
	}
	return err
}

// createRootPartition uses the KV CPut operation to create metadata for the
// root partition, and then returns that metadata.
//
// NOTE: CPut always "sees" the latest write of the metadata record, even if the
// timestamp of that write is higher than this transaction's.
func (tx *Txn) createRootPartition(
	ctx context.Context, metadataKey roachpb.Key,
) (cspann.PartitionMetadata, error) {
	b := tx.kv.NewBatch()
	metadata := cspann.PartitionMetadata{Level: cspann.LeafLevel, Centroid: tx.store.emptyVec}
	encoded, err := vecencoding.EncodePartitionMetadata(metadata.Level, metadata.Centroid)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// Use CPutAllowingIfNotExists in order to handle the case where the same
	// transaction inserts multiple vectors (e.g. multiple VALUES rows). In that
	// case, the first row will trigger creation of the metadata record. However,
	// subsequent inserts will not be able to "see" this record, since they will
	// read at a lower sequence number than the metadata record was written.
	// However, CPutAllowingIfNotExists will read at the higher sequence number
	// and see that the record was already created.
	//
	// On the other hand, if a different transaction wrote the record, it will
	// have a higher timestamp, and that will trigger a WriteTooOld error.
	// Transactions which lose that race need to be refreshed.
	var roachval roachpb.Value
	roachval.SetBytes(encoded)
	b.CPutAllowingIfNotExists(metadataKey, &roachval, roachval.TagAndDataBytes())
	if err := tx.kv.Run(ctx, b); err != nil {
		// Lost the race to a different transaction.
		return cspann.PartitionMetadata{}, errors.Wrapf(err, "creating root partition metadata")
	}
	return metadata, nil
}

// QuantizeAndEncode quantizes the given vector (which has already been
// randomized by the caller) with respect to the given centroid. It returns the
// encoded form of that quantized vector.
func (tx *Txn) QuantizeAndEncode(
	partitionKey cspann.PartitionKey, centroid, randomizedVec vector.T,
) (quantized []byte, err error) {
	// Quantize and encode the randomized vector.
	codec := tx.getCodecForPartitionKey(partitionKey)
	return codec.encodeVector(&tx.workspace, randomizedVec, centroid)
}

// getMetadataFromKVResult returns the partition metadata row from the KV
// result, returning the partition's K-means tree level and centroid.
func (tx *Txn) getMetadataFromKVResult(
	partitionKey cspann.PartitionKey, result *kv.Result,
) (cspann.PartitionMetadata, error) {
	if result.Err != nil {
		return cspann.PartitionMetadata{}, result.Err
	}

	// If the value of the first result row is nil and this is a root partition,
	// then it must be a root partition without a metadata record (a nil result
	// happens when Get is used to fetch the metadata row).
	value := result.Rows[0].ValueBytes()
	if value == nil {
		if partitionKey != cspann.RootKey {
			return cspann.PartitionMetadata{}, cspann.ErrPartitionNotFound
		}

		// Construct synthetic metadata.
		metadata := cspann.PartitionMetadata{Level: cspann.LeafLevel, Centroid: tx.store.emptyVec}
		return metadata, nil
	}

	level, centroid, err := vecencoding.DecodePartitionMetadata(value)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// Return the metadata.
	return cspann.PartitionMetadata{Level: level, Centroid: centroid}, nil
}

// calculateMetaKeyLen returns the length of the metadata partition key.
func calculateMetaKeyLen(
	store *Store, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) int {
	return len(store.prefix) + len(treeKey) + vecencoding.EncodedPartitionKeyLen(partitionKey)
}
