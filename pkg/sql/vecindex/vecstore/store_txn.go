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
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
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

	// codec is used to decode KV rows and encode vectors.
	codec partitionCodec

	// Retained allocations to prevent excessive reallocation.
	tmpSpans   []roachpb.Span
	tmpSpanIDs []int
}

var _ cspann.Txn = (*Txn)(nil)

// Init sets initial values for the transaction, wrapping it around a kv
// transaction for use with the cspann.Store API. The Init pattern is used
// rather than New so that Txn can be embedded within larger structs and so that
// temporary state can be reused.
func (tx *Txn) Init(store *Store, kv *kv.Txn) {
	tx.kv = kv
	tx.store = store
	tx.codec = makePartitionCodec(store.rootQuantizer, store.quantizer)

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

// GetPartitionMetadata implements the cspann.Txn interface.
func (tx *Txn) GetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, forUpdate bool,
) (cspann.PartitionMetadata, error) {
	if forUpdate && tx.store.ReadOnly() {
		return cspann.PartitionMetadata{}, errors.AssertionFailedf(
			"cannot lock partition metadata in read-only mode")
	}

	metadataKey := vecencoding.EncodeMetadataKey(tx.store.prefix, treeKey, partitionKey)

	// By acquiring a shared lock on metadata key, we prevent splits/merges of
	// this partition from conflicting with the add operation.
	b, err := func() (b *kv.Batch, err error) {
		// TODO(mw5h): Add to an existing batch instead of starting a new one.
		b = tx.kv.NewBatch()

		if tx.kv.Sender().GetSteppingMode(ctx) == kv.SteppingEnabled {
			// When there are multiple inserts within the same SQL statement, the
			// first insert will trigger creation of the metadata record. However,
			// subsequent inserts will not be able to "see" this record, since they
			// will read at a lower sequence number than the metadata record was
			// written. Handle this issue by temporarily stepping the read sequence
			// number so the latest metadata can be read.
			prevSeqNum := tx.kv.GetReadSeqNum()
			if err = tx.kv.Step(ctx, false /* allowReadTimestampStep */); err != nil {
				return nil, err
			}
			defer func() {
				// Restore the original sequence number.
				if readErr := tx.kv.SetReadSeqNum(prevSeqNum); err != nil {
					err = errors.CombineErrors(err, readErr)
				}
			}()
		}

		if forUpdate {
			b.GetForShare(metadataKey, tx.lockDurability)
		} else {
			b.Get(metadataKey)
		}

		// Run the batch.
		if err := tx.kv.Run(ctx, b); err != nil {
			return nil, errors.Wrapf(err, "getting partition metadata for %d", partitionKey)
		}

		return b, nil
	}()
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// If we're preparing to update the root partition, then lazily create its
	// metadata if it does not yet exist.
	if forUpdate && partitionKey == cspann.RootKey && b.Results[0].Rows[0].Value == nil {
		return tx.createRootPartition(ctx, metadataKey)
	}

	metadata, err := tx.store.getMetadataFromKVResult(partitionKey, &b.Results[0])
	if err != nil {
		return cspann.PartitionMetadata{},
			errors.Wrapf(err, "getting partition metadata for %d", partitionKey)
	}

	// Do not allow updates to the partition if the state doesn't allow it.
	if forUpdate && !metadata.StateDetails.State.AllowAddOrRemove() {
		err = cspann.NewConditionFailedError(metadata)
		return cspann.PartitionMetadata{}, errors.Wrapf(err,
			"getting partition metadata %d (state=%s)",
			partitionKey, metadata.StateDetails.State.String())
	}

	return metadata, nil
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
	if tx.store.ReadOnly() {
		return errors.AssertionFailedf("cannot add to partition in read-only mode")
	}

	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := tx.kv.NewBatch()

	// Get partition metadata, needed to quantize the vector. Lock the metadata
	// key in order to prevent splits/merges from interfering.
	metadataKey := vecencoding.EncodeMetadataKey(tx.store.prefix, treeKey, partitionKey)
	b.GetForShare(metadataKey, tx.lockDurability)
	err := tx.kv.Run(ctx, b)
	if err != nil {
		return errors.Wrapf(err, "locking partition %d for add", partitionKey)
	}

	// If we're preparing to update the root partition, then lazily create its
	// metadata if it does not yet exist.
	var metadata cspann.PartitionMetadata
	if partitionKey == cspann.RootKey && b.Results[0].Rows[0].Value == nil {
		metadata, err = tx.createRootPartition(ctx, metadataKey)
	} else {
		metadata, err = tx.store.getMetadataFromKVResult(partitionKey, &b.Results[0])
	}
	if err != nil {
		return err
	}

	// Do not allow vectors to be added to the partition if the state doesn't
	// allow it.
	if !metadata.StateDetails.State.AllowAddOrRemove() {
		return errors.Wrapf(cspann.NewConditionFailedError(metadata),
			"adding to partition %d (state=%s)", partitionKey, metadata.StateDetails.State.String())
	}

	entryKey := vecencoding.EncodePrefixVectorKey(metadataKey, level)
	entryKey = vecencoding.EncodeChildKey(entryKey, childKey)

	// Quantize the vector and add it to the partition with a Put command.
	b = tx.kv.NewBatch()
	encodedValue, err := tx.codec.EncodeVector(partitionKey, vec, metadata.Centroid)
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
	if tx.store.ReadOnly() {
		return errors.AssertionFailedf("cannot remove from partition in read-only mode")
	}

	// TODO(mw5h): Add to an existing batch instead of starting a new one.
	b := tx.kv.NewBatch()

	metadataKey := vecencoding.EncodeMetadataKey(tx.store.prefix, treeKey, partitionKey)

	// Get partition metadata, needed to quantize the vector. Lock the metadata
	// key in order to prevent splits/merges from interfering.
	b.GetForShare(metadataKey, tx.lockDurability)
	if err := tx.kv.Run(ctx, b); err != nil {
		return errors.Wrapf(err, "locking partition %d for add", partitionKey)
	}

	metadata, err := tx.store.getMetadataFromKVResult(partitionKey, &b.Results[0])
	if err != nil {
		return err
	}

	// Do not allow vectors to be removed from the partition if the state doesn't
	// allow it.
	if !metadata.StateDetails.State.AllowAddOrRemove() {
		return errors.Wrapf(cspann.NewConditionFailedError(metadata),
			"removing from partition %d (state=%s)", partitionKey, metadata.StateDetails.State.String())
	}

	b = tx.kv.NewBatch()
	entryKey := vecencoding.EncodePrefixVectorKey(metadataKey, level)
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
) error {
	b := tx.kv.NewBatch()

	for i := range toSearch {
		metadataKey := vecencoding.EncodeMetadataKey(tx.store.prefix, treeKey, toSearch[i].Key)
		b.Get(metadataKey)
		if toSearch[i].ExcludeLeafVectors {
			// Skip past vectors at the leaf level.
			startKey := vecencoding.EncodePrefixVectorKey(metadataKey, cspann.SecondLevel)
			endKey := vecencoding.EncodeEndVectorKey(metadataKey)
			b.Scan(startKey, endKey)
		} else {
			startKey := vecencoding.EncodeStartVectorKey(metadataKey)
			endKey := vecencoding.EncodeEndVectorKey(metadataKey)
			b.Scan(startKey, endKey)
		}
	}

	if err := tx.kv.Run(ctx, b); err != nil {
		return err
	}

	for i := range toSearch {
		partition, err := tx.store.decodePartition(
			treeKey, toSearch[i].Key, &tx.codec, &b.Results[i*2], &b.Results[i*2+1])
		if err != nil {
			if errors.Is(err, cspann.ErrPartitionNotFound) {
				// Partition not found, so return InvalidLevel, MissingState, and
				// Count=0.
				toSearch[i].Level = cspann.InvalidLevel
				toSearch[i].StateDetails = cspann.PartitionStateDetails{}
				toSearch[i].Count = 0
			} else {
				return err
			}
		} else {
			toSearch[i].Level = partition.Level()
			toSearch[i].StateDetails = partition.Metadata().StateDetails
			toSearch[i].Count = partition.Search(
				&tx.codec.workspace, toSearch[i].Key, queryVector, searchSet)
		}
	}

	return nil
}

// getFullVectorsFromPK fills in refs that are specified by primary key. Refs
// that specify a partition ID are ignored. The values are returned in-line in
// the refs slice.
func (tx *Txn) getFullVectorsFromPK(ctx context.Context, refs []cspann.VectorWithKey) (err error) {
	if cap(tx.tmpSpans) >= len(refs) {
		tx.tmpSpans = tx.tmpSpans[:0]
		tx.tmpSpanIDs = tx.tmpSpanIDs[:0]
	} else {
		tx.tmpSpans = make([]roachpb.Span, 0, len(refs))
		tx.tmpSpanIDs = make([]int, 0, len(refs))
	}

	for refIdx, ref := range refs {
		if ref.Key.PartitionKey != cspann.InvalidKey {
			return errors.AssertionFailedf(
				"cannot mix partition key and primary key requests to GetFullVectors")
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
			} else {
				refs[refIdx].Vector = nil
			}
		}
	}
	return err
}

// getFullVectorsFromPartitionMetadata traverses the refs list and fills in refs
// specified by partition ID. Primary key references are ignored.
func (tx *Txn) getFullVectorsFromPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, refs []cspann.VectorWithKey,
) error {
	var b *kv.Batch

	for _, ref := range refs {
		if ref.Key.PartitionKey == cspann.InvalidKey {
			return errors.AssertionFailedf(
				"cannot mix partition key and primary key requests to GetFullVectors")
		}
		metadataKey := vecencoding.EncodeMetadataKey(tx.store.prefix, treeKey, ref.Key.PartitionKey)
		if b == nil {
			b = tx.kv.NewBatch()
		}
		b.Get(metadataKey)
	}

	if err := tx.kv.Run(ctx, b); err != nil {
		return errors.Wrapf(err, "fetching partition metadata for GetFullVectors")
	}

	idx := 0
	for _, result := range b.Results {
		if result.Rows[0].ValueBytes() == nil {
			// If this is the root partition, then the metadata row is missing;
			// it is only created when the first split of the root happens.
			if refs[idx].Key.PartitionKey == cspann.RootKey {
				refs[idx].Vector = tx.store.emptyVec
			} else {
				refs[idx].Vector = nil
			}
		} else {
			// Get the centroid from the partition metadata.
			metadata, err := vecencoding.DecodeMetadataValue(result.Rows[0].ValueBytes())
			if err != nil {
				return err
			}
			refs[idx].Vector = metadata.Centroid
		}
		idx++
	}
	return nil
}

// GetFullVectors implements the cspann.Txn interface.
func (tx *Txn) GetFullVectors(
	ctx context.Context, treeKey cspann.TreeKey, refs []cspann.VectorWithKey,
) error {
	if len(refs) == 0 {
		return nil
	}

	// All vectors must be at the same level of the tree.
	if refs[0].Key.PartitionKey != cspann.InvalidKey {
		// Get partition centroids.
		return tx.getFullVectorsFromPartitionMetadata(ctx, treeKey, refs)
	}

	// Get vectors from primary index.
	return tx.getFullVectorsFromPK(ctx, refs)
}

// createRootPartition uses the KV CPut operation to create metadata for the
// root partition, and then returns that metadata. If another transaction races
// and creates the root partition at a higher timestamp, createRootPartition
// returns a WriteTooOld error, which will trigger a refresh of this transaction
// at the higher timestamp.
func (tx *Txn) createRootPartition(
	ctx context.Context, metadataKey roachpb.Key,
) (cspann.PartitionMetadata, error) {
	b := tx.kv.NewBatch()
	metadata := cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, tx.store.emptyVec)
	encoded := vecencoding.EncodeMetadataValue(metadata)

	// Use CPut to detect the case where another transaction is racing to create
	// the root partition. CPut always "sees" the latest version of the metadata
	// record.
	b.CPut(metadataKey, encoded, nil /* expValue */)
	if err := tx.kv.Run(ctx, b); err != nil {
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
	return tx.codec.EncodeVector(partitionKey, randomizedVec, centroid)
}
