// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecencoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Store implements the cspann.Store interface for KV backed vector indices.
type Store struct {
	db descs.DB // Used to generate new partition IDs
	kv *kv.DB

	// readOnly is true if the store does not accept writes.
	readOnly bool

	// The root partition always uses the UnQuantizer while other partitions may
	// use any quantizer.
	rootQuantizer quantize.Quantizer
	quantizer     quantize.Quantizer

	// minConsistency can override default INCONSISTENCY usage when estimating
	// the size of a partition. This is used for testing.
	minConsistency kvpb.ReadConsistencyType

	prefix    roachpb.Key            // KV prefix for the vector index.
	pkPrefix  roachpb.Key            // KV prefix for the primary key.
	fetchSpec fetchpb.IndexFetchSpec // A pre-built fetch spec for this index.
	colIdxMap catalog.TableColMap    // A column map for extracting full sized vectors from the PK.
	emptyVec  vector.T               // A zero-valued vector, used when root centroid does not exist.
}

var _ cspann.Store = (*Store)(nil)

// NewWithColumnID creates a Store for an index on the provided table descriptor
// using the provided column ID as the vector column for the index. This is used
// in unit tests where full vector index creation capabilities aren't
// necessarily available.
func NewWithColumnID(
	ctx context.Context,
	db descs.DB,
	quantizer quantize.Quantizer,
	defaultCodec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexID catid.IndexID,
	vectorColumnID descpb.ColumnID,
) (ps *Store, err error) {
	ps = &Store{
		db:             db,
		kv:             db.KV(),
		rootQuantizer:  quantize.NewUnQuantizer(quantizer.GetDims(), quantizer.GetDistanceMetric()),
		quantizer:      quantizer,
		minConsistency: kvpb.INCONSISTENT,
		emptyVec:       make(vector.T, quantizer.GetDims()),
	}

	codec := defaultCodec
	tableID := tableDesc.GetID()
	pk := tableDesc.GetPrimaryIndex()
	if ext := tableDesc.ExternalRowData(); ext != nil {
		// The table is external, so use the external codec and table ID. Also set
		// the index to read-only.
		log.VInfof(ctx, 2,
			"table %d is external, using read-only mode for vector index %d",
			tableDesc.GetID(), indexID,
		)
		ps.readOnly = true
		codec = keys.MakeSQLCodec(ext.TenantID)
		tableID = ext.TableID
	}
	ps.prefix = rowenc.MakeIndexKeyPrefix(codec, tableID, indexID)
	ps.pkPrefix = rowenc.MakeIndexKeyPrefix(codec, tableID, pk.GetID())

	ps.colIdxMap.Set(vectorColumnID, 0)
	err = rowenc.InitIndexFetchSpec(
		&ps.fetchSpec,
		codec,
		tableDesc,
		pk,
		[]descpb.ColumnID{vectorColumnID},
	)
	return ps, err
}

// New creates a cspann.Store interface backed by the KV for a single vector
// index.
func New(
	ctx context.Context,
	db descs.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	tableID catid.DescID,
	indexID catid.IndexID,
) (ps *Store, err error) {
	var tableDesc catalog.TableDescriptor
	err = db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		tableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, tableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	var index catalog.Index
	for _, desc := range tableDesc.DeletableNonPrimaryIndexes() {
		if desc.GetID() == indexID {
			index = desc
			break
		}
	}
	if index == nil {
		return nil, errors.AssertionFailedf("index %d not found in table %d", indexID, tableID)
	}

	vectorColumnID := index.VectorColumnID()

	return NewWithColumnID(ctx, db, quantizer, codec, tableDesc, indexID, vectorColumnID)
}

// SetConsistency sets the minimum consistency level to use when reading
// partitions. This is set to a higher level for deterministic tests.
func (s *Store) SetMinimumConsistency(consistency kvpb.ReadConsistencyType) {
	s.minConsistency = consistency
}

// ReadOnly returns true if the store does not allow writes.
func (s *Store) ReadOnly() bool {
	return s.readOnly
}

// RunTransaction is part of the cspann.Store interface. It runs a function in
// the context of a transaction.
func (s *Store) RunTransaction(ctx context.Context, fn func(txn cspann.Txn) error) (err error) {
	var txn Txn
	txn.Init(s, s.kv.NewTxn(ctx, "vecstore.Store transaction"))
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = txn.kv.Commit(ctx)
		}
		if err != nil {
			err = errors.CombineErrors(err, txn.kv.Rollback(ctx))
		}
	}()

	return fn(&txn)
}

// MakePartitionKey is part of the cspann.Store interface. It allocates a new
// unique partition key.
func (s *Store) MakePartitionKey() cspann.PartitionKey {
	instanceID := s.kv.Context().NodeID.SQLInstanceID()
	return cspann.PartitionKey(unique.GenerateUniqueUnorderedID(unique.ProcessUniqueID(instanceID)))
}

// EstimatePartitionCount is part of the cspann.Store interface. It returns an
// estimate of the number of vectors in the given partition.
func (s *Store) EstimatePartitionCount(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (int, error) {
	// Create a batch with INCONSISTENT read consistency to avoid updating the
	// timestamp cache or blocking on locks.
	// NOTE: In rare edge cases, INCONSISTENT scans can return results that are
	// arbitrarily old. However, there is a fixup processor on every node, so each
	// partition has its size checked multiple times across nodes. At least two
	// nodes in a cluster will have up-to-date results for any given partition, so
	// stale results are not a concern in practice. If we ever find evidence that
	// it is, we can fall back to a consistent scan if the inconsistent scan
	// returns results that are too old.
	b := s.kv.NewBatch()
	b.Header.ReadConsistency = s.minConsistency

	// Count the number of rows in the partition after the metadata row.
	metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
	startKey := vecencoding.EncodeStartVectorKey(metadataKey)
	endKey := vecencoding.EncodeEndVectorKey(metadataKey)
	b.Scan(startKey, endKey)

	// Execute the batch and count the rows in the response.
	if err := s.kv.Run(ctx, b); err != nil {
		return 0, errors.Wrap(err, "estimating partition count")
	}
	if err := b.Results[0].Err; err != nil {
		return 0, errors.Wrap(err, "extracting Scan rows for partition count")
	}
	return len(b.Results[0].Rows), nil
}

// MergeStats is part of the cspann.Store interface.
func (s *Store) MergeStats(ctx context.Context, stats *cspann.IndexStats, skipMerge bool) error {
	if !skipMerge && s.ReadOnly() {
		return errors.AssertionFailedf("cannot merge stats in read-only mode")
	}
	// TODO(mw5h): Implement MergeStats. We're not panicking here because some tested
	// functionality needs to call this function but does not depend on the results.
	return nil
}

// TryDeletePartition is part of the cspann.Store interface. It deletes an
// existing partition.
func (s *Store) TryDeletePartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) error {
	if s.ReadOnly() {
		return errors.AssertionFailedf("cannot delete partition in read-only mode")
	}
	// Delete the metadata key and all vector keys in the partition.
	b := s.kv.NewBatch()
	metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
	endKey := vecencoding.EncodeEndVectorKey(metadataKey)
	b.DelRange(metadataKey, endKey, true /* returnKeys */)
	if err := s.kv.Run(ctx, b); err != nil {
		return err
	}
	if len(b.Results[0].Keys) == 0 {
		// No metadata row existed, so partition must not exist.
		return cspann.ErrPartitionNotFound
	}
	return nil
}

// TryCreateEmptyPartition is part of the cspann.Store interface. It creates a
// new partition that contains no vectors.
func (s *Store) TryCreateEmptyPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	metadata cspann.PartitionMetadata,
) error {
	if s.ReadOnly() {
		return errors.AssertionFailedf("cannot create partition in read-only mode")
	}
	meta := vecencoding.EncodeMetadataValue(metadata)
	metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
	if err := s.kv.CPut(ctx, metadataKey, meta, nil /* expValue */); err != nil {
		return remapConditionFailedError(err)
	}
	return nil
}

// TryGetPartition is part of the cspann.Store interface. It returns an existing
// partition, including both its metadata and data.
func (s *Store) TryGetPartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	b := s.kv.NewBatch()
	metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
	startKey := vecencoding.EncodeStartVectorKey(metadataKey)
	endKey := vecencoding.EncodeEndVectorKey(metadataKey)
	b.Get(metadataKey)
	b.Scan(startKey, endKey)
	if err := s.kv.Run(ctx, b); err != nil {
		return nil, err
	}

	codec := makePartitionCodec(s.rootQuantizer, s.quantizer)
	partition, err := s.decodePartition(treeKey, partitionKey, &codec, &b.Results[0], &b.Results[1])
	if err != nil {
		return nil, err
	}
	return partition, nil
}

// TryGetPartitionMetadata is part of the cspann.Store interface. It returns the
// metadata for a batch of partitions.
func (s *Store) TryGetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, toGet []cspann.PartitionMetadataToGet,
) error {
	// Construct a batch with one Get request per partition.
	b := s.kv.NewBatch()
	for i := range toGet {
		metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, toGet[i].Key)
		b.Get(metadataKey)
	}

	// Run the batch and return results.
	var err error
	if err = s.kv.Run(ctx, b); err != nil {
		return errors.Wrapf(err, "getting partition metadata for %d partitions", len(toGet))
	}

	for i := range toGet {
		item := &toGet[i]
		item.Metadata, err = s.getMetadataFromKVResult(item.Key, &b.Results[i])

		// If partition is missing, just return Missing metadata.
		if err != nil {
			if !errors.Is(err, cspann.ErrPartitionNotFound) {
				return err
			}
		}
	}

	return nil
}

// TryUpdatePartitionMetadata is part of the cspann.Store interface. It updates
// the metadata for an existing partition.
func (s *Store) TryUpdatePartitionMetadata(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	metadata cspann.PartitionMetadata,
	expected cspann.PartitionMetadata,
) error {
	if s.ReadOnly() {
		return errors.AssertionFailedf("cannot update partition in read-only mode")
	}
	metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
	encodedMetadata := vecencoding.EncodeMetadataValue(metadata)
	encodedExpected := vecencoding.EncodeMetadataValue(expected)

	var roachval roachpb.Value
	roachval.SetBytes(encodedExpected)
	err := s.kv.CPut(ctx, metadataKey, encodedMetadata, roachval.TagAndDataBytes())
	if err != nil {
		return remapConditionFailedError(err)
	}
	return nil
}

// TryAddToPartition is part of the cspann.Store interface. It adds vectors to
// an existing partition.
func (s *Store) TryAddToPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	vectors vector.Set,
	childKeys []cspann.ChildKey,
	valueBytes []cspann.ValueBytes,
	expected cspann.PartitionMetadata,
) (added bool, err error) {
	if s.ReadOnly() {
		return added, errors.AssertionFailedf("cannot add to partition in read-only mode")
	}
	return added, s.kv.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Acquire a shared lock on the partition, to ensure that another agent
		// doesn't modify it. Also, this will be used to verify expected metadata.
		b := txn.NewBatch()
		metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
		b.GetForShare(metadataKey, kvpb.BestEffort)
		if err = txn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, "locking partition %d for add", partitionKey)
		}

		// Verify expected metadata.
		metadata, err := s.getMetadataFromKVResult(partitionKey, &b.Results[0])
		if err != nil {
			return err
		}
		if !metadata.Equal(&expected) {
			return cspann.NewConditionFailedError(metadata)
		}
		if !metadata.StateDetails.State.AllowAdd() {
			return errors.AssertionFailedf(
				"cannot add to partition in state %s that disallows adds", metadata.StateDetails.State)
		}

		// Do not add vectors that are found to already exist.
		var exclude cspann.ChildKeyDeDup
		exclude.Init(vectors.Count)

		// Cap the key so that appends allocate a new slice.
		vectorKey := vecencoding.EncodePrefixVectorKey(metadataKey, metadata.Level)
		vectorKey = slices.Clip(vectorKey)
		for {
			// Quantize the vectors and add them to the partition with CPut commands
			// that only take action if there is no value present yet.
			b = txn.NewBatch()
			codec := makePartitionCodec(s.rootQuantizer, s.quantizer)
			for i := range vectors.Count {
				if !exclude.TryAdd(childKeys[i]) {
					// Vector already exists in the partition, do not add.
					continue
				}
				added = true

				encodedValue, err := codec.EncodeVector(partitionKey, vectors.At(i), metadata.Centroid)
				if err != nil {
					return err
				}
				encodedValue = append(encodedValue, valueBytes[i]...)

				encodedKey := vecencoding.EncodeChildKey(vectorKey, childKeys[i])
				b.CPut(encodedKey, encodedValue, nil /* expValue */)
			}

			if err = txn.Run(ctx, b); err == nil {
				// The batch succeeded, so done.
				return nil
			}

			// If the batch failed due to a CPut failure, then retry, but with
			// any existing vectors excluded.
			var errConditionFailed *kvpb.ConditionFailedError
			if !errors.As(err, &errConditionFailed) {
				// This was a different error, so exit.
				return err
			}

			// Scan for existing vectors so they can be excluded from next attempt
			// to add.
			added = false
			exclude.Clear()
			b = txn.NewBatch()
			startKey := vecencoding.EncodeStartVectorKey(metadataKey)
			endKey := vecencoding.EncodeEndVectorKey(metadataKey)
			b.Scan(startKey, endKey)
			if err = txn.Run(ctx, b); err != nil {
				return errors.Wrapf(err, "scanning for existing vectors in partition %d", partitionKey)
			}
			for _, keyval := range b.Results[0].Rows {
				// Extract child key from the KV key.
				prefixLen := len(vectorKey)
				childKey, err := vecencoding.DecodeChildKey(keyval.Key[prefixLen:], metadata.Level)
				if err != nil {
					return errors.Wrapf(err, "decoding vector index key in partition %d: %+v",
						partitionKey, keyval.Key)
				}
				exclude.TryAdd(childKey)
			}
		}
	})
}

// TryRemoveFromPartition is part of the cspann.Store interface. It removes
// vectors from an existing partition.
func (s *Store) TryRemoveFromPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	childKeys []cspann.ChildKey,
	expected cspann.PartitionMetadata,
) (removed bool, err error) {
	if s.ReadOnly() {
		return removed, errors.AssertionFailedf("cannot remove from partition in read-only mode")
	}
	return removed, s.kv.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Acquire a shared lock on the partition, to ensure that another agent
		// doesn't modify it. Also, this will be used to verify expected metadata.
		b := txn.NewBatch()
		metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
		b.GetForShare(metadataKey, kvpb.BestEffort)
		if err = txn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, "locking partition %d for add", partitionKey)
		}

		// Verify expected metadata.
		metadata, err := s.getMetadataFromKVResult(partitionKey, &b.Results[0])
		if err != nil {
			return err
		}
		if !metadata.Equal(&expected) {
			return cspann.NewConditionFailedError(metadata)
		}

		// Cap the vector key so that appends allocate a new slice.
		vectorKey := vecencoding.EncodePrefixVectorKey(metadataKey, metadata.Level)
		vectorKey = slices.Clip(vectorKey)

		// Quantize the vectors and add them to the partition with CPut commands
		// that only take action if there is no value present yet.
		b = txn.NewBatch()
		codec := makePartitionCodec(s.rootQuantizer, s.quantizer)
		codec.InitForDecoding(partitionKey, metadata, 1)
		for _, childKey := range childKeys {
			encodedKey := vecencoding.EncodeChildKey(vectorKey, childKey)
			b.Del(encodedKey)
		}

		if err = txn.CommitInBatch(ctx, b); err != nil {
			return err
		}

		for _, response := range b.RawResponse().Responses {
			del := response.GetDelete()
			if del != nil && del.FoundKey {
				removed = true
				break
			}
		}

		return nil
	})
}

// TryClearPartition is part of the cspann.Store interface. It removes vectors
// from an existing partition.
func (s *Store) TryClearPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	expected cspann.PartitionMetadata,
) (count int, err error) {
	if s.ReadOnly() {
		return count, errors.AssertionFailedf("cannot clear partition in read-only mode")
	}
	return count, s.kv.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Acquire a shared lock on the partition, to ensure that another agent
		// doesn't modify it. Also, this will be used to verify expected metadata.
		b := txn.NewBatch()
		metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
		b.GetForShare(metadataKey, kvpb.BestEffort)
		if err = txn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, "locking partition %d for add", partitionKey)
		}

		// Verify expected metadata.
		metadata, err := s.getMetadataFromKVResult(partitionKey, &b.Results[0])
		if err != nil {
			return err
		}
		if !metadata.Equal(&expected) {
			return cspann.NewConditionFailedError(metadata)
		}
		if metadata.StateDetails.State.AllowAdd() {
			return errors.AssertionFailedf(
				"cannot clear partition in state %s that allows adds", metadata.StateDetails.State)
		}

		// Clear all vectors in the partition using DelRange.
		b = txn.NewBatch()
		startKey := vecencoding.EncodeStartVectorKey(metadataKey)
		endKey := vecencoding.EncodeEndVectorKey(metadataKey)
		b.DelRange(startKey, endKey, true /* returnKeys */)
		if err = txn.CommitInBatch(ctx, b); err != nil {
			return err
		}

		count = len(b.Results[0].Keys)
		return nil
	})
}

// getMetadataFromKVResult returns the partition metadata row from the KV
// result, returning the partition's K-means tree level and centroid.
func (s *Store) getMetadataFromKVResult(
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
		return cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, s.emptyVec), nil
	}

	return vecencoding.DecodeMetadataValue(value)
}

// decodePartition decodes the metadata and data KV results into an ephemeral
// partition. This partition will become invalid when the codec is next reset,
// so it needs to be cloned if it will be used outside of the store.
func (s *Store) decodePartition(
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	codec *partitionCodec,
	metaResult, dataResult *kv.Result,
) (*cspann.Partition, error) {
	metadata, err := s.getMetadataFromKVResult(partitionKey, metaResult)
	if err != nil {
		return nil, err
	}
	if dataResult.Err != nil {
		return nil, dataResult.Err
	}
	vectorEntries := dataResult.Rows

	// Initialize the partition codec.
	// NOTE: This reuses the memory returned by the last call to decodePartition.
	codec.InitForDecoding(partitionKey, metadata, len(vectorEntries))

	// Determine the length of the prefix of vector data records.
	metadataKey := vecencoding.EncodeMetadataKey(s.prefix, treeKey, partitionKey)
	prefixLen := vecencoding.EncodedPrefixVectorKeyLen(metadataKey, metadata.Level)
	for _, entry := range vectorEntries {
		err = codec.DecodePartitionData(entry.Key[prefixLen:], entry.ValueBytes())
		if err != nil {
			return nil, err
		}
	}

	return codec.GetPartition(), nil
}

// remapConditionFailedError checks if the provided error is
// kvpb.ConditionFiledError. If so, it translates it to a corresponding
// cspann.ConditionFailedError by deserializing the partition metadata. If the
// record does not exist, it returns ErrPartitionNotFound.
func remapConditionFailedError(err error) error {
	var errConditionFailed *kvpb.ConditionFailedError
	if errors.As(err, &errConditionFailed) {
		if errConditionFailed.ActualValue == nil {
			// Metadata record does not exist.
			return cspann.ErrPartitionNotFound
		}

		encodedMetadata, err := errConditionFailed.ActualValue.GetBytes()
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"partition metadata value should always be bytes")
		}
		actualMetadata, err := vecencoding.DecodeMetadataValue(encodedMetadata)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"cannot decode partition metadata: %v", encodedMetadata)
		}
		return cspann.NewConditionFailedError(actualMetadata)
	}
	return err
}
