// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"

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
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Store implements the cspann.Store interface for KV backed vector indices.
type Store struct {
	db descs.DB // Used to generate new partition IDs
	kv *kv.DB

	// Used for generating prefixes and reading from the PK to get full length
	// vectors.
	codec   keys.SQLCodec
	tableID catid.DescID
	indexID catid.IndexID

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
	db descs.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexID catid.IndexID,
	vectorColumnID descpb.ColumnID,
) (ps *Store, err error) {
	ps = &Store{
		db:             db,
		kv:             db.KV(),
		codec:          codec,
		tableID:        tableDesc.GetID(),
		indexID:        indexID,
		rootQuantizer:  quantize.NewUnQuantizer(quantizer.GetDims()),
		quantizer:      quantizer,
		minConsistency: kvpb.INCONSISTENT,
		emptyVec:       make(vector.T, quantizer.GetDims()),
	}

	pk := tableDesc.GetPrimaryIndex()
	ps.prefix = rowenc.MakeIndexKeyPrefix(codec, tableDesc.GetID(), indexID)
	ps.pkPrefix = rowenc.MakeIndexKeyPrefix(codec, tableDesc.GetID(), pk.GetID())

	ps.colIdxMap.Set(vectorColumnID, 0)
	err = rowenc.InitIndexFetchSpec(
		&ps.fetchSpec,
		ps.codec,
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

	return NewWithColumnID(db, quantizer, codec, tableDesc, indexID, vectorColumnID)
}

// SetConsistency sets the minimum consistency level to use when reading
// partitions. This is set to a higher level for deterministic tests.
func (s *Store) SetMinimumConsistency(consistency kvpb.ReadConsistencyType) {
	s.minConsistency = consistency
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
	return cspann.PartitionKey(unique.GenerateUniqueInt(unique.ProcessUniqueID(instanceID)))
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
	// TODO(mw5h): Implement MergeStats. We're not panicking here because some tested
	// functionality needs to call this function but does not depend on the results.
	return nil
}

// TryDeletePartition is part of the cspann.Store interface. It deletes an
// existing partition.
func (s *Store) TryDeletePartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) error {
	return errors.AssertionFailedf("TryDeletePartition is not yet implemented")
}

// TryCreateEmptyPartition is part of the cspann.Store interface. It creates a
// new partition that contains no vectors.
func (s *Store) TryCreateEmptyPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	metadata cspann.PartitionMetadata,
) error {
	return errors.AssertionFailedf("TryCreateEmptyPartition is not yet implemented")
}

// TryGetPartition is part of the cspann.Store interface. It returns an existing
// partition, including both its metadata and data.
func (s *Store) TryGetPartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	return nil, errors.AssertionFailedf("TryGetPartition is not yet implemented")
}

// TryGetPartitionMetadata is part of the cspann.Store interface. It returns the
// metadata of an existing partition.
func (s *Store) TryGetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (cspann.PartitionMetadata, error) {
	return cspann.PartitionMetadata{},
		errors.AssertionFailedf("TryGetPartitionMetadata is not yet implemented")
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
	return errors.AssertionFailedf("TryUpdatePartitionMetadata is not yet implemented")
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
	return false, errors.AssertionFailedf("TryAddToPartition is not yet implemented")
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
	return false, errors.AssertionFailedf("TryRemoveFromPartition is not yet implemented")
}

// TryClearPartition is part of the cspann.Store interface. It removes vectors
// from an existing partition.
func (s *Store) TryClearPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	expected cspann.PartitionMetadata,
) (count int, err error) {
	return -1, errors.AssertionFailedf("TryRemoveFromPartition is not yet implemented")
}
