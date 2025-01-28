// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// InitRootPartition is called during a schema change that creates a vector
// index. It initializes the keyspace with a root partition. It does not require
// an active VectorIndex or PersistentStore instance.
func InitRootPartition(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	tableID descpb.ID,
	indexID descpb.IndexID,
	dims int,
) error {
	// NOTE: we don't have to worry about ExternalRowData here, because this
	// function is only called during index creation.
	prefix := rowenc.MakeIndexKeyPrefix(codec, tableID, indexID)
	key := EncodePartitionKey(prefix, RootKey)
	rootCentroid := make(vector.T, dims)
	meta, err := EncodePartitionMetadata(LeafLevel, rootCentroid)
	if err != nil {
		return err
	}
	b := txn.NewBatch()
	b.Put(key, meta)
	return txn.Run(ctx, b)
}

// PersistentStore implements the Store interface for KV backed vector indices.
type PersistentStore struct {
	db descs.DB // Used to generate new partition IDs

	// Used for generating prefixes and reading from the PK to get full length
	// vectors.
	codec   keys.SQLCodec
	tableID catid.DescID
	indexID catid.IndexID

	// The root partition always uses the UnQuantizer while other partitions may use
	// any quantizer.
	rootQuantizer quantize.Quantizer
	quantizer     quantize.Quantizer

	prefix    roachpb.Key            // KV prefix for the vector index.
	pkPrefix  roachpb.Key            // KV prefix for the primary key.
	fetchSpec fetchpb.IndexFetchSpec // A pre-built fetch spec for this index.
	colIdxMap catalog.TableColMap    // A column map for extracting full sized vectors from the PK
}

var _ Store = (*PersistentStore)(nil)

func NewPersistentStoreWithColumnID(
	ctx context.Context,
	db descs.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexID catid.IndexID,
	vectorColumnID descpb.ColumnID,
) (ps *PersistentStore, err error) {
	ps = &PersistentStore{
		db:            db,
		codec:         codec,
		tableID:       tableDesc.GetID(),
		indexID:       indexID,
		quantizer:     quantizer,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetOriginalDims()),
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
	return ps, nil
}

// NewPersistentStore creates a vecstore.Store interface backed by the KV for a
// single vector index.
func NewPersistentStore(
	ctx context.Context,
	db descs.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	tableID catid.DescID,
	indexID catid.IndexID,
) (ps *PersistentStore, err error) {
	// TODO (mw5h): Check for staleness of the table descriptor when we create a new persistentStoreTxn.
	var tableDesc catalog.TableDescriptor
	err = db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		tableDesc, err = txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get().Table(ctx, tableID)
		return err
	})

	var index catalog.Index
	for _, desc := range tableDesc.DeletableNonPrimaryIndexes() {
		if desc.GetID() == indexID {
			index = desc
			break
		}
	}

	vectorColumnID := index.GetKeyColumnID(index.NumKeyColumns() - 1)

	return NewPersistentStoreWithColumnID(ctx, db, quantizer, codec, tableDesc, indexID, vectorColumnID)
}

// Begin is part of the vecstore.Store interface. Begin creates a new KV
// transaction on behalf of the user and prepares it to operate on the persistent
// vector store.
func (s *PersistentStore) Begin(ctx context.Context) (Txn, error) {
	return newPersistentStoreTxn(s, s.db.KV().NewTxn(ctx, "vecstore.PersistentStore begin transaction")), nil
}

// Commit is part of the vecstore.Store interface. Commit commits the
// underlying KV transaction wrapped by the vecstore.Txn passed in.
func (s *PersistentStore) Commit(ctx context.Context, txn Txn) error {
	return txn.(*persistentStoreTxn).kv.Commit(ctx)
}

// Abort is part of the vecstore.Store interface. Abort causes the underlying
// KV transaction wrapped by the passed vecstore.Txn to roll back.
func (s *PersistentStore) Abort(ctx context.Context, txn Txn) error {
	return txn.(*persistentStoreTxn).kv.Rollback(ctx)
}

// MergeStats is part of the vecstore.Store interface.
func (s *PersistentStore) MergeStats(ctx context.Context, stats *IndexStats, skipMerge bool) error {
	// TODO(mw5h): Implement MergeStats. We're not panicking here because some tested
	// functionality needs to call this function but does not depend on the results.
	return nil
}

// WrapTxn wraps the passed KV transaction in a vecstore.Txn. This allows vector
// search operations to be performed in an existing transaction.
func (s *PersistentStore) WrapTxn(txn *kv.Txn) Txn {
	return newPersistentStoreTxn(s, txn)
}

// QuantizeAndEncode returns the quantized and encoded form of the given vector.
// TODO(drewk): this probably needs some refactoring.
func (s *PersistentStore) QuantizeAndEncode(
	ctx context.Context, partition PartitionKey, centroid, v vector.T,
) (quantized []byte, err error) {
	workspace := &internal.Workspace{}
	ctx = internal.WithWorkspace(ctx, workspace)

	// Determine the correct quantizer for the partition.
	var quantizer quantize.Quantizer
	if partition == RootKey {
		quantizer = s.rootQuantizer
	} else {
		quantizer = s.quantizer
	}

	// Randomize the query vector, if the quantizer requires it.
	tempRandomized := workspace.AllocVector(quantizer.GetRandomDims())
	defer workspace.FreeVector(tempRandomized)
	quantizer.RandomizeVector(ctx, v, tempRandomized, false /* invert */)

	// Quantizer and encode the randomized vector.
	codec := newPersistentStoreCodec(quantizer)
	return codec.encodeVector(ctx, tempRandomized, centroid)
}
