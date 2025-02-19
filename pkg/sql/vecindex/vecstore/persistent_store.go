// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/veclib"
)

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

// Create a PersistentStore for an index on the provided table descriptor using
// the provided column ID as the vector column for the index. This is used in
// unit tests where full vector index creation capabilities aren't necessarily
// available.
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
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetDims()),
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

	vectorColumnID := index.VectorColumnID()

	return NewPersistentStoreWithColumnID(ctx, db, quantizer, codec, tableDesc, indexID, vectorColumnID)
}

// Begin is part of the vecstore.Store interface. Begin creates a new KV
// transaction on behalf of the user and prepares it to operate on the persistent
// vector store.
func (s *PersistentStore) Begin(ctx context.Context, w *veclib.Workspace) (Txn, error) {
	return NewPersistentStoreTxn(w, s, s.db.KV().NewTxn(ctx, "vecstore.PersistentStore begin transaction")), nil
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
