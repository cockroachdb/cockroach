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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
)

// PersistentStore implements the Store interface for KV backed vector indices.
type PersistentStore struct {
	db *kv.DB // Used to generate new partition IDs

	// Used for generating prefixes and reading from the PK to get full length
	// vectors.
	codec keys.SQLCodec
	table catalog.TableDescriptor
	index catalog.Index

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

// NewPersistentStore creates a vecstore.Store interface backed by the KV for a
// single vector index.
func NewPersistentStore(
	db *kv.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	index catalog.Index,
) (ps *PersistentStore, err error) {
	// TODO (mw5h): Check for staleness of the table descriptor when we create a new persistentStoreTxn.
	ps = &PersistentStore{
		db:            db,
		codec:         codec,
		table:         table,
		index:         index,
		quantizer:     quantizer,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetOriginalDims()),
	}

	ps.prefix = rowenc.MakeIndexKeyPrefix(codec, table.GetID(), index.GetID())
	ps.pkPrefix = rowenc.MakeIndexKeyPrefix(codec, table.GetID(), table.GetPrimaryIndex().GetID())

	vectorColumnID := ps.index.GetKeyColumnID(ps.index.NumKeyColumns() - 1)
	ps.colIdxMap.Set(vectorColumnID, 0)

	err = rowenc.InitIndexFetchSpec(
		&ps.fetchSpec,
		ps.codec,
		ps.table,
		ps.table.GetPrimaryIndex(),
		[]descpb.ColumnID{vectorColumnID},
	)

	return ps, err
}

// Begin is part of the vecstore.Store interface. Begin creates a new KV
// transaction on behalf of the user and prepares it to operate on the persistent
// vector store.
func (s *PersistentStore) Begin(ctx context.Context) (Txn, error) {
	return NewPersistentStoreTxn(s, s.db.NewTxn(ctx, "vecstore.PersistentStore begin transaction")), nil
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
	panic("MergeStats() unimplemented")
}
