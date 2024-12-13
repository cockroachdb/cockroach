// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
)

// PersistentStore implements the Store interface for KV backed vector indices.
type PersistentStore struct {
	db            *kv.DB // Needed for index maintenance functions
	quantizer     quantize.Quantizer
	rootQuantizer quantize.Quantizer

	prefix roachpb.Key
}

var _ Store = (*PersistentStore)(nil)

// NewPersistentStore creates a vecstore.Store interface backed by the KV for a
// single vector index.
func NewPersistentStore(
	db *kv.DB, quantizer quantize.Quantizer, prefix roachpb.Key,
) *PersistentStore {
	ps := PersistentStore{
		db:            db,
		quantizer:     quantizer,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetOriginalDims()),
		prefix:        prefix,
	}

	return &ps
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
