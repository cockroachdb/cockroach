// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// kvWriter implements writer using the raw KV API.
type kvWriter struct {
	db *kv.DB
	w  bootstrap.KVWriter
}

func newKVWriter(codec keys.SQLCodec, db *kv.DB, id descpb.ID) *kvWriter {
	return &kvWriter{
		db: db,
		w:  bootstrap.MakeKVWriter(codec, leaseTableWithID(id)),
	}
}

func leaseTableWithID(id descpb.ID) catalog.TableDescriptor {
	if id == keys.LeaseTableID {
		return systemschema.LeaseTable
	}
	// Custom IDs are only used for testing.
	mut := systemschema.LeaseTable.NewBuilder().
		BuildExistingMutable().(*tabledesc.Mutable)
	mut.ID = id
	return mut.ImmutableCopy().(catalog.TableDescriptor)
}

func (w *kvWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	return w.do(ctx, txn, l, func(b *kv.Batch, datum ...tree.Datum) error {
		return w.w.Insert(ctx, b, false /* kvTrace */, datum...)
	})
}

func (w *kvWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	return w.do(ctx, txn, l, func(b *kv.Batch, datum ...tree.Datum) error {
		return w.w.Delete(ctx, b, false /* kvTrace */, datum...)
	})
}

type addToBatchFunc = func(*kv.Batch, ...tree.Datum) error

func (w *kvWriter) do(ctx context.Context, txn *kv.Txn, l leaseFields, f addToBatchFunc) error {
	run := (*kv.Txn).Run
	do := func(ctx context.Context, txn *kv.Txn) error {
		b, err := newBatch(txn, l, f)
		if err != nil {
			return err
		}
		return run(txn, ctx, b)
	}
	if txn != nil {
		return do(ctx, txn)
	}
	run = (*kv.Txn).CommitInBatch
	return w.db.Txn(ctx, do)
}

func newBatch(txn *kv.Txn, l leaseFields, f addToBatchFunc) (*kv.Batch, error) {
	entries := [...]tree.Datum{
		tree.NewDInt(tree.DInt(l.descID)),
		tree.NewDInt(tree.DInt(l.version)),
		tree.NewDInt(tree.DInt(l.instanceID)),
		&l.expiration,
	}
	b := txn.NewBatch()
	if err := f(b, entries[:]...); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "failed to encode lease entry")
	}
	return b, nil
}
