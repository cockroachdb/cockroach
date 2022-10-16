// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type kvWriter struct {
	codec      keys.SQLCodec
	db         *kv.DB
	desc       catalog.TableDescriptor
	primaryIdx catalog.Index
	colMap     catalog.TableColMap
}

func newKVWriter(codec keys.SQLCodec, db *kv.DB, id descpb.ID) *kvWriter {
	mut := systemschema.LeaseTable.NewBuilder().BuildExistingMutable().
		ImmutableCopy().NewBuilder().BuildExistingMutable().(*tabledesc.Mutable)
	mut.ID = id
	desc := mut.ImmutableCopy().(catalog.TableDescriptor)
	return &kvWriter{
		codec:      codec,
		db:         db,
		desc:       desc,
		primaryIdx: desc.GetPrimaryIndex(),
		colMap:     catalog.ColumnIDToOrdinalMap(desc.PublicColumns()),
	}
}

func (w *kvWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	del := func(b *kv.Batch, e *rowenc.IndexEntry) { b.Del(e.Key) }
	return w.do(ctx, txn, l, del)
}

func (w *kvWriter) do(
	ctx context.Context, txn *kv.Txn, l leaseFields, f func(b *kv.Batch, e *rowenc.IndexEntry),
) error {
	entries, err := w.encodeEntries(l)
	if err != nil {
		return err
	}
	makeBatch := func(txn *kv.Txn) *kv.Batch {
		b := txn.NewBatch()
		for _, e := range entries {
			f(b, &e)
		}
		return b
	}
	if txn != nil {
		return txn.Run(ctx, makeBatch(txn))
	}
	return w.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.CommitInBatch(ctx, makeBatch(txn))
	})
}

func (w *kvWriter) encodeEntries(l leaseFields) ([]rowenc.IndexEntry, error) {
	const includeEmpty = false
	entries, err := rowenc.EncodePrimaryIndex(w.codec, w.desc, w.primaryIdx, w.colMap, []tree.Datum{
		tree.NewDInt(tree.DInt(l.descID)),
		tree.NewDInt(tree.DInt(l.version)),
		tree.NewDInt(tree.DInt(l.instanceID)),
		&l.expiration,
	}, includeEmpty)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to encode lease table entries")
	}
	return entries, nil
}

func (w *kvWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	insert := func(b *kv.Batch, e *rowenc.IndexEntry) {
		b.CPutAllowingIfNotExists(e.Key, &e.Value, nil)
	}
	return w.do(ctx, txn, l, insert)
}
