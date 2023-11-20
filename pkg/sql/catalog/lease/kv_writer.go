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
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
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

	oldWriter bootstrap.KVWriter
	newWriter bootstrap.KVWriter

	settingsWatcher *settingswatcher.SettingsWatcher
}

func newKVWriter(
	codec keys.SQLCodec, db *kv.DB, id descpb.ID, settingsWatcher *settingswatcher.SettingsWatcher,
) *kvWriter {
	return &kvWriter{
		db:              db,
		newWriter:       bootstrap.MakeKVWriter(codec, leaseTableWithID(id)),
		oldWriter:       bootstrap.MakeKVWriter(codec, systemschema.V22_2_LeaseTable()),
		settingsWatcher: settingsWatcher,
	}
}

func leaseTableWithID(id descpb.ID) catalog.TableDescriptor {
	if id == keys.LeaseTableID {
		return systemschema.LeaseTable()
	}
	// Custom IDs are only used for testing.
	mut := systemschema.LeaseTable().NewBuilder().
		BuildExistingMutable().(*tabledesc.Mutable)
	mut.ID = id
	return mut.ImmutableCopy().(catalog.TableDescriptor)
}

func (w *kvWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	return w.do(ctx, txn, l, func(b *kv.Batch) error {
		err := w.newWriter.Insert(ctx, b, false /*kvTrace */, leaseAsRbrDatum(l)...)
		if err != nil {
			return err
		}
		return nil
	})
}

func (w *kvWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	return w.do(ctx, txn, l, func(b *kv.Batch) error {
		err := w.newWriter.Delete(ctx, b, false /*kvTrace */, leaseAsRbrDatum(l)...)
		if err != nil {
			return err
		}
		return nil
	})
}

type addToBatchFunc = func(*kv.Batch) error

func (w *kvWriter) do(
	ctx context.Context, txn *kv.Txn, lease leaseFields, addToBatch addToBatchFunc,
) error {
	run := (*kv.Txn).Run
	do := func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		if err := addToBatch(b); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "failed to encode lease entry")
		}
		return run(txn, ctx, b)
	}
	if txn != nil {
		return do(ctx, txn)
	}
	run = (*kv.Txn).CommitInBatch
	return w.db.Txn(ctx, do)
}

func leaseAsRbrDatum(l leaseFields) []tree.Datum {
	return []tree.Datum{
		tree.NewDInt(tree.DInt(l.descID)),
		tree.NewDInt(tree.DInt(l.version)),
		tree.NewDInt(tree.DInt(l.instanceID)),
		&l.expiration,
		tree.NewDBytes(tree.DBytes(l.regionPrefix)),
	}

}
