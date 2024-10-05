// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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

func (w *kvWriter) versionGuard(
	ctx context.Context, txn *kv.Txn,
) (settingswatcher.VersionGuard, error) {
	return w.settingsWatcher.MakeVersionGuard(ctx, txn, clusterversion.V23_1_SystemRbrCleanup)
}

func (w *kvWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	return w.do(ctx, txn, l, func(guard settingswatcher.VersionGuard, b *kv.Batch) error {
		if guard.IsActive(clusterversion.V23_1_SystemRbrDualWrite) {
			err := w.newWriter.Insert(ctx, b, false /*kvTrace */, leaseAsRbrDatum(l)...)
			if err != nil {
				return err
			}
		}
		if !guard.IsActive(clusterversion.V23_1_SystemRbrSingleWrite) {
			err := w.oldWriter.Insert(ctx, b, false /*kvTrace */, leaseAsRbtDatum(l)...)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (w *kvWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	return w.do(ctx, txn, l, func(guard settingswatcher.VersionGuard, b *kv.Batch) error {
		if guard.IsActive(clusterversion.V23_1_SystemRbrDualWrite) {
			err := w.newWriter.Delete(ctx, b, false /*kvTrace */, leaseAsRbrDatum(l)...)
			if err != nil {
				return err
			}
		}
		if !guard.IsActive(clusterversion.V23_1_SystemRbrSingleWrite) {
			err := w.oldWriter.Delete(ctx, b, false /*kvTrace */, leaseAsRbtDatum(l)...)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type addToBatchFunc = func(settingswatcher.VersionGuard, *kv.Batch) error

func (w *kvWriter) do(
	ctx context.Context, txn *kv.Txn, lease leaseFields, addToBatch addToBatchFunc,
) error {
	run := (*kv.Txn).Run
	do := func(ctx context.Context, txn *kv.Txn) error {
		guard, err := w.versionGuard(ctx, txn)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		err = addToBatch(guard, b)
		if err != nil {
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

func leaseAsRbtDatum(l leaseFields) []tree.Datum {
	return []tree.Datum{
		tree.NewDInt(tree.DInt(l.descID)),
		tree.NewDInt(tree.DInt(l.version)),
		tree.NewDInt(tree.DInt(l.instanceID)),
		&l.expiration,
	}
}
