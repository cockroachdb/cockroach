// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// Used to write leases that have will no longer have an expiration,
	// but have their lifetime tied to a sqlliveness session.
	sessionBasedWriter bootstrap.KVWriter
	// Used to write leases that would use an expiration time
	// previously.
	expiryBasedWriter bootstrap.KVWriter

	settingsWatcher    *settingswatcher.SettingsWatcher
	sessionBasedReader sessionBasedLeasingModeReader
}

func newKVWriter(
	codec keys.SQLCodec,
	db *kv.DB,
	id descpb.ID,
	settingsWatcher *settingswatcher.SettingsWatcher,
	sessionModeReader sessionBasedLeasingModeReader,
) *kvWriter {
	return &kvWriter{
		db:                 db,
		sessionBasedWriter: bootstrap.MakeKVWriter(codec, leaseTableWithID(id, systemschema.LeaseTable())),
		expiryBasedWriter:  bootstrap.MakeKVWriter(codec, leaseTableWithID(id, systemschema.LeaseTable_V23_2())),
		settingsWatcher:    settingsWatcher,
		sessionBasedReader: sessionModeReader,
	}
}

func leaseTableWithID(id descpb.ID, table systemschema.SystemTable) catalog.TableDescriptor {
	if id == keys.LeaseTableID {
		return table
	}
	// Custom IDs are only used for testing.
	mut := table.NewBuilder().
		BuildExistingMutable().(*tabledesc.Mutable)
	mut.ID = id
	return mut.ImmutableCopy().(catalog.TableDescriptor)
}

func (w *kvWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	if err := w.do(ctx, txn, l, func(b *kv.Batch) error {
		// We support writing both session based and expiry based leases within
		// the KV writer. To be able to support a migration between the two types
		// of writer will in some cases need to be able to write both types of leases.
		// As a result based on our currently active mode, determine which types
		// of leases should be written. The scenarios we support are:
		// 1) Session Based Off => Only expiry based leases are written.
		// 2) Dual-Write => Both session and expiry based leases will be written.
		// 3) Session Only => Only session based leases will get written.
		if w.sessionBasedReader.sessionBasedLeasingModeAtLeast(ctx, SessionBasedDualWrite) &&
			l.sessionID != nil {
			err := w.sessionBasedWriter.Insert(ctx, b, false /*kvTrace*/, leaseAsSessionBasedDatum(l)...)
			if err != nil {
				return err
			}
		}
		if !w.sessionBasedReader.sessionBasedLeasingModeAtLeast(ctx, SessionBasedOnly) {
			err := w.expiryBasedWriter.Insert(ctx, b, false /*kvTrace */, leaseAsRbrDatum(l)...)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to insert lease %v", l)
	}
	return nil
}

func (w *kvWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	if err := w.do(ctx, txn, l, func(b *kv.Batch) error {
		// We support deleting both session based and expiry based leases within
		// the KV writer. To be able to support a migration between the two types
		// of writer will in some cases need to be able to delete both types of leases.
		// As a result based on our currently active mode, determine which types
		// of leases should be deleted. The scenarios we support are:
		// 1) Session Based Off => Only expiry based leases are deleted.
		// 2) Dual-Write => Both session and expiry based leases will be deleted.
		// 3) Session Only => Only session based leases will get deleted.
		if w.sessionBasedReader.sessionBasedLeasingModeAtLeast(ctx, SessionBasedDualWrite) &&
			l.sessionID != nil {
			err := w.sessionBasedWriter.Delete(ctx, b, false /*kvTrace*/, leaseAsSessionBasedDatum(l)...)
			if err != nil {
				return err
			}
		}
		if !w.sessionBasedReader.sessionBasedLeasingModeAtLeast(ctx, SessionBasedOnly) {
			err := w.expiryBasedWriter.Delete(ctx, b, false /*kvTrace */, leaseAsRbrDatum(l)...)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to delete lease: %v", l)
	}
	return nil
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

func leaseAsSessionBasedDatum(l leaseFields) []tree.Datum {
	return []tree.Datum{
		tree.NewDInt(tree.DInt(l.descID)),
		tree.NewDInt(tree.DInt(l.version)),
		tree.NewDInt(tree.DInt(l.instanceID)),
		tree.NewDBytes(tree.DBytes(l.sessionID)),
		tree.NewDBytes(tree.DBytes(l.regionPrefix)),
	}
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
