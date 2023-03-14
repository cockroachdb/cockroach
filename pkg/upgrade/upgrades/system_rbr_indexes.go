// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// The migrations in this file convert the system.sqliveness,
// system.sql_instance, and system.lease tables to have a primary index that is
// binary compatible with a regional by row table. The migration is guarded by
// four version gates. The version gates step the crdb servers through a
// migration that is similar to an alter primary key operation. The migration
// is hand coded because the tables are used to bootstrap the SQL layer and may
// not use SQL.
//
//	# V23_1_SystemRbrDualWrite
//  CRDB begins writing to the old and new indexes.
//
//	# V23_1_SystemRbrReadNew
//  The backFillRegionalByRowIndex upgrade job is attached to this version. It
//  backfills the new index then upgrades the descriptor to use the new index.
//  The descriptor is not used internally, but clients can use it to inspect
//  the system table. After the upgrade finishes, the version is advanced and
//  CRDB begins reading the new value.
//
//  # V23_1_SystemRbrSingleWrite
//  Now that all nodes are reading the new value, nodes only need to write to
//  the new value.
//
//	# V23_1_SystemRbrCleanup
//  The cleanUpRegionalByTableIndex is attached to this version gate and will
//  delete data associated with the old index.

// backfillRegionalByRowIndex copies values in the source index regional by
// table index into the regional by row index. It must run before servers can
// read from the new index.
func backfillRegionalByRowIndex(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for _, migration := range migrations(deps.Codec) {
		if err := migrateTableToRbrIndex(ctx, migration, deps); err != nil {
			return errors.Wrapf(err, "unable to backfill system.%s's regional by row compatible index", migration.tableName)
		}
	}
	return nil
}

// cleanUpRegionalByTableIndex deletes all kvs in the old regional by table
// index. Deleting the old index bytes is not needed for correctness, but it
// does save a few bytes and may avoid issues down the road.
func cleanUpRegionalByTableIndex(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for _, migration := range migrations(deps.Codec) {
		if err := deleteOldIndex(ctx, migration, deps); err != nil {
			return errors.Wrapf(err, "unable to delete system.%s's original index", migration.tableName)
		}
	}
	return nil
}

func migrations(codec keys.SQLCodec) (result []rbrMigration) {
	return []rbrMigration{
		sqlLivenessMigration(codec),
		sqlInstanceMigration(codec),
		// leaseMigration(codec),
	}
}

func sqlLivenessMigration(codec keys.SQLCodec) rbrMigration {
	descriptor := systemschema.SqllivenessTable()
	return rbrMigration{
		tableName:       "sqlliveness",
		keyMapper:       makeKeyMapper(codec, descriptor, 1),
		finalDescriptor: descriptor,
	}
}

func sqlInstanceMigration(codec keys.SQLCodec) rbrMigration {
	descriptor := systemschema.SQLInstancesTable()
	return rbrMigration{
		tableName:       "sql_instances",
		keyMapper:       makeKeyMapper(codec, descriptor, 1),
		finalDescriptor: descriptor,
	}
}

// TODO(jeffswenson): enable this migration
//func leaseMigration(codec keys.SQLCodec) rbrMigration {
//	descriptor := systemschema.LeaseTable()
//	return rbrMigration{
//		tableName:       "lease",
//		keyMapper:       makeKeyMapper(codec, descriptor, 1),
//		finalDescriptor: descriptor,
//	}
//}

type rbrMigration struct {
	tableName       string
	keyMapper       prefixKeyMapper
	finalDescriptor catalog.TableDescriptor
}

type batchMigrator func(ctx context.Context, txn *kv.Txn, rows []kv.KeyValue) error

// batchIterate is based on txn.Iterate, but each batch of keys is wrapped in
// its own transaction. The migration does not need to occur in a single
// transaction. Attempting to use a single transaction would likely cause
// problems for the lease table, which may be large and serves high priority
// heartbeat traffic.
func batchIterate(
	ctx context.Context, db *kv.DB, begin, end interface{}, pageSize int, f batchMigrator,
) error {
	singleBatch := func(start interface{}) (next interface{}, err error) {
		err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			next = nil
			rows, err := txn.Scan(ctx, start, end, int64(pageSize))
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return nil
			}
			if err := f(ctx, txn, rows); err != nil {
				return err
			}
			if pageSize == len(rows) {
				next = rows[len(rows)-1].Key.Next()
			}
			return nil
		})
		return next, err
	}
	for {
		var err error
		begin, err = singleBatch(begin)
		if err != nil {
			return err
		}
		if begin == nil {
			return nil
		}
	}
}

func migrateTableToRbrIndex(
	ctx context.Context, migration rbrMigration, deps upgrade.TenantDeps,
) error {
	// batchSize is intended to be large enough to improve efficiency and small
	// enough to avoid contention issues.
	const batchSize = 64

	oldIndex := migration.keyMapper.OldPrefix()

	// Copy values from the old index into the new index. This must run after
	// all servers have started dual writing and before servers start reading
	// from the new index.
	err := batchIterate(ctx, deps.KVDB, oldIndex.Clone(), oldIndex.PrefixEnd(), batchSize,
		func(ctx context.Context, txn *kv.Txn, rows []kv.KeyValue) error {
			batch := txn.NewBatch()
			for i := range rows {
				if !rows[i].Value.IsPresent() {
					// skip tombstones
					continue
				}

				destKey, err := migration.keyMapper.OldToNew(rows[i].Key)
				if err != nil {
					return err
				}

				// The batch API expects the timestamp to be empty.
				rows[i].Value.Timestamp = hlc.Timestamp{}

				// The checksum includes the key. Since we are changing the
				// key, we need to change the checksum.
				rows[i].Value.ClearChecksum()

				batch.Put(destKey, rows[i].Value)
			}
			return txn.CommitInBatch(ctx, batch)
		})
	if err != nil {
		return errors.Wrap(err, "unable to back fill rbr index")
	}

	// Replace the stored descriptor with the bootstrap descriptor.
	err = deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		expectedDesc := migration.finalDescriptor.TableDesc()

		mutableDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, expectedDesc.GetID())
		if err != nil {
			return err
		}

		version := mutableDesc.Version

		mutableDesc.TableDescriptor = *expectedDesc
		mutableDesc.Version = version

		return txn.Descriptors().WriteDesc(ctx, false, mutableDesc, txn.KV())
	})
	if err != nil {
		return errors.Wrapf(err, "unable to replace system descriptor for system.%s (%+v)", migration.tableName, migration.finalDescriptor)
	}

	// The LeaseManager.WaitForOneVersion is here to ensure sql servers pick up
	// the new descriptor before dual writing is stopped or the old index is
	// deleted.
	//
	// This is particulary important for the lease table, because sql is used
	// to find inactive lease entries and if versions advance before the
	// descriptor is picked up, a schema change could violate the leasing
	// protocol.
	_, err = deps.LeaseManager.WaitForOneVersion(ctx, migration.finalDescriptor.GetID(), retry.Options{
		InitialBackoff: time.Millisecond,
		Multiplier:     1.5,
		MaxBackoff:     time.Second,
	})
	return err
}

func deleteOldIndex(ctx context.Context, migration rbrMigration, deps upgrade.TenantDeps) error {
	oldIndex := migration.keyMapper.OldPrefix()
	return deps.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := txn.DelRange(ctx, oldIndex, oldIndex.PrefixEnd(), false /*return keys*/)
		return err
	})
}

// prefixKeyMapper maps keys from the old index format to the new index format.
// It's implemented in a generic way that takes advantage of the fact the
// transformation for all three tables is equivalent at the binary level.
//
// Logically prefixKeyMapper takes a key that looks something like:
// /Tenant/<tenant>/Table/<table>/<old_index>/<some_key...>/<column_family>
// and rewrites it to
// /Tenant/<tenant>/Table/<table>/<new_index>/enum.One/<some_key...>/<column_family>
type prefixKeyMapper struct {
	codec     keys.SQLCodec
	oldPrefix roachpb.Key
	newPrefix roachpb.Key
}

func makeKeyMapper(
	codec keys.SQLCodec, newDescriptor catalog.TableDescriptor, oldIndex uint32,
) prefixKeyMapper {
	newPrefix := codec.IndexPrefix(uint32(newDescriptor.GetID()), uint32(newDescriptor.GetPrimaryIndexID()))
	newPrefix = encoding.EncodeBytesAscending(newPrefix, enum.One)
	return prefixKeyMapper{
		codec:     codec,
		oldPrefix: codec.IndexPrefix(uint32(newDescriptor.GetID()), oldIndex),
		newPrefix: newPrefix,
	}
}

func (l *prefixKeyMapper) OldToNew(key roachpb.Key) (roachpb.Key, error) {
	if !bytes.HasPrefix(key, l.oldPrefix) {
		return nil, errors.Newf("unexpected table prefix: %v", key)
	}
	rem := key[len(l.oldPrefix):]

	return append(l.newPrefix.Clone(), rem...), nil
}

func (l *prefixKeyMapper) OldPrefix() roachpb.Key {
	return l.oldPrefix.Clone()
}

func (l *prefixKeyMapper) NewPrefix() roachpb.Key {
	return l.newPrefix.Clone()
}
