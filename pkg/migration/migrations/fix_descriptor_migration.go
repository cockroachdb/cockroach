// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// fixDescriptorMigration calls RunPostDeserializationChanges on every descriptor.
func fixDescriptorMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	fixDescriptorFunc := func(ids []descpb.ID, descs []descpb.Descriptor, timestamps []hlc.Timestamp) error {
		for i, id := range ids {
			b := catalogkv.NewBuilderWithMVCCTimestamp(&descs[i], timestamps[i])
			if b == nil {
				return errors.Newf("unable to find descriptor for id %d", id)
			}

			err := b.RunPostDeserializationChanges(ctx, nil /* DescGetter */)
			if err != nil {
				return err
			}
			mutableDesc := b.BuildExistingMutable()

			if mutableDesc.HasPostDeserializationChanges() {
				// Only need to fix the descriptor if there was a change.
				if err := fixDescriptor(ctx, d, mutableDesc.GetID(), mutableDesc.GetVersion()); err != nil {
					return err
				}
			}
		}
		return nil
	}

	query := `SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC`
	rows, err := d.InternalExecutor.QueryIterator(
		ctx, "fix-privileges", nil /* txn */, query,
	)
	if err != nil {
		return err
	}

	return descriptorUpgradeMigration(ctx, rows, fixDescriptorFunc, 524288 /* batchSizeInBytes */)
}

// fixDescriptor grabs a descriptor using it's ID and fixes the descriptor
// by running RunPostDeserializationChanges.
func fixDescriptor(
	ctx context.Context, d migration.TenantDeps, id descpb.ID, version descpb.DescriptorVersion,
) error {
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
		// GetMutableDescriptorByID calls RunPostDeserializationChanges which
		// fixes the descriptor.
		desc, err := descriptors.GetMutableDescriptorByID(ctx, id, txn)
		if err != nil {
			return err
		}
		if desc.GetVersion() > version {
			// Already rewritten.
			return nil
		}
		log.Infof(ctx, "upgrading descriptor with id %d", desc.GetID())
		return descriptors.WriteDesc(ctx, false /* kvTrace */, desc, txn)
	})
}

// fixDescriptorFunction is used in descriptorUpgradeMigration to fix a set
// of descriptors specified by the id.
type fixDescriptorFunction func(ids []descpb.ID, descs []descpb.Descriptor, timestamps []hlc.Timestamp) error

// descriptorUpgradeMigration is an abstraction for a descriptor upgrade migration.
// The rows provided should be the result of a select id, descriptor, crdb_internal_mvcc_timestamp
// from system.descriptor table.
// The datums returned from the query are parsed to grab the descpb.Descriptor
// and fixDescriptorFunction is called on the desc.
// If minBatchSizeInBytes is specified, fixDescriptor will only be called once the
// size of the descriptors in the id array surpasses minBatchSizeInBytes.
func descriptorUpgradeMigration(
	ctx context.Context,
	rows sqlutil.InternalRows,
	fixDescFunc fixDescriptorFunction,
	minBatchSizeInBytes int,
) error {
	defer func() { _ = rows.Close() }()
	ok, err := rows.Next(ctx)
	if err != nil {
		return err
	}
	currSize := 0 // in bytes.
	var ids []descpb.ID
	var descs []descpb.Descriptor
	var timestamps []hlc.Timestamp
	for ; ok; ok, err = rows.Next(ctx) {
		datums := rows.Cur()
		id, desc, ts, err := unmarshalDescFromDescriptorRow(datums)
		if err != nil {
			return err
		}
		ids = append(ids, id)
		descs = append(descs, desc)
		timestamps = append(timestamps, ts)
		currSize += desc.Size()
		if currSize > minBatchSizeInBytes || minBatchSizeInBytes == 0 {
			err = fixDescFunc(ids, descs, timestamps)
			if err != nil {
				return err
			}
			// Reset size and id array after the batch is fixed.
			currSize = 0
			ids = nil
			descs = nil
			timestamps = nil
		}
	}
	// Fix remaining descriptors.
	return fixDescFunc(ids, descs, timestamps)
}

// unmarshalDescFromDescriptorRow takes in an InternalRow from a query that gets:
// id, descriptor, crdb_internal_mvcc_timestamp from the system.descriptor table.
// ie: SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC
// and parses the id, descriptor and mvcc_timestamp fields.
func unmarshalDescFromDescriptorRow(
	datums tree.Datums,
) (descpb.ID, descpb.Descriptor, hlc.Timestamp, error) {
	id := descpb.ID(*datums[0].(*tree.DInt))
	ts, err := tree.DecimalToHLC(&datums[2].(*tree.DDecimal).Decimal)
	if err != nil {
		return id, descpb.Descriptor{}, ts, errors.Wrapf(err,
			"failed to convert MVCC timestamp decimal to HLC for ID %d", id)
	}
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(([]byte)(*datums[1].(*tree.DBytes)), &desc); err != nil {
		return id, descpb.Descriptor{}, ts, errors.Wrapf(err,
			"failed to unmarshal descriptor with ID %d", id)
	}
	return id, desc, ts, nil
}
