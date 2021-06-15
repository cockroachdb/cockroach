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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// fixDescriptorMigration calls RunPostDeserializationChanges on every descriptor.
func fixDescriptorMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	rows, err := d.InternalExecutor.QueryIterator(ctx, "fix-privileges", nil, /* txn */
		`
SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	ok, err := rows.Next(ctx)
	for ; ok; ok, err = rows.Next(ctx) {
		datums := rows.Cur()
		desc, err := getMutableDescFromDescriptorRow(datums)
		if err != nil {
			return err
		}
		if err := fixDescriptor(ctx, desc.GetID(), d, desc.GetVersion()); err != nil {
			return err
		}
	}
	return err
}

// getMutableDescFromDescriptorRow takes in an InternalRow from the query:
// SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC
// and parses the id, mvcc_timestamp and descriptor fields to create and return
// a MutableDescriptor.
func getMutableDescFromDescriptorRow(datums tree.Datums) (catalog.MutableDescriptor, error) {
	id := descpb.ID(*datums[0].(*tree.DInt))
	ts, err := tree.DecimalToHLC(&datums[2].(*tree.DDecimal).Decimal)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to convert MVCC timestamp decimal to HLC for ID %d", id)
	}
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(([]byte)(*datums[1].(*tree.DBytes)), &desc); err != nil {
		return nil, errors.Wrapf(err,
			"failed to unmarshal descriptor with ID %d", id)
	}

	b := catalogkv.NewBuilderWithMVCCTimestamp(&desc, ts)
	if b == nil {
		return nil, errors.Newf("unable to find descriptor for id %d", id)
	}

	return b.BuildExistingMutable(), nil
}

// fixDescriptor grabs a descriptor using it's ID and fixes the descriptor
// by running RunPostDeserializationChanges.
func fixDescriptor(
	ctx context.Context, id descpb.ID, d migration.TenantDeps, version descpb.DescriptorVersion,
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
		if !desc.GetChanged() {
			// No changes to be made.
			return nil
		}
		log.Infof(ctx, "upgrading descriptor with id %d", desc.GetID())
		return descriptors.WriteDesc(ctx, false /* kvTrace */, desc, txn)
	})
}
