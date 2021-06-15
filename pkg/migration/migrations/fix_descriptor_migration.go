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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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
		desc, err := getMutableDescFromDescriptorRow(rows)
		if err != nil {
			return err
		}
		if desc.GetIsRepaired() {
			continue
		}
		if err := runPostDeserializationChanges(ctx, desc, d); err != nil {
			return err
		}
	}
	return err
}

// getMutableDescFromDescriptorRow takes in an InternalRow from the query:
// SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor ORDER BY ID ASC
// and parses the id, mvcc_timestamp and descriptor fields to create and return
// a MutableDescriptor.
func getMutableDescFromDescriptorRow(rows sqlutil.InternalRows) (catalog.MutableDescriptor, error) {
	row := rows.Cur()
	id := descpb.ID(*row[0].(*tree.DInt))
	ts, err := tree.DecimalToHLC(&row[2].(*tree.DDecimal).Decimal)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to convert MVCC timestamp decimal to HLC for ID %d", id)
	}
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(([]byte)(*row[1].(*tree.DBytes)), &desc); err != nil {
		return nil, errors.Wrapf(err,
			"failed to unmarshal descriptor with ID %d", id)
	}

	b := catalogkv.NewBuilderWithMVCCTimestamp(&desc, ts)
	if b == nil {
		return nil, errors.Newf("unable to find descriptor for id %d", id)
	}

	return b.BuildExistingMutable(), nil
}

func runPostDeserializationChanges(
	ctx context.Context, desc catalog.MutableDescriptor, d migration.TenantDeps,
) error {
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
		desc := catalogkv.NewBuilder(desc.DescriptorProto())
		dg := catalogkv.NewOneLevelUncachedDescGetter(txn, d.Codec)
		changed, err := desc.RunPostDeserializationChanges(ctx, dg)
		if err != nil {
			return err
		}
		if changed {
			writeDesc := desc.BuildExistingMutable()
			writeDesc.SetIsRepaired()
			log.Infof(ctx, "upgrading descriptor with id %d", writeDesc.GetID())
			return descriptors.WriteDesc(ctx, false /* kvTrace */, writeDesc, txn)
		}
		return nil
	})
}
