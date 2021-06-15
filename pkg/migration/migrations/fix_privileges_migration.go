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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func fixPrivilegesMigration(
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
		row := rows.Cur()
		id := descpb.ID(*row[0].(*tree.DInt))
		ts, err := tree.DecimalToHLC(&row[2].(*tree.DDecimal).Decimal)
		if err != nil {
			return errors.Wrapf(err,
				"failed to convert MVCC timestamp decimal to HLC for ID %d", id)
		}
		var desc descpb.Descriptor
		if err := protoutil.Unmarshal(([]byte)(*row[1].(*tree.DBytes)), &desc); err != nil {
			return errors.Wrapf(err,
				"failed to unmarshal descriptor with ID %d", id)
		}
		b := catalogkv.NewBuilderWithMVCCTimestamp(&desc, ts)
		if b == nil {
			return errors.Newf("unable to find descriptor for id %d", id)
		}

		mutableDesc := b.BuildExistingMutable()
		privilegeDesc := mutableDesc.GetPrivileges()

		var objectType privilege.ObjectType
		switch b.DescriptorType() {
		case catalog.Database:
			objectType = privilege.Database
		case catalog.Schema:
			objectType = privilege.Schema
		case catalog.Table:
			objectType = privilege.Table
		case catalog.Type:
			objectType = privilege.Type
		}

		err = descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			if privilegeDesc.Version >= descpb.Version21_2 {
				log.Infof(ctx, "privilege descriptor has already been fixed, skipping %d", id)
				return nil
			}

			privs := mutableDesc.GetPrivileges()
			descpb.MaybeFixPrivileges(
				mutableDesc.GetID(), mutableDesc.GetParentID(),
				&privs, objectType,
			)

			return descriptors.WriteDesc(ctx, false /* kvTrace */, mutableDesc, txn)
		})

		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return nil
}
