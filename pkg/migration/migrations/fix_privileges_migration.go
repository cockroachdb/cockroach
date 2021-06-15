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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func fixPrivilegesMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	var lastUpgradedID descpb.ID
	for {
		done, idToUpgrade, objectType, err := findNextDescriptorToFix(ctx, d.InternalExecutor, lastUpgradedID)
		if err != nil || done {
			return err
		}
		if err := fixPrivileges(ctx, idToUpgrade, d, objectType); err != nil {
			return err
		}
		lastUpgradedID = idToUpgrade
	}
}

func fixPrivileges(
	ctx context.Context, upgrade descpb.ID, d migration.TenantDeps, objectType privilege.ObjectType,
) error {
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		var desc catalog.MutableDescriptor
		var err error
		var privilegeDesc *descpb.PrivilegeDescriptor
		switch objectType {
		case privilege.Table:
			desc, err = descriptors.GetMutableTableByID(ctx, txn, upgrade, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return err
			}
			privilegeDesc = desc.GetPrivileges()
		case privilege.Database:
			desc, err = descriptors.GetMutableDatabaseByID(ctx, txn, upgrade, tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return err
			}
			privilegeDesc = desc.GetPrivileges()
		case privilege.Schema:
			desc, err = descriptors.GetMutableSchemaByID(ctx, txn, upgrade, tree.SchemaLookupFlags{Required: true})
			if err != nil {
				return err
			}
			privilegeDesc = desc.GetPrivileges()
		case privilege.Type:
			desc, err = descriptors.GetMutableTypeByID(ctx, txn, upgrade, tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{Required: true}},
			)
			privilegeDesc = desc.GetPrivileges()
		}

		if privilegeDesc.Version >= descpb.Version21_2 {
			log.Infof(ctx, "privilege descriptor has already been fixed, skipping %d", upgrade)
			return nil
		}

		for i := range privilegeDesc.Users {
			privilegeDesc.Users[i].Privileges &= privilege.GetValidPrivilegesForObject(objectType).ToBitField()
		}
		if privilegeDesc.Owner().Undefined() {
			if desc.GetID() == keys.SystemDatabaseID || desc.GetParentID() == keys.SystemDatabaseID {
				privilegeDesc.SetOwner(security.NodeUserName())
			}
			privilegeDesc.SetOwner(security.RootUserName())
		}

		if privilegeDesc.Version < descpb.Version21_2 {
			privilegeDesc.SetVersion(descpb.Version21_2)
		}

		return descriptors.WriteDesc(ctx, false /* kvTrace */, desc, txn)
	})
}

func findNextDescriptorToFix(
	ctx context.Context, ie sqlutil.InternalExecutor, lastScannedID descpb.ID,
) (done bool, idToUpgrade descpb.ID, objectType privilege.ObjectType, _ error) {
	rows, err := ie.QueryIterator(ctx, "fix-table-privileges", nil, /* txn */
		`
SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor WHERE id > $1 ORDER BY ID ASC
`, lastScannedID)
	if err != nil {
		return false, 0, privilege.Any, err
	}
	defer func() { _ = rows.Close() }()
	ok, err := rows.Next(ctx)
	for ; ok; ok, err = rows.Next(ctx) {
		row := rows.Cur()
		id := descpb.ID(*row[0].(*tree.DInt))
		ts, err := tree.DecimalToHLC(&row[2].(*tree.DDecimal).Decimal)
		if err != nil {
			return false, 0, privilege.Any, errors.Wrapf(err,
				"failed to convert MVCC timestamp decimal to HLC for ID %d", id)
		}
		var desc descpb.Descriptor
		if err := protoutil.Unmarshal(([]byte)(*row[1].(*tree.DBytes)), &desc); err != nil {
			return false, 0, privilege.Any, errors.Wrapf(err,
				"failed to unmarshal descriptor with ID %d", id)
		}
		table, db, typ, schema := descpb.FromDescriptorWithMVCCTimestamp(&desc, ts)

		switch {
		case table != nil:
			if !table.Dropped() && needsPrivilegeFix(table.GetPrivileges(), privilege.Table) {
				return false, id, privilege.Table, nil
			}
		case db != nil:
			if !db.Dropped() && needsPrivilegeFix(db.GetPrivileges(), privilege.Database) {
				return false, id, privilege.Database, nil
			}
		case schema != nil:
			if !schema.Dropped() && needsPrivilegeFix(schema.GetPrivileges(), privilege.Schema) {
				return false, id, privilege.Schema, nil
			}
		case typ != nil:
			if !typ.Dropped() && needsPrivilegeFix(typ.GetPrivileges(), privilege.Type) {
				return false, id, privilege.Type, nil
			}
		}
	}
	if err != nil {
		return false, 0, privilege.Any, err
	}
	return true, 0, privilege.Any, nil
}

func needsPrivilegeFix(privDesc *descpb.PrivilegeDescriptor, objectType privilege.ObjectType) bool {
	if privDesc.Version < descpb.Version21_2 {
		return true
	}
	if privDesc.Owner().Undefined() {
		return true
	}
	for _, u := range privDesc.Users {
		if u.Privileges&privilege.GetValidPrivilegesForObject(objectType).ToBitField() != u.Privileges {
			return true
		}
	}

	return false
}
