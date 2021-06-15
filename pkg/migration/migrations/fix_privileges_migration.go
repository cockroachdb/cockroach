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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// fixPrivilegesMigration calls descpb.MaybeFixPrivileges on every PrivilegeDescriptor
// that is not yet on Version21_2. After the migration, all PrivilegeDescriptors
// should have valid privileges, have it's owner field populated and be on
// Version21_2.
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
		desc, err := getMutableDescFromDescriptorRow(rows)
		if err != nil {
			return err
		}
		if desc.GetPrivileges().Version >= descpb.Version21_2 {
			continue
		}
		if err := upgradePrivileges(ctx, desc, d); err != nil {
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

func descriptorTypeToObjectType(b catalog.MutableDescriptor) privilege.ObjectType {
	switch b.DescriptorType() {
	case catalog.Database:
		return privilege.Database
	case catalog.Schema:
		return privilege.Schema
	case catalog.Table:
		return privilege.Table
	case catalog.Type:
		return privilege.Type
	default:
		panic(fmt.Sprintf("unexpected descriptor type: %s", b.DescriptorType()))
	}
}

func upgradePrivileges(
	ctx context.Context, desc catalog.MutableDescriptor, d migration.TenantDeps,
) error {
	objectType := descriptorTypeToObjectType(desc)
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {

		privs := desc.GetPrivileges()
		descpb.MaybeFixPrivileges(
			desc.GetID(), desc.GetParentID(),
			&privs, objectType,
		)

		log.Infof(ctx, "upgrading PrivilegeDescriptor for descriptor with id %d", desc.GetID())
		return descriptors.WriteDesc(ctx, false /* kvTrace */, desc, txn)
	})
}
