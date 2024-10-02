// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// sqlInstancesAddDrainingMigration adds a new column `is_draining` to the
// system.sql_instances table.
func sqlInstancesAddDrainingMigration(
	ctx context.Context, cs clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	finalDescriptor := systemschema.SQLInstancesTable()
	// Replace the stored descriptor with the bootstrap descriptor.
	err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		expectedDesc := finalDescriptor.TableDesc()
		mutableDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, expectedDesc.GetID())
		if err != nil {
			return err
		}
		version := mutableDesc.Version
		mutableDesc.TableDescriptor = *protoutil.Clone(expectedDesc).(*descpb.TableDescriptor)
		mutableDesc.Version = version
		return txn.Descriptors().WriteDesc(ctx, false, mutableDesc, txn.KV())
	})
	if err != nil {
		return errors.Wrapf(err, "unable to replace system descriptor for system.%s (%+v)",
			finalDescriptor.GetName(), finalDescriptor)
	}
	return err
}
