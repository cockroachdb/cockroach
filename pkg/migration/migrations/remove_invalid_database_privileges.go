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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func convertIncompatibleDatabasePrivilegesToDefaultPrivileges(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	return d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB,
		func(ctx context.Context, txn *kv.Txn, collections *descs.Collection) error {
			descriptors, err := collections.GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			batch := txn.NewBatch()
			err = descriptors.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
				if desc.DescriptorType() != catalog.Database {
					return nil
				}
				database, err := collections.GetMutableDatabaseByName(ctx, txn, desc.GetName(), tree.DatabaseLookupFlags{})
				if err != nil {
					return err
				}
				// MaybeIncrementVersion will cause PostDeserializationDescriptorChanges
				// to be called which will convert the incompatible database privileges
				// to default privileges.
				database.MaybeIncrementVersion()
				return collections.WriteDescToBatch(ctx, false, database, batch)
			})
			if err != nil {
				return err
			}
			return txn.Run(ctx, batch)
		})
}
