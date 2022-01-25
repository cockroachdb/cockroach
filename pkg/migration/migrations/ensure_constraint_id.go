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

func ensureTablesHasConstraintID(ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job) error {
	return d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB,
		func(ctx context.Context, txn *kv.Txn, collections *descs.Collection) error {
			// Get all possible table descriptors and rewrite them out to
			// have constraint IDs.
			descriptors, err := collections.GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			batch := txn.NewBatch()
			for _, descriptor := range descriptors {
				if descriptor.DescriptorType() != catalog.Table {
					continue
				}
				table, err := collections.GetMutableTableByID(ctx, txn, descriptor.GetID(), tree.ObjectLookupFlagsWithRequired())
				if err != nil {
					return err
				}
				// PostDeserializationDescriptorChanges migration will update the
				// descriptor to add constraint IDs. We will just write them to the
				// batch.
				table.MaybeIncrementVersion()
				if err := collections.WriteDescToBatch(ctx, false, table, batch); err != nil {
					return err
				}
			}
			return txn.Run(ctx, batch)
		})
}
