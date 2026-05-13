// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// backfillFuncDescCanMutate conservatively sets can_mutate = true on all
// existing function descriptors. Pre-existing descriptors lack this field
// (defaulting to false), which could cause the executor to incorrectly use
// a LeafTxn for queries that call mutating routines. Setting it to true is
// always safe (it just means we use a RootTxn), while false on a mutating
// routine is a correctness issue.
func backfillFuncDescCanMutate(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Get all database descriptors.
	var databases nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		databases, err = txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}

	// Process each database separately to avoid loading all descriptors at once.
	return databases.ForEachDescriptor(func(desc catalog.Descriptor) error {
		db := desc.(catalog.DatabaseDescriptor)
		return backfillFuncDescCanMutateInDatabase(ctx, d, db)
	})
}

func backfillFuncDescCanMutateInDatabase(
	ctx context.Context, d upgrade.TenantDeps, db catalog.DatabaseDescriptor,
) error {
	// Collect function descriptor IDs that need backfill.
	var fnIDs []descpb.ID
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		all, err := txn.Descriptors().GetAllInDatabase(ctx, txn.KV(), db)
		if err != nil {
			return err
		}
		return all.ForEachDescriptor(func(desc catalog.Descriptor) error {
			if desc.DescriptorType() != catalog.Function {
				return nil
			}
			fnIDs = append(fnIDs, desc.GetID())
			return nil
		})
	}); err != nil {
		return err
	}

	if len(fnIDs) == 0 {
		return nil
	}

	log.Dev.Infof(ctx, "backfilling can_mutate on %d function descriptors in database %q",
		len(fnIDs), db.GetName())

	// Process in batches to avoid overly large transactions.
	const batchSize = 100
	for i := 0; i < len(fnIDs); i += batchSize {
		end := i + batchSize
		if end > len(fnIDs) {
			end = len(fnIDs)
		}
		batch := fnIDs[i:end]

		if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			for _, fnID := range batch {
				fn, err := txn.Descriptors().MutableByID(txn.KV()).Function(ctx, fnID)
				if err != nil {
					return err
				}
				fn.SetCanMutate(true)
				if err := txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, fn, txn.KV()); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
