// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// FirstUpgradeFromRelease implements the tenant upgrade step for all
// V[0-9]+_[0-9]+Start cluster versions, which is every first internal version
// following each major release version.
//
// The purpose of this upgrade function is to keep the data in the system
// database up to date to mitigate the impact of metadata corruptions
// caused by bugs in earlier versions.
//
// As of the time of this writing, this implementation covers only the
// system.descriptor table, but in the future this may grow to
// include other system tables as well.
func FirstUpgradeFromRelease(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	var all nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		all, err = txn.Descriptors().GetAll(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}
	var batch catalog.DescriptorIDSet
	const batchSize = 1000
	if err := all.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if !desc.GetPostDeserializationChanges().HasChanges() {
			return nil
		}
		batch.Add(desc.GetID())
		if batch.Len() >= batchSize {
			if err := upgradeDescriptors(ctx, d, batch); err != nil {
				return err
			}
			batch = catalog.MakeDescriptorIDSet()
		}
		return nil
	}); err != nil {
		return err
	}
	return upgradeDescriptors(ctx, d, batch)
}

// upgradeDescriptors round-trips the descriptor protobufs for the given keys
// and updates them in place if they've changed.
func upgradeDescriptors(
	ctx context.Context, d upgrade.TenantDeps, ids catalog.DescriptorIDSet,
) error {
	if ids.Empty() {
		return nil
	}
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		muts, err := txn.Descriptors().MutableByID(txn.KV()).Descs(ctx, ids.Ordered())
		if err != nil {
			return err
		}
		b := txn.KV().NewBatch()
		for _, mut := range muts {
			if !mut.GetPostDeserializationChanges().HasChanges() {
				continue
			}
			key := catalogkeys.MakeDescMetadataKey(d.Codec, mut.GetID())
			b.CPut(key, mut.DescriptorProto(), mut.GetRawBytesInStorage())
		}
		return txn.KV().Run(ctx, b)
	})
}

// FirstUpgradeFromReleasePrecondition is the precondition check for upgrading
// from any supported major release.
//
// This precondition function performs health checks on the catalog metadata.
// This prevents cluster version upgrades from proceeding when it detects
// known corruptions. This forces cluster operations to repair the metadata,
// which may be somewhat annoying, but it's considered a lesser evil to being
// stuck in a mixed-version state due to a later upgrade step throwing
// errors.
//
// The implementation of this function may evolve in the future to include
// other checks beyond querying the invalid_objects virtual table, which
// mainly checks descriptor validity, as that has historically been the main
// source of problems.
func FirstUpgradeFromReleasePrecondition(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// For performance reasons, we look back in time when performing
	// a diagnostic query. If no corruptions were found back then, we assume that
	// there are no corruptions now. Otherwise, we retry and do everything
	// without an AOST clause henceforth.
	withAOST := true
	diagnose := func(tbl string) (hasRows bool, err error) {
		q := fmt.Sprintf("SELECT count(*) FROM \"\".crdb_internal.%s", tbl)
		if withAOST {
			q = q + " AS OF SYSTEM TIME '-10s'"
		}
		row, err := d.InternalExecutor.QueryRow(ctx, "query-"+tbl, nil /* txn */, q)
		if err == nil && row[0].String() != "0" {
			hasRows = true
		}
		return hasRows, err
	}
	// Check for possibility of time travel.
	if hasRows, err := diagnose("databases"); err != nil {
		return err
	} else if !hasRows {
		// We're looking back in time to before the cluster was bootstrapped
		// and no databases exist at that point. Disable time-travel henceforth.
		withAOST = false
	}
	// Check for repairable catalog corruptions.
	if hasRows, err := diagnose("kv_repairable_catalog_corruptions"); err != nil {
		return err
	} else if hasRows {
		// Attempt to repair catalog corruptions in batches.
		log.Info(ctx, "auto-repairing catalog corruptions detected during upgrade attempt")
		var n int
		const repairQuery = `
SELECT
	count(*)
FROM
	(
		SELECT
			crdb_internal.repair_catalog_corruption(id, corruption) AS was_repaired
		FROM
			"".crdb_internal.kv_repairable_catalog_corruptions
		LIMIT
			1000
	)
WHERE
	was_repaired`
		for {
			row, err := d.InternalExecutor.QueryRow(
				ctx, "repair-catalog-corruptions", nil /* txn */, repairQuery,
			)
			if err != nil {
				return err
			}
			c := tree.MustBeDInt(row[0])
			if c == 0 {
				break
			}
			n += int(c)
			log.Infof(ctx, "repaired %d catalog corruptions", c)
		}
		if n == 0 {
			log.Info(ctx, "no catalog corruptions found to repair during upgrade attempt")
		} else {
			// Repairs have actually been performed: stop all time travel henceforth.
			withAOST = false
			log.Infof(ctx, "%d catalog corruptions have been repaired in total", n)
		}
	}
	// Check for all known catalog corruptions.
	if hasRows, err := diagnose("invalid_objects"); err != nil {
		return err
	} else if !hasRows {
		return nil
	}
	if !withAOST {
		return errors.AssertionFailedf("\"\".crdb_internal.invalid_objects is not empty")
	}
	// At this point, corruptions were found using the AS OF SYSTEM TIME clause.
	// Re-run the diagnosis without the clause, because we might not be seeing
	// repairs which might have taken place recently.
	withAOST = false
	if hasRows, err := diagnose("invalid_objects"); err != nil {
		return err
	} else if !hasRows {
		return nil
	}
	return errors.AssertionFailedf("\"\".crdb_internal.invalid_objects is not empty")
}
