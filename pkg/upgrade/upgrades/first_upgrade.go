// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// RunFirstUpgradePrecondition short-circuits FirstUpgradeFromReleasePrecondition if set to false.
var RunFirstUpgradePrecondition = envutil.EnvOrDefaultBool("COCKROACH_RUN_FIRST_UPGRADE_PRECONDITION", true)

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
	if !RunFirstUpgradePrecondition {
		return nil
	}
	var all nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		all, err = txn.Descriptors().GetAll(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}
	var descsToUpdate catalog.DescriptorIDSet
	if err := all.ForEachDescriptor(func(desc catalog.Descriptor) error {
		changes := desc.GetPostDeserializationChanges()
		if !changes.HasChanges() || (changes.Len() == 1 && changes.Contains(catalog.SetModTimeToMVCCTimestamp)) {
			return nil
		}
		descsToUpdate.Add(desc.GetID())
		return nil
	}); err != nil {
		return err
	}
	return upgradeDescriptors(ctx, d, descsToUpdate)
}

// upgradeDescriptors round-trips the descriptor protobufs for the given keys
// and updates them in place if they've changed.
func upgradeDescriptors(
	ctx context.Context, d upgrade.TenantDeps, ids catalog.DescriptorIDSet,
) error {
	if ids.Empty() {
		return nil
	}
	batchSize := 100
	// Any batch size below this will use high priority.
	const HighPriBatchSize = 25
	repairBatchTimeLimit := lease.LeaseDuration.Get(&d.Settings.SV)
	currentIdx := 0
	idsToRewrite := ids.Ordered()
	for currentIdx <= len(idsToRewrite) {
		descBatch := idsToRewrite[currentIdx:min(currentIdx+batchSize, len(idsToRewrite))]
		err := timeutil.RunWithTimeout(ctx, "repair-post-deserialization", repairBatchTimeLimit, func(ctx context.Context) error {
			return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
				if batchSize <= HighPriBatchSize {
					if err := txn.KV().SetUserPriority(roachpb.MaxUserPriority); err != nil {
						return err
					}
				}
				muts, err := txn.Descriptors().MutableByID(txn.KV()).Descs(ctx, descBatch)
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
		})
		if err != nil {
			// If either the operation hits the retry limit or
			// times out, then reduce the batch size.
			if kv.IsAutoRetryLimitExhaustedError(err) ||
				errors.HasType(err, (*timeutil.TimeoutError)(nil)) {
				batchSize = max(batchSize/2, 1)
				log.Infof(ctx, "reducing batch size of invalid_object repair query to %d (hipri=%t)",
					batchSize,
					batchSize <= HighPriBatchSize)
				continue
			}
			return err
		}
		currentIdx += batchSize
	}
	return nil
}

var firstUpgradePreconditionUsesAOST = true

// TestingSetFirstUpgradePreconditionAOST allows tests to disable withAOST trick in upgrade
// precondition check, which otherwise can produce false negative.
func TestingSetFirstUpgradePreconditionAOST(enabled bool) func() {
	old := firstUpgradePreconditionUsesAOST
	firstUpgradePreconditionUsesAOST = enabled
	return func() { firstUpgradePreconditionUsesAOST = old }
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
	withAOST := firstUpgradePreconditionUsesAOST
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
			$1
	)
WHERE
	was_repaired`
		batchSize := 100
		// Any batch size below this will use high priority.
		const HighPriBatchSize = 25
		repairBatchTimeLimit := lease.LeaseDuration.Get(&d.Settings.SV)
		for {
			var rowsUpdated tree.DInt
			err := timeutil.RunWithTimeout(ctx, "descriptor-repair", repairBatchTimeLimit, func(ctx context.Context) error {
				return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
					if batchSize <= HighPriBatchSize {
						if err = txn.KV().SetUserPriority(roachpb.MaxUserPriority); err != nil {
							return err
						}
					}
					row, err := txn.QueryRow(
						ctx, "repair-catalog-corruptions", txn.KV() /* txn */, repairQuery, batchSize,
					)
					if err != nil {
						return err
					}
					rowsUpdated = tree.MustBeDInt(row[0])
					return nil
				})
			})
			if err != nil {
				// If either the operation hits the retry limit or
				// times out, then reduce the batch size.
				if kv.IsAutoRetryLimitExhaustedError(err) ||
					errors.HasType(err, (*timeutil.TimeoutError)(nil)) {
					batchSize = max(batchSize/2, 1)
					log.Infof(ctx, "reducing batch size of invalid_object repair query to %d (hipri=%t)",
						batchSize,
						batchSize <= HighPriBatchSize)
					continue
				}
				// Otherwise, return any unknown errors.
				return err
			}
			if rowsUpdated == 0 {
				break
			}
			n += int(rowsUpdated)
			log.Infof(ctx, "repaired %d catalog corruptions", rowsUpdated)
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

// newFirstUpgrade creates a TenantUpgrade that corresponds to the first
// (Vxy_zStart) internal version of a release.
func newFirstUpgrade(v roachpb.Version) *upgrade.TenantUpgrade {
	if v.Internal != 2 {
		panic("not the first internal release")
	}
	return upgrade.NewTenantUpgrade(
		firstUpgradeDescription(v),
		v,
		FirstUpgradeFromReleasePrecondition,
		FirstUpgradeFromRelease,
		upgrade.RestoreActionNotRequired("first upgrade is a persists nothing"),
	)
}

func firstUpgradeDescription(v roachpb.Version) string {
	return fmt.Sprintf("prepare upgrade to v%d.%d release", v.Major, v.Minor)
}
