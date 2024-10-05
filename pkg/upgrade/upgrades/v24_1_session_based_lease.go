// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// disableWritesForExpiryBasedLeases disables writing expiry based leases, ensuring
// that any new leases are session based.
func disableWritesForExpiryBasedLeases(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Bump the version on the leases table, which will trigger a full refresh
	// of all leases.
	err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		leaseTable, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, keys.LeaseTableID)
		if err != nil {
			return err
		}
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, leaseTable, txn.KV())
	})
	if err != nil {
		return err
	}

	return nil
}

// adoptUsingOnlySessionBasedLeases ensures that every active lease has a session
// bassed equivalent.
func adoptUsingOnlySessionBasedLeases(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	const countQuery = `
WITH
	current
		AS (
			SELECT
				count(*) AS matching_total
			FROM
				crdb_internal.kv_session_based_leases AS s,
				system.lease AS e
			WHERE
				e."descID" = s.desc_id
				AND s.sql_instance_id = e."nodeID"
				AND s.crdb_region = e.crdb_region
				AND e.expiration > current_timestamp()
		),
	expected
		AS (
			SELECT
				count(*) AS lease_count
			FROM
				system.lease
			WHERE
				expiration > current_timestamp()
		)
SELECT
	e.lease_count = c.matching_total as leases_are_session_based
FROM
	expected AS e, current AS c;
`
	r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: time.Millisecond * 500,
		Multiplier:     2,
		MaxBackoff:     time.Second * 10,
	})
	for r.Next() {
		sessionBasedOnly := tree.DBool(false)
		if err := deps.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return txn.WithSyntheticDescriptors(catalog.Descriptors{systemschema.LeaseTable_V23_2()}, func() error {
				row, err := txn.QueryRowEx(ctx,
					"check-for-session-based-leases-only",
					txn.KV(),
					sessiondata.InternalExecutorOverride{
						User:     username.NodeUserName(),
						Database: catconstants.SystemDatabaseName,
					},
					countQuery)
				if err != nil {
					return err
				}
				if len(row) != 1 {
					return errors.AssertionFailedf("unexpected row from session based leasing drain query %v", row)
				}
				sessionBasedOnly = tree.MustBeDBool(row[0])
				return nil
			})
		}); err != nil {
			return err
		}
		if sessionBasedOnly {
			break
		}
	}
	return nil
}

// upgradeSystemLeasesDescriptor upgrades the system.lease descriptor to be
// session based.
func upgradeSystemLeasesDescriptor(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Upgrade the descriptor in storage to have the new format.
	if err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		leaseTable, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, keys.LeaseTableID)
		if err != nil {
			return err
		}
		regionType := leaseTable.Columns[4].Type
		newLeaseTableFormat := systemschema.LeaseTable()
		oldPartitioning := leaseTable.PrimaryIndex.Partitioning
		leaseTable.PrimaryIndex = newLeaseTableFormat.TableDesc().PrimaryIndex
		leaseTable.PrimaryIndex.Partitioning = oldPartitioning
		leaseTable.Columns = newLeaseTableFormat.TableDesc().Columns
		// If we are running on multi-region serverless the region column will
		// have the type transformed. So copy that over now.
		leaseTable.Columns[4].Type = protoutil.Clone(regionType).(*types.T)
		leaseTable.Families = newLeaseTableFormat.TableDesc().Families
		leaseTable.NextColumnID = newLeaseTableFormat.TableDescriptor.GetNextColumnID()
		leaseTable.NextIndexID = newLeaseTableFormat.TableDescriptor.GetNextIndexID()
		return txn.Descriptors().WriteDesc(ctx, false, leaseTable, txn.KV())
	}); err != nil {
		return err
	}
	return bumpSystemDatabaseSchemaVersion(ctx, version, deps)
}
