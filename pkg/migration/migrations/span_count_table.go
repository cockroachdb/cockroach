// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// spanCountTableMigration creates the system.span_count table for secondary
// tenants.
func spanCountTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	if d.Codec.ForSystemTenant() {
		return nil // only applicable for secondary tenants
	}

	return createSystemTable(
		ctx, d.DB, d.Codec, systemschema.SpanCountTable,
	)
}

// seedSpanCountTableMigration seeds system.span_count with data for existing
// secondary tenants.
func seedSpanCountTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	if d.Codec.ForSystemTenant() {
		return nil // only applicable for secondary tenants
	}

	return d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
		dbs, err := descriptors.GetAllDatabaseDescriptors(ctx, txn)
		if err != nil {
			return err
		}

		var spanCount int
		for _, db := range dbs {
			if db.GetID() == systemschema.SystemDB.GetID() {
				continue // we don't count system table descriptors
			}

			tables, err := descriptors.GetAllTableDescriptorsInDatabase(ctx, txn, db.GetID())
			if err != nil {
				return err
			}

			for _, table := range tables {
				splits, err := d.SpanConfig.Splitter.Splits(ctx, table)
				if err != nil {
					return err
				}
				spanCount += splits
			}
		}

		const seedSpanCountStmt = `
INSERT INTO system.span_count (span_count) VALUES ($1)
ON CONFLICT (singleton)
DO UPDATE SET span_count = $1
RETURNING span_count
`
		datums, err := d.InternalExecutor.QueryRowEx(ctx, "seed-span-count", txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			seedSpanCountStmt, spanCount)
		if err != nil {
			return err
		}
		if len(datums) != 1 {
			return errors.AssertionFailedf("expected to return 1 row, return %d", len(datums))
		}
		if insertedSpanCount := int64(tree.MustBeDInt(datums[0])); insertedSpanCount != int64(spanCount) {
			return errors.AssertionFailedf("expected to insert %d, got %d", spanCount, insertedSpanCount)
		}
		return nil
	})
}

func hotRangesTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	if d.Codec.ForSystemTenant() {
		return nil // only applicable for secondary tenants
	}

	return createSystemTable(
		ctx, d.DB, d.Codec, systemschema.HotRangesTable,
	)
}
