// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// sqlInstancesAddDrainingMigration adds a new column `is_draining` to the
// system.sql_instances table.
func sqlInstancesAddDrainingMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Update the sql_instance table to add the is_draining column.
	err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		_, err := txn.Exec(ctx, "add-draining-column", txn.KV(),
			"ALTER TABLE system.sql_instances ADD COLUMN IF NOT EXISTS is_draining BOOL NULL")
		return err
	})
	if err != nil {
		return errors.Wrapf(err, "unable to add column to system.sql_instances")
	}
	return err
}
