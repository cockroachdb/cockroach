// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func hidePayloadProgressFromSystemJobs(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	if err := deps.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		// No idempotent RENAME … IF EXISTS, so use a txn and check the catalog.
		if rows, err := txn.QueryRow(ctx, "check-payload-exists", txn.KV(),
			`SELECT table_name, column_name FROM system.information_schema.columns WHERE table_name = 'jobs' AND column_name = 'payload'`,
		); err != nil || rows == nil {
			return err
		}
		for _, stmt := range []string{
			`ALTER TABLE system.jobs RENAME COLUMN payload TO dropped_payload`,
			`ALTER TABLE system.jobs RENAME COLUMN progress TO dropped_progress`,
			`ALTER TABLE system.jobs ALTER COLUMN dropped_payload SET NOT VISIBLE, ALTER COLUMN dropped_progress SET NOT VISIBLE`,
		} {
			if _, err := txn.ExecEx(ctx, "hide-legacy-payload-progress", txn.KV(),
				sessiondata.NodeUserSessionDataOverride, stmt,
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return bumpSystemDatabaseSchemaVersion(ctx, cv, deps)
}
