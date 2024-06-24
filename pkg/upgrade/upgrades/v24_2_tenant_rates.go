// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// tenantRatesMigration adds two columns to the system.tenant_usage table that
// store consumption rates (e.g. write batches per second) used for tenant cost
// modeling.
func tenantRatesMigration(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:       "add-tenant-usage-rates",
			schemaList: []string{"current_rates, next_rates"},
			query: `ALTER TABLE system.tenant_usage ` +
				`ADD COLUMN IF NOT EXISTS current_rates BYTES FAMILY "primary", ` +
				`ADD COLUMN IF NOT EXISTS next_rates BYTES FAMILY "primary"`,
			schemaExistsFn: hasColumn,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.TenantUsageTableID,
			systemschema.TenantUsageTable); err != nil {
			return err
		}
	}
	return nil
}
