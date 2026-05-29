// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// createTenantResourceGroupsTable creates the host-side
// system.tenant_resource_groups table and the per-tenant singleton
// reconciliation job that backfills it. The job creation is also done at
// tenant bootstrap; running it again here is safe because
// CreateIfNotExistAdoptableJobWithTxn no-ops when the job already exists.
func createTenantResourceGroupsTable(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSystemTable(
		ctx, d.DB, d.Settings, d.Codec,
		systemschema.TenantResourceGroupsTable, tree.LocalityLevelTable,
	); err != nil {
		return err
	}
	return resourceGroupReconciliationJobMigration(ctx, cv, d)
}
