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
// system.tenant_resource_groups table that holds the reconciled view of every
// tenant's resource group configurations.
func createTenantResourceGroupsTable(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return createSystemTable(
		ctx, d.DB, d.Settings, d.Codec,
		systemschema.TenantResourceGroupsTable, tree.LocalityLevelTable,
	)
}
