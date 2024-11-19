// Copyright 2024 The Cockroach Authors.
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

// createTenantSystemTables creates the system tables in app tenants that were
// previously missing due to only being present in the system tenant. A change
// was made in 24.1 to always create system tables in both app and system
// tenants, even if they were not used in app tenants. Without this migration,
// we're in an intermediate state where newly created app tenants have all
// system tables, but older app tenants are missing some of the tables.
func createTenantSystemTables(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TenantsTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TenantSettingsTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TenantUsageTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.SpanConfigurationsTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.SystemTaskPayloadsTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.SystemTenantTasksTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TenantIDSequence, tree.LocalityLevelTable); err != nil {
		return err
	}
	return nil
}
