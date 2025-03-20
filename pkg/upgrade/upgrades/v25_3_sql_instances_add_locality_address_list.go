// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func sqlInstancesAddLocalityAddressListColumn(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// This upgrade adds a new column to the sql_instances table to store the list of locality aware addresses.
	// The upgrade is idempotent and can be run multiple times without any issues.
	op := operation{
		name:           "add-locality-address-list-column",
		schemaList:     []string{"locality_address_list"},
		schemaExistsFn: columnExists,
		query: `ALTER TABLE system.sql_instances 
  		ADD COLUMN IF NOT EXISTS locality_address_list JSONB DEFAULT NULL FAMILY "primary"`,
		// The locality_address_list column won't need to be backfilled and is nullable.
	}
	return migrateTable(ctx, cv, deps, op, keys.SQLInstancesTableID, systemschema.SQLInstancesTable())
}
