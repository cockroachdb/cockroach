// Copyright 2026 The Cockroach Authors.
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

// SQLInstancesAddLocalityAddressesColumn adds the locality_addresses column
// to the system.sql_instances table, enabling locality-aware address resolution.
func SQLInstancesAddLocalityAddressesColumn(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-locality-addresses-column",
		schemaList:     []string{"locality_addresses"},
		schemaExistsFn: columnExists,
		query:          `ALTER TABLE system.sql_instances ADD COLUMN IF NOT EXISTS locality_addresses JSONB DEFAULT NULL FAMILY "primary"`,
	}
	return migrateTable(ctx, cv, deps, op, keys.SQLInstancesTableID, systemschema.SQLInstancesTable())
}
