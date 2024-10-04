// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// systemExternalConnectionsTableMigration creates the
// system.external_connections table.
func systemExternalConnectionsTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return createSystemTable(
		ctx, d.DB.KV(), d.Settings, d.Codec, systemschema.SystemExternalConnectionsTable,
	)
}
