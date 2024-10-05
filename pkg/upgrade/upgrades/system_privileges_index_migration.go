// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func alterSystemPrivilegesAddSecondaryIndex(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()

	// Query the table ID for the system.privileges table since it is dynamically
	idRow, err := ie.QueryRowEx(ctx, "get-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.privileges'::regclass::oid`,
	)
	if err != nil {
		return err
	}

	op := operation{
		name:           "add-secondary-index-system-priv",
		schemaList:     []string{"privileges_path_username_key"},
		query:          `CREATE UNIQUE INDEX IF NOT EXISTS privileges_path_username_key ON system.privileges (path,username) STORING (privileges, grant_options)`,
		schemaExistsFn: hasIndex,
	}

	tableID := descpb.ID(tree.MustBeDOid(idRow[0]).Oid)
	if err := migrateTable(ctx, cs, d, op, tableID, systemschema.SystemPrivilegeTable); err != nil {
		return err
	}

	return nil
}
