// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addOwnerIDColumnToExternalConnectionsTableStmt = `
ALTER TABLE system.external_connections
ADD COLUMN IF NOT EXISTS owner_id OID
FAMILY "primary"
`

func alterExternalConnectionsTableAddOwnerIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()

	// Query the table ID for system.external_connections since it is dynamically
	// assigned.
	idRow, err := ie.QueryRowEx(ctx, "get-table-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT 'system.external_connections'::regclass::oid`,
	)
	if err != nil {
		return err
	}
	tableID := descpb.ID(tree.MustBeDOid(idRow[0]).Oid)

	for _, op := range []operation{
		{
			name:           "add-owner-id-column-external-connections-table",
			schemaList:     []string{"owner_id"},
			query:          addOwnerIDColumnToExternalConnectionsTableStmt,
			schemaExistsFn: columnExists,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, tableID, systemschema.SystemExternalConnectionsTable); err != nil {
			return err
		}
	}

	return nil
}

const backfillOwnerIDColumnExternalConnectionsTableStmt = `
UPDATE system.external_connections
SET owner_id = user_id
FROM system.users
WHERE owner_id IS NULL AND owner = username
LIMIT 1000
`

func backfillExternalConnectionsTableOwnerIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	for {
		rowsAffected, err := ie.ExecEx(ctx, "backfill-owner-id-col-external_connections-table", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			backfillOwnerIDColumnExternalConnectionsTableStmt,
		)
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			break
		}
	}

	return nil
}
