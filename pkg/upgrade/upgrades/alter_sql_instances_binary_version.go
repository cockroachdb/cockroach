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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addBinaryVersionColumnToSQLInstancesTableStmt = `
	ALTER TABLE system.sql_instances
	ADD COLUMN IF NOT EXISTS binary_version STRING
	FAMILY "primary"
`

func alterSystemSQLInstancesTableAddBinaryVersion(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	tableID := catid.DescID(keys.SQLInstancesTableID)
	for _, op := range []operation{
		{
			name:           "add-sql-instances-binary-version-column",
			schemaList:     []string{"binary_version"},
			query:          addBinaryVersionColumnToSQLInstancesTableStmt,
			schemaExistsFn: columnExists,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, tableID, systemschema.SQLInstancesTable()); err != nil {
			return err
		}
	}
	return nil
}
