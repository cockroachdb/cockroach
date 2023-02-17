// Copyright 2022 The Cockroach Authors.
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

const addSqlAddrCol = `
ALTER TABLE system.sql_instances
ADD COLUMN IF NOT EXISTS "sql_addr" STRING
FAMILY "primary"
`

func alterSystemSQLInstancesAddSqlAddr(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-sql-instances-sql-addr-col",
		schemaList:     []string{"sql_addr"},
		query:          addSqlAddrCol,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, cs, d, op, keys.SQLInstancesTableID, systemschema.SQLInstancesTable()); err != nil {
		return err
	}
	return nil
}
