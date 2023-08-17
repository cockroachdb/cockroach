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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func ensureExecInsightsRowLevelTTLSchedule(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	tableNames := []string{string(catconstants.StmtExecInsightsTableName), string(catconstants.TxnExecInsightsTableName)}
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {

		for _, tableName := range tableNames {
			idRow, err := ie.QueryRowEx(ctx, "get-table-id", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				fmt.Sprintf("SELECT 'system.%s'::regclass::oid", tableName),
			)
			if err != nil {
				return err
			}
			tableID := tree.MustBeDOid(idRow[0]).Oid

			_, err = ie.ExecEx(
				ctx,
				fmt.Sprintf("validate row level TTL job for system.%s", tableName),
				txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"select crdb_internal.repair_ttl_table_scheduled_job($1)",
				tableID,
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
