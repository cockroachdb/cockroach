// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// systemStatementStatisticsActivityTableMigration creates the
// system.statement_activity and system.transaction_activity table.
func systemStatisticsActivityTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	tables := []catalog.TableDescriptor{
		systemschema.StatementActivityTable,
		systemschema.TransactionActivityTable,
	}

	for _, table := range tables {
		err := createSystemTable(ctx, d.DB, d.Settings, d.Codec,
			table, tree.LocalityLevelTable)
		if err != nil {
			return err
		}
	}

	return nil
}
