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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
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
		err := createSystemTable(ctx, d.DB.KV(), d.Settings, d.Codec,
			table)
		if err != nil {
			return err
		}
	}

	return nil
}
