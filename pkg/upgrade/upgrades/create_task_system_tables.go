// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// createTaskSystemTables creates the system.task_payloads and
// system.tenant_tasks tables.
func createTaskSystemTables(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {

	tables := []catalog.TableDescriptor{
		systemschema.SystemTaskPayloadsTable,
		systemschema.SystemTenantTasksTable,
	}

	for _, table := range tables {
		err := createSystemTable(ctx, d.DB, d.Settings, keys.SystemSQLCodec,
			table, tree.LocalityLevelTable)
		if err != nil {
			return err
		}
	}

	return nil
}
