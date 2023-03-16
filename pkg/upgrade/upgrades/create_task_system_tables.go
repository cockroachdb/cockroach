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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
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
		err := createSystemTable(ctx, d.DB.KV(), d.Settings, keys.SystemSQLCodec,
			table)
		if err != nil {
			return err
		}
	}

	return nil
}
