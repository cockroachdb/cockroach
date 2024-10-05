// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// systemExecInsightsTableMigration creates the system.insights table.
func systemExecInsightsTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TransactionExecInsightsTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.StatementExecInsightsTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	return nil
}
