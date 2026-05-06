// Copyright 2026 The Cockroach Authors.
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

// createResourceGroupsTable creates the system.resource_groups table and the
// system.resource_group_id_seq sequence used by the resource manager.
func createResourceGroupsTable(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSystemTable(
		ctx, d.DB, d.Settings, d.Codec, systemschema.ResourceGroupsTable, tree.LocalityLevelTable,
	); err != nil {
		return err
	}
	return createSystemTable(
		ctx, d.DB, d.Settings, d.Codec, systemschema.ResourceGroupIDSequence, tree.LocalityLevelTable,
	)
}
