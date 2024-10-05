// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func tenantIDSequenceForSystemTenant(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {
	return createSystemTable(ctx, d.DB, d.Settings, keys.SystemSQLCodec, systemschema.TenantIDSequence, tree.LocalityLevelTable)
}
