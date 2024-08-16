// Copyright 2024 The Cockroach Authors.
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

// createPreparedTransactionsTable creates the prepared_transactions system
// table.
func createPreparedTransactionsTable(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.PreparedTransactionsTable, tree.LocalityLevelTable)
}
