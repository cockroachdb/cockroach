// Copyright 2024 The Cockroach Authors.
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
