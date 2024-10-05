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

// addSpanCountTable creates the system.span_counts table if it does not exist.
// It should already exist in recently bootstrapped secondary tenants but not in
// system tenants.
func addSpanCountTable(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.SpanCountTable, tree.LocalityLevelTable); err != nil {
		return err
	}
	return bumpSystemDatabaseSchemaVersion(ctx, cv, d)
}
