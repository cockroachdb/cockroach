// Copyright 2025 The Cockroach Authors.
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

// createWorkSpanTable creates the system.work_span table for query
// observability through work span capture.
func createWorkSpanTable(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return createSystemTable(
		ctx, d.DB, d.Settings, d.Codec, systemschema.WorkSpanTable, tree.LocalityLevelTable,
	)
}
