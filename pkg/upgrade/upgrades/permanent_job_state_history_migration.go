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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// keyVisualizerTablesMigration creates the system.span_stats_unique_keys, system.span_stats_buckets,
// system.span_stats_samples, and system.span_stats_tenant_boundaries tables.
func jobStateHistoryMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {

	err := createSystemTable(ctx, d.DB, d.Settings, keys.SystemSQLCodec,
		systemschema.JobStateHistory,
		tree.LocalityLevelTable)
	if err != nil {
		return err
	}

	return nil
}
