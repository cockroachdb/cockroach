// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// addClusterMetricsUpdaterJob creates the cluster metrics updater job.
func addClusterMetricsUpdaterJob(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createClusterMetricsUpdaterJob(ctx, version, d); err != nil {
		return err
	}
	return nil
}
