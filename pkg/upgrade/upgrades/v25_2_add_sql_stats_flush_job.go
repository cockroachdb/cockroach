// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// addSqlStatsFlushJob creates the sql stats flush job.
func addSqlStatsFlushJob(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSqlStatsFlushJob(ctx, version, d); err != nil {
		return err
	}
	return nil
}
