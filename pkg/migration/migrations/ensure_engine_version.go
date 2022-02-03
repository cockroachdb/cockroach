// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// ensureEngineVersionAtLeast waits for the engine version to be at least
// compatible with the given clusterversion.ClusterVersion, on all nodes in the
// cluster.
func ensureEngineVersionAtLeast(
	ctx context.Context, v clusterversion.ClusterVersion, deps migration.SystemDeps, _ *jobs.Job,
) error {
	return deps.Cluster.UntilClusterStable(ctx, func() error {
		return deps.Cluster.ForEveryNode(ctx, "ensure-engine-version",
			func(ctx context.Context, client serverpb.MigrationClient) error {
				req := &serverpb.WaitForEngineVersionRequest{
					Version: &v.Version,
				}
				_, err := client.WaitForEngineVersion(ctx, req)
				return err
			})
	})
}
