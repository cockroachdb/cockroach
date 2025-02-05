// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
)

// runResumeChangefeeds resumes all paused changefeed jobs by interacting with the cluster.
func runResumeChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	// Establish a connection to the cluster.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()
	err := changeStateForChangefeed(ctx, o, conn, "RESUME", []jobs.Status{jobs.StatusPaused},
		[]jobs.Status{jobs.StatusRunning})
	if err != nil {
		o.Fatal(err)
	}

	return nil // No cleanup operation needed.
}
