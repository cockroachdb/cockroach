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

// runCancelChangefeeds cancels a running or paused changefeed job.
func runCancelChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {

	// Connect to the cluster and ensure the connection is closed after use.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()
	err := changeStateOrCreateChangefeed(ctx, o, conn, "CANCEL", []jobs.State{jobs.StateRunning, jobs.StatePaused},
		[]jobs.State{jobs.StateCanceled})
	if err != nil {
		o.Fatal(err)
	}

	// Return a nil cleanup function as there's no specific cleanup required here.
	return nil
}
