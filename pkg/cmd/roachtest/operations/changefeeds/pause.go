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

// runPauseChangefeeds pauses a percentage of running changefeed jobs.
func runPauseChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {

	// Connect to the cluster and ensure the connection is closed after use.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()
	err := changeStateOrCreateChangefeed(ctx, o, conn, "PAUSE", []jobs.State{jobs.StateRunning},
		[]jobs.State{jobs.StatePaused})
	if err != nil {
		o.Fatal(err)
	}

	// Return a nil cleanup function as there's no specific cleanup required here.
	return nil
}
