// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerRoachtest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "roachtest/noop",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Owner:            registry.OwnerTestEng,
		Run:              func(_ context.Context, _ test.Test, _ cluster.Cluster) {},
		Cluster:          r.MakeClusterSpec(0),
	})
	r.Add(registry.TestSpec{
		Name:             "roachtest/noop-maybefail",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Owner:            registry.OwnerTestEng,
		Run: func(_ context.Context, t test.Test, _ cluster.Cluster) {
			if rand.Float64() <= 0.2 {
				t.Fatal("randomly failing")
			}
		},
		Cluster: r.MakeClusterSpec(0),
	})
	// This test can be run manually to check what happens if a test times out.
	// In particular, can manually verify that suitable artifacts are created.
	r.Add(registry.TestSpec{
		Name:             "roachtest/hang",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Owner:            registry.OwnerTestEng,
		Run: func(_ context.Context, t test.Test, c cluster.Cluster) {
			ctx := context.Background() // intentional
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
			time.Sleep(time.Hour)
		},
		Timeout: 3 * time.Minute,
		Cluster: r.MakeClusterSpec(3),
	})

	// Manual test for verifying framework behavior in a test failure scenario
	// via unexpected node fatal error from an explicitly monitored node
	r.Add(registry.TestSpec{
		Name:             "roachtest/manual/monitor/test-failure/node-fatal-explicit-monitor",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.ManualOnly,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			monitorFatalTest(ctx, t, c)
		},
	})

	// Manual test for verifying framework behavior in a test failure scenario
	// via unexpected node fatal error using the roachtest built-in node monitor
	r.Add(registry.TestSpec{
		Name:             "roachtest/manual/monitor/test-failure/node-fatal-global-monitor",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.ManualOnly,
		Monitor:          true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			monitorFatalTestGlobal(ctx, t, c)
		},
	})
}

// monitorFatalTest will always fail with a node logging a fatal error in a
// goroutine that is being watched by a monitor
func monitorFatalTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	m := c.NewDeprecatedMonitor(ctx, c.Node(1))
	n1 := c.Conn(ctx, t.L(), 1)
	defer n1.Close()
	require.NoError(t, n1.PingContext(ctx))

	m.Go(func(ctx context.Context) (err error) {
		_, err = n1.ExecContext(ctx, "SELECT crdb_internal.force_log_fatal('oops');")
		return err
	})
	m.Wait()
}

// monitorFatalTestGlobal will always fail with a node logging a fatal error
// not within an explicit goroutine. Expects registry.TestSpec.Monitor to be
// set to True
func monitorFatalTestGlobal(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	n1 := c.Conn(ctx, t.L(), 1)
	defer n1.Close()
	require.NoError(t, n1.PingContext(ctx))

	_, err := n1.ExecContext(ctx, "SELECT crdb_internal.force_log_fatal('oops');")
	if err != nil {
		t.L().Printf("Error executing query: %s", err)
	}
}
