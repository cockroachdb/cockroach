// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerDRPC(r registry.Registry) {
	{
		r.Add(registry.TestSpec{
			Name:             "drpc/tpcc/enableDRPC=true",
			Owner:            registry.OwnerServer,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.OnlyLocal,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTPCCWorkload(ctx, t, c, true)
			},
		})
		r.Add(registry.TestSpec{
			Name:             "drpc/tpcc/enableDRPC=false",
			Owner:            registry.OwnerServer,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.OnlyLocal,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTPCCWorkload(ctx, t, c, false)
			},
		})
	}
}

func runTPCCWorkload(ctx context.Context, t test.Test, c cluster.Cluster, enableDRPC bool) {
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings(install.EnvOption{
		// enable or disable DRPC
		fmt.Sprintf("COCKROACH_EXPERIMENTAL_DRPC_ENABLED=%s", strconv.FormatBool(enableDRPC)),
	})
	c.Start(ctx, t.L(), opts, settings, c.All())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	numWarehouses := 1
	nodeOne := c.Nodes(1)
	runDuration := 30 * time.Second

	// Initialize workload
	init := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", nodeOne).
		Flag("warehouses", numWarehouses)

	err := c.RunE(ctx, option.WithNodes(nodeOne), init.String())
	require.NoError(t, err, "failed to run tpcc init")

	// Run workload
	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		run := roachtestutil.NewCommand("./cockroach workload run tpcc").
			Arg("{pgurl%s}", nodeOne).
			Flag("warehouses", numWarehouses).
			Flag("duration", runDuration).
			Option("tolerate-errors")

		err := c.RunE(ctx, option.WithNodes(nodeOne), run.String())
		// Instead, return err?
		require.NoError(t, err, "failed to run tpcc workload")
		return nil
	})

	m.Wait()
}
