// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerMultiStoreOverload(r registry.Registry) {
	runKV := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		startOpts.RoachprodOpts.StoreCount = 2
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		// db1 on store1 and db2 on store2. Writes to db2 will overload store2 and
		// cause admission control to maintain health by queueing these
		// operations. This will not affect reads and writes to db1.
		for _, name := range []string{"db1", "db2"} {
			constraint := "store1"
			if name == "db2" {
				constraint = "store2"
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", name)); err != nil {
				t.Fatalf("failed to create %s: %v", name, err)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf(
				"ALTER DATABASE %s CONFIGURE ZONE USING constraints = '[+%s]', "+
					"voter_constraints = '[+%s]', num_voters = 1", name, constraint, constraint)); err != nil {
				t.Fatalf("failed to configure zone for %s: %v", name, err)
			}
		}
		// Defensive, since admission control is enabled by default. This test can
		// fail if admission control is disabled.
		roachtestutil.SetAdmissionControl(ctx, t, c, true)
		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'"); err != nil {
			t.Fatalf("failed to disable load based splitting: %v", err)
		}
		t.Status("running workload")
		dur := 20 * time.Minute
		duration := " --duration=" + roachtestutil.IfLocal(c, "10s", dur.String())
		labels := map[string]string{
			"duration": dur.String(),
		}
		histograms := " " + roachtestutil.GetWorkloadHistogramArgs(t, c, labels)
		m1 := c.NewMonitor(ctx, c.CRDBNodes())
		m1.Go(func(ctx context.Context) error {
			dbRegular := " --db=db1"
			concurrencyRegular := roachtestutil.IfLocal(c, "", " --concurrency=8")
			readPercentRegular := " --read-percent=95"
			cmdRegular := fmt.Sprintf("./cockroach workload run kv --init"+
				dbRegular+histograms+concurrencyRegular+duration+readPercentRegular+
				" {pgurl%s}", c.CRDBNodes())
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdRegular)
			return nil
		})
		m2 := c.NewMonitor(ctx, c.CRDBNodes())
		m2.Go(func(ctx context.Context) error {
			dbOverload := " --db=db2"
			concurrencyOverload := roachtestutil.IfLocal(c, "", " --concurrency=64")
			readPercentOverload := " --read-percent=0"
			bs := 1 << 16 /* 64KB */
			blockSizeOverload := fmt.Sprintf(" --min-block-bytes=%d --max-block-bytes=%d",
				bs, bs)
			cmdOverload := fmt.Sprintf("./cockroach workload run kv --init"+
				dbOverload+histograms+concurrencyOverload+duration+readPercentOverload+blockSizeOverload+
				" {pgurl%s}", c.CRDBNodes())
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdOverload)
			return nil
		})
		m1.Wait()
		m2.Wait()
	}

	r.Add(registry.TestSpec{
		Name:             "admission-control/multi-store-with-overload",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          r.MakeClusterSpec(2, spec.CPU(8), spec.WorkloadNode(), spec.SSD(2)),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runKV(ctx, t, c)
		},
	})
}
