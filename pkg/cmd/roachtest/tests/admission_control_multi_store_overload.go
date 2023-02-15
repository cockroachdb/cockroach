// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerMultiStoreOverload(r registry.Registry) {
	runKV := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		nodes := c.Spec().NodeCount - 1
		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, nodes))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))
		startOpts := option.DefaultStartOptsNoBackups()
		startOpts.RoachprodOpts.StoreCount = 2
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(1, nodes))

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
		setAdmissionControl(ctx, t, c, true)
		if _, err := db.ExecContext(ctx,
			"SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'"); err != nil {
			t.Fatalf("failed to disable load based splitting: %v", err)
		}
		t.Status("running workload")
		dur := 20 * time.Minute
		duration := " --duration=" + ifLocal(c, "10s", dur.String())
		histograms := " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
		m1 := c.NewMonitor(ctx, c.Range(1, nodes))
		m1.Go(func(ctx context.Context) error {
			dbRegular := " --db=db1"
			concurrencyRegular := ifLocal(c, "", " --concurrency=8")
			readPercentRegular := " --read-percent=95"
			cmdRegular := fmt.Sprintf("./workload run kv --init"+
				dbRegular+histograms+concurrencyRegular+duration+readPercentRegular+
				" {pgurl:1-%d}", nodes)
			c.Run(ctx, c.Node(nodes+1), cmdRegular)
			return nil
		})
		m2 := c.NewMonitor(ctx, c.Range(1, nodes))
		m2.Go(func(ctx context.Context) error {
			dbOverload := " --db=db2"
			concurrencyOverload := ifLocal(c, "", " --concurrency=64")
			readPercentOverload := " --read-percent=0"
			bs := 1 << 16 /* 64KB */
			blockSizeOverload := fmt.Sprintf(" --min-block-bytes=%d --max-block-bytes=%d",
				bs, bs)
			cmdOverload := fmt.Sprintf("./workload run kv --init"+
				dbOverload+histograms+concurrencyOverload+duration+readPercentOverload+blockSizeOverload+
				" {pgurl:1-%d}", nodes)
			c.Run(ctx, c.Node(nodes+1), cmdOverload)
			return nil
		})
		m1.Wait()
		m2.Wait()
	}

	r.Add(registry.TestSpec{
		Name:    "admission-control/multi-store-with-overload",
		Owner:   registry.OwnerAdmissionControl,
		Tags:    []string{`weekly`},
		Cluster: r.MakeClusterSpec(2, spec.CPU(8), spec.SSD(2)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runKV(ctx, t, c)
		},
	})
}
