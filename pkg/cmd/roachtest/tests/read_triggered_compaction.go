// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// rows2TiBRTC is the number of rows to import to load 2TB of data (when
// replicated across 3 machines).
const rows2TiBRTC = 217_013_886

func importBankDataRTC(
	ctx context.Context, rows,
	runOnNode int, t test.Test, c cluster.Cluster) {

	t.Status("importing data")
	importArgs := []string{
		"./workload", "fixtures", "import", "bank",
		"--db=bank", "--payload-bytes=10240",
		fmt.Sprintf("--rows=%d", rows), "--seed=1", "{pgurl:1}",
	}
	c.Run(ctx, c.Node(runOnNode), importArgs...)
}

// TODO: try this with ycsb C instead.
// We want to run ycsb A, while running a backup as an attempt
// to test issues faced here:
// https://github.com/cockroachdb/pebble/issues/1143.
func registerRTC(r registry.Registry) {
	workloads := []string{"A"}
	cpusConfigs := []int{32}

	concurrencyConfigs := map[string] /* workload */ map[int] /* cpus */ int{
		"A": {32: 144},
	}

	runWorkload := func(ctx context.Context, t test.Test, c cluster.Cluster, wl string, cpus int) {
		nodes := c.Spec().NodeCount - 1

		conc, ok := concurrencyConfigs[wl][cpus]
		if !ok {
			t.Fatalf("missing concurrency for (workload, cpus) = (%s, %d)", wl, cpus)
		}

		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, nodes))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(1))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes))
		WaitFor3XReplication(t, c.Conn(ctx, 1))

		// Import some data for the backup.
		rows := rows2TiBRTC
		if c.IsLocal() {
			rows = 100
		}
		importBankDataRTC(ctx, rows, 1, t, c)

		m := c.NewMonitor(ctx, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			t.Status(fmt.Sprintf("running ycsb %s workload", wl))

			sfu := fmt.Sprintf(" --select-for-update=%t", t.IsBuildVersion("v19.2.0"))
			ramp := " --ramp=" + ifLocal(c, "0s", "2m")

			// TODO: Trying out a 30min runtime for now to see if the issue can be
			// reproduced.
			duration := " --duration=" + ifLocal(c, "10s", "30m")
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --insert-count=1000000 --workload=%s --concurrency=%d"+
					" --splits=%d --histograms="+t.PerfArtifactsDir()+"/stats.json"+sfu+ramp+duration+
					" {pgurl:1-%d}",
				wl, conc, nodes, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})

		// Run the backup statement on the first node.
		t.L().Printf("gonna run backup")
		go func(ctx context.Context) {
			t.Status("running backup")
			c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "BACKUP bank.bank"`)
		}(ctx)

		m.Wait()
	}

	for _, wl := range workloads {
		for _, cpus := range cpusConfigs {
			name := fmt.Sprintf("rtcissue/%s/nodes=3/cpu=%d", wl, cpus)
			wl, cpus := wl, cpus
			r.Add(registry.TestSpec{
				Name:    name,
				Owner:   registry.OwnerStorage,
				Cluster: r.MakeClusterSpec(4, spec.CPU(cpus)),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runWorkload(ctx, t, c, wl, cpus)
				},
			})
		}
	}
}
