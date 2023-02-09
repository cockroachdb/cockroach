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
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

const envYCSBFlags = "ROACHTEST_YCSB_FLAGS"

type ycsbEnvConfig struct {
	nodes, cpus int
}

func registerYCSB(r registry.Registry) {
	workloads := []string{"A", "B", "C", "D", "E", "F"}
	envConfigs := []ycsbEnvConfig{{3, 8}, {3, 32}, {7, 8}}
	cpusWithGlobalMVCCRangeTombstone := 32

	// concurrencyConfigs contains near-optimal concurrency levels for each
	// (workload, cluster cpu count) combination. All of these figures were tuned on GCP
	// n1-standard instance types. We should consider implementing a search for
	// the optimal concurrency level in the roachtest itself (see kvbench).
	concurrencyConfigs := map[string] /* workload */ map[int] /* totalCPUs */ int{
		"A": {24: 96, 96: 144, 56: 144},
		"B": {24: 144, 96: 192, 56: 192},
		"C": {24: 144, 96: 192, 56: 192},
		"D": {24: 96, 96: 144, 56: 144},
		"E": {24: 96, 96: 144, 56: 144},
		"F": {24: 96, 96: 144, 56: 144},
	}

	// insertCountConfig contains the number of inserts to be run before
	// beginning the benchmark.
	insertCountConfig := map[int]int{
		3: 1_000_000,  // 1.25gb unreplicated
		7: 50_000_000, // 62.5gb unreplicated
	}

	runYCSB := func(
		ctx context.Context, t test.Test, c cluster.Cluster, wl string, cpus int, rangeTombstone bool,
	) {
		// For now, we only want to run the zfs tests on GCE, since only GCE supports
		// starting roachprod instances on zfs.
		if c.Spec().FileSystem == spec.Zfs && c.Spec().Cloud != spec.GCE {
			t.Skip("YCSB zfs benchmark can only be run on GCE", "")
		}

		nodes := c.Spec().NodeCount - 1

		totalCPUs := nodes * cpus
		conc, ok := concurrencyConfigs[wl][totalCPUs]
		if !ok {
			t.Fatalf("missing concurrency for (workload, nodes, cpus) = (%s, %d, %d)", wl, nodes, cpus)
		}

		settings := install.MakeClusterSettings()
		if rangeTombstone {
			settings.Env = append(settings.Env, "COCKROACH_GLOBAL_MVCC_RANGE_TOMBSTONE=true")
		}

		c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, nodes))
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))
		c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), settings, c.Range(1, nodes))
		err := WaitFor3XReplication(ctx, t, c.Conn(ctx, t.L(), 1))
		require.NoError(t, err)

		t.Status("running workload")
		m := c.NewMonitor(ctx, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			var args string
			args += fmt.Sprintf(" --select-for-update=%t", t.IsBuildVersion("v19.2.0"))
			args += " --ramp=" + ifLocal(c, "0s", "2m")
			args += " --duration=" + ifLocal(c, "10s", "30m")
			if envFlags := os.Getenv(envYCSBFlags); envFlags != "" {
				args += " " + envFlags
			}
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --insert-count=%d --workload=%s --concurrency=%d"+
					" --splits=%d --histograms="+t.PerfArtifactsDir()+"/stats.json"+args+
					" {pgurl:1-%d}",
				insertCountConfig[nodes], wl, conc, nodes, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, wl := range workloads {
		for _, env := range envConfigs {
			var name string
			cpus, nodes, workload := env.cpus, env.nodes, wl
			if cpus == 8 && nodes == 3 { // support legacy test name which didn't include cpu
				name = fmt.Sprintf("ycsb/%s/nodes=3", workload)
			} else {
				name = fmt.Sprintf("ycsb/%s/nodes=%d/cpu=%d", workload, nodes, cpus)
			}
			r.Add(registry.TestSpec{
				Name:    name,
				Owner:   registry.OwnerTestEng,
				Cluster: r.MakeClusterSpec(nodes+1, spec.CPU(cpus)),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runYCSB(ctx, t, c, workload, cpus, false /* rangeTombstone */)
				},
			})

			if workload == "A" && nodes == 3 {
				r.Add(registry.TestSpec{
					Name:    fmt.Sprintf("zfs/ycsb/%s/nodes=3/cpu=%d", workload, cpus),
					Owner:   registry.OwnerStorage,
					Cluster: r.MakeClusterSpec(4, spec.CPU(cpus), spec.SetFileSystem(spec.Zfs)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, workload, cpus, false /* rangeTombstone */)
					},
				})
			}

			if cpus == cpusWithGlobalMVCCRangeTombstone {
				r.Add(registry.TestSpec{
					Name:    fmt.Sprintf("%s/mvcc-range-keys=global", name),
					Owner:   registry.OwnerTestEng,
					Cluster: r.MakeClusterSpec(4, spec.CPU(cpus)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, workload, cpus, true /* rangeTombstone */)
					},
				})
			}
		}
	}
}
