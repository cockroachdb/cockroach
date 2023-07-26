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

func registerYCSB(r registry.Registry) {
	workloads := []string{"A", "B", "C", "D", "E", "F"}
	cpusConfigs := []int{8, 32}
	cpusWithReadCommitted := 32
	cpusWithGlobalMVCCRangeTombstone := 32

	// concurrencyConfigs contains near-optimal concurrency levels for each
	// (workload, cpu count) combination. All of these figures were tuned on GCP
	// n1-standard instance types. We should consider implementing a search for
	// the optimal concurrency level in the roachtest itself (see kvbench).
	concurrencyConfigs := map[string] /* workload */ map[int] /* cpus */ int{
		"A": {8: 96, 32: 144},
		"B": {8: 144, 32: 192},
		"C": {8: 144, 32: 192},
		"D": {8: 96, 32: 144},
		"E": {8: 96, 32: 144},
		"F": {8: 96, 32: 144},
	}

	runYCSB := func(
		ctx context.Context, t test.Test, c cluster.Cluster, wl string, cpus int, readCommitted, rangeTombstone bool,
	) {
		// For now, we only want to run the zfs tests on GCE, since only GCE supports
		// starting roachprod instances on zfs.
		if c.Spec().FileSystem == spec.Zfs && c.Spec().Cloud != spec.GCE {
			t.Skip("YCSB zfs benchmark can only be run on GCE", "")
		}

		nodes := c.Spec().NodeCount - 1

		conc, ok := concurrencyConfigs[wl][cpus]
		if !ok {
			t.Fatalf("missing concurrency for (workload, cpus) = (%s, %d)", wl, cpus)
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
			args += " --ramp=" + ifLocal(c, "0s", "2m")
			args += " --duration=" + ifLocal(c, "10s", "30m")
			if readCommitted {
				args += " --isolation-level=read_committed"
			}
			if envFlags := os.Getenv(envYCSBFlags); envFlags != "" {
				args += " " + envFlags
			}
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --insert-count=1000000 --workload=%s --concurrency=%d"+
					" --splits=%d --histograms="+t.PerfArtifactsDir()+"/stats.json"+args+
					" {pgurl:1-%d}",
				wl, conc, nodes, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, wl := range workloads {
		for _, cpus := range cpusConfigs {
			var name string
			if cpus == 8 { // support legacy test name which didn't include cpu
				name = fmt.Sprintf("ycsb/%s/nodes=3", wl)
			} else {
				name = fmt.Sprintf("ycsb/%s/nodes=3/cpu=%d", wl, cpus)
			}
			wl, cpus := wl, cpus
			r.Add(registry.TestSpec{
				Name:      name,
				Owner:     registry.OwnerTestEng,
				Benchmark: true,
				Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus)),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runYCSB(ctx, t, c, wl, cpus, false /* readCommitted */, false /* rangeTombstone */)
				},
				Tags: registry.Tags(`aws`),
			})

			if wl == "A" {
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("zfs/ycsb/%s/nodes=3/cpu=%d", wl, cpus),
					Owner:     registry.OwnerStorage,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus), spec.SetFileSystem(spec.Zfs)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, false /* readCommitted */, false /* rangeTombstone */)
					},
				})
			}

			if cpus == cpusWithReadCommitted {
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("%s/isolation-level=read-committed", name),
					Owner:     registry.OwnerTestEng,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, true /* readCommitted */, false /* rangeTombstone */)
					},
					Tags: registry.Tags(`aws`),
				})
			}

			if cpus == cpusWithGlobalMVCCRangeTombstone {
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("%s/mvcc-range-keys=global", name),
					Owner:     registry.OwnerTestEng,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, false /* readCommitted */, true /* rangeTombstone */)
					},
					Tags: registry.Tags(`aws`),
				})
			}
		}
	}
}
