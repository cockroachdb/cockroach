// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerYCSB(r registry.Registry) {
	workloads := []string{"A", "B", "C", "D", "E", "F"}
	cpusConfigs := []int{8, 32}
	cpusWithReadCommitted := 32
	cpusWithGlobalMVCCRangeTombstone := 32
	cpusWithUniformDistribution := 32

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

	type ycsbOptions struct {
		readCommitted       bool
		rangeTombstone      bool
		uniformDistribution bool
	}

	runYCSB := func(
		ctx context.Context, t test.Test, c cluster.Cluster, wl string, cpus int, opts ycsbOptions,
	) {
		// For now, we only want to run the zfs tests on GCE, since only GCE supports
		// starting roachprod instances on zfs.
		if c.Spec().FileSystem == spec.Zfs && c.Cloud() != spec.GCE {
			t.Skip("YCSB zfs benchmark can only be run on GCE", "")
		}

		conc, ok := concurrencyConfigs[wl][cpus]
		if !ok {
			t.Fatalf("missing concurrency for (workload, cpus) = (%s, %d)", wl, cpus)
		}

		settings := install.MakeClusterSettings()
		if opts.rangeTombstone {
			settings.Env = append(settings.Env, "COCKROACH_GLOBAL_MVCC_RANGE_TOMBSTONE=true")
		}

		c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, c.CRDBNodes())

		db := c.Conn(ctx, t.L(), 1)
		err := enableIsolationLevels(ctx, t, db)
		require.NoError(t, err)
		err = roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		t.Status("running workload")
		m := c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			var args string
			args += " --ramp=" + roachtestutil.IfLocal(c, "0s", "2m")
			if opts.readCommitted {
				args += " --isolation-level=read_committed"
			}
			if opts.uniformDistribution {
				args += " --request-distribution=uniform"
			}

			defaultDuration := roachtestutil.IfLocal(c, "10s", "30m")
			args += roachtestutil.GetEnvWorkloadDurationValueOrDefault(defaultDuration)

			labels := map[string]string{
				"workload_ycsb_type": wl,
				"concurrency":        fmt.Sprintf("%d", conc),
				"cpu":                fmt.Sprintf("%d", cpus),
			}

			cmd := fmt.Sprintf(
				"./cockroach workload run ycsb --init --insert-count=1000000 --workload=%s --concurrency=%d"+
					" --splits=%d %s %s {pgurl%s}", wl, conc, len(c.CRDBNodes()),
				roachtestutil.GetWorkloadHistogramArgs(t, c, labels), args, c.CRDBNodes())
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
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
			r.Add(registry.TestSpec{
				Name:      name,
				Owner:     registry.OwnerTestEng,
				Benchmark: true,
				Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(cpus)),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runYCSB(ctx, t, c, wl, cpus, ycsbOptions{})
				},
				CompatibleClouds: registry.AllClouds,
				Suites:           registry.Suites(registry.Nightly),
			})

			if wl == "A" {
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("zfs/ycsb/%s/nodes=3/cpu=%d", wl, cpus),
					Owner:     registry.OwnerStorage,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(cpus), spec.SetFileSystem(spec.Zfs)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, ycsbOptions{})
					},
					CompatibleClouds: registry.OnlyGCE,
					Suites:           registry.Suites(registry.Nightly),
				})
			}

			if cpus == cpusWithReadCommitted {
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("%s/isolation-level=read-committed", name),
					Owner:     registry.OwnerTestEng,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(cpus)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, ycsbOptions{readCommitted: true})
					},
					CompatibleClouds: registry.AllClouds,
					Suites:           registry.Suites(registry.Nightly),
				})
			}

			if cpus == cpusWithGlobalMVCCRangeTombstone {
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("%s/mvcc-range-keys=global", name),
					Owner:     registry.OwnerTestEng,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(cpus)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, ycsbOptions{rangeTombstone: true})
					},
					CompatibleClouds: registry.AllClouds,
					Suites:           registry.Suites(registry.Nightly),
				})
			}

			if cpus == cpusWithUniformDistribution {
				// Run YCSB workloads with uniform request distribution instead of zipfian.
				// We expect to see higher throughput as zipfian distribution causes hotspots,
				// especially on workloads A and F which have high contention. Zipfian is a more
				// realistic distribution model a customer would see, this serves mostly as a
				// baseline to compare against.
				r.Add(registry.TestSpec{
					Name:      fmt.Sprintf("%s/uniform", name),
					Owner:     registry.OwnerTestEng,
					Benchmark: true,
					Cluster:   r.MakeClusterSpec(4, spec.CPU(cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(cpus)),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runYCSB(ctx, t, c, wl, cpus, ycsbOptions{uniformDistribution: true})
					},
					CompatibleClouds: registry.AllClouds,
					Suites:           registry.Suites(registry.Nightly),
				})
			}
		}
	}
}

func enableIsolationLevels(ctx context.Context, t test.Test, db *gosql.DB) error {
	for _, cmd := range []string{
		// NOTE: even as we switch the default value of these setting to true on
		// master, we should keep these to ensure that the settings are configured
		// properly in mixed-version roachtests.
		`SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = 'true';`,
		// NOTE: for a similar reason, we use the deprecated name for this setting
		// to ensure that it is properly configured in mixed-version roachtests.
		`SET CLUSTER SETTING sql.txn.snapshot_isolation.enabled = 'true';`,
	} {
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			return err
		}
	}
	return nil
}
