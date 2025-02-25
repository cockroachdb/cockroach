// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv/kvtestutils"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerKV(r registry.Registry) {
	type kvOptions struct {
		nodes       int
		cpus        int
		readPercent int
		// If true, the reads are limited reads over the full span of the table.
		// Currently this also enables SFU writes on the workload since this is
		// geared towards testing optimistic locking and latching.
		spanReads bool
		batchSize int
		blockSize int
		splits    int
		// If true, load-based splitting will be disabled.
		disableLoadSplits        bool
		encryption               bool
		sequential               bool
		globalMVCCRangeTombstone bool
		expirationLeases         bool
		concMultiplier           int
		ssds                     int
		raid0                    bool
		duration                 time.Duration
		tracing                  bool // `trace.debug.enable`
		weekly                   bool
		owner                    registry.Owner // defaults to KV
		sharedProcessMT          bool
		// Set to true to make jemalloc release memory more aggressively to the
		// OS, to reduce resident size.
		jemallocReleaseFaster bool
		// Set to true to reduce the Pebble block cache from 25% to 20%.
		smallBlockCache bool
	}
	computeConcurrency := func(opts kvOptions) int {
		// Scale the workload concurrency with the number of nodes in the cluster to
		// account for system capacity, then scale inversely with the batch size to
		// account for the cost of each operation.
		// TODO(nvanbenschoten): should we also scale the workload concurrency with
		// the number of CPUs on each node? Probably, but doing so will disrupt
		// regression tracking.
		concPerNode := 64
		if opts.concMultiplier != 0 {
			concPerNode = opts.concMultiplier
		}
		conc := concPerNode * opts.nodes
		const batchOpsPerConc = 2
		if opts.batchSize >= batchOpsPerConc {
			conc /= opts.batchSize / batchOpsPerConc
		}
		return conc
	}
	runKV := func(ctx context.Context, t test.Test, c cluster.Cluster, opts kvOptions) {
		nodes := c.Spec().NodeCount - 1

		// Don't start a scheduled backup on this perf sensitive roachtest that reports to roachperf.
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		if opts.ssds > 1 && !opts.raid0 {
			startOpts.RoachprodOpts.StoreCount = opts.ssds
		}
		// Use a secure cluster so we can test with a non-root user.
		settings := install.MakeClusterSettings()
		if opts.globalMVCCRangeTombstone {
			settings.Env = append(settings.Env, "COCKROACH_GLOBAL_MVCC_RANGE_TOMBSTONE=true")
		}
		if opts.jemallocReleaseFaster {
			settings.Env = append(settings.Env,
				"MALLOC_CONF=background_thread:true,dirty_decay_ms:2000,muzzy_decay_ms:0")
		}
		if opts.smallBlockCache {
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--cache=0.20")
		}
		c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		if opts.disableLoadSplits {
			if _, err := db.ExecContext(ctx, "SET CLUSTER SETTING kv.range_split.by_load_enabled = 'false'"); err != nil {
				t.Fatalf("failed to disable load based splitting: %v", err)
			}
		}
		if opts.expirationLeases {
			if _, err := db.ExecContext(ctx, "SET CLUSTER SETTING kv.expiration_leases_only.enabled = true"); err != nil {
				t.Fatalf("failed to enable expiration leases: %v", err)
			}
		}
		if opts.tracing {
			if _, err := db.ExecContext(ctx, "SET CLUSTER SETTING trace.debug.enable = true"); err != nil {
				t.Fatalf("failed to enable tracing: %v", err)
			}
		}
		if opts.sharedProcessMT {
			startOpts = option.StartSharedVirtualClusterOpts(appTenantName)
			c.StartServiceForVirtualCluster(
				ctx, t.L(), startOpts, install.MakeClusterSettings(),
			)
		}

		t.Status("running workload")
		m := c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			concurrency := roachtestutil.IfLocal(c, "", " --concurrency="+fmt.Sprint(computeConcurrency(opts)))
			splits := ""
			if opts.splits > 0 {
				splits = " --splits=" + strconv.Itoa(opts.splits)
			}
			if opts.duration == 0 {
				opts.duration = 30 * time.Minute
			}
			var readPercent string
			if opts.spanReads {
				// SFU makes sense only if we repeat writes to the same key. Here
				// we've arbitrarily picked a cycle-length of 1000, so 1 in 1000
				// writes will contend with the limited scan wrt locking.
				readPercent =
					fmt.Sprintf(" --span-percent=%d --span-limit=1 --sfu-writes=true --cycle-length=1000",
						opts.readPercent)
			} else {
				readPercent = fmt.Sprintf(" --read-percent=%d", opts.readPercent)
			}

			var batchSize string
			if opts.batchSize > 0 {
				batchSize = fmt.Sprintf(" --batch=%d", opts.batchSize)
			}

			var blockSize string
			if opts.blockSize > 0 {
				blockSize = fmt.Sprintf(" --min-block-bytes=%d --max-block-bytes=%d",
					opts.blockSize, opts.blockSize)
			}

			var sequential string
			if opts.sequential {
				splits = "" // no splits
				sequential = " --sequential"
			}

			defaultDuration := roachtestutil.IfLocal(c, "10s", opts.duration.String())
			duration := roachtestutil.GetEnvWorkloadDurationValueOrDefault(defaultDuration)
			url := fmt.Sprintf(" {pgurl:1-%d}", nodes)
			if opts.sharedProcessMT {
				url = fmt.Sprintf(" {pgurl:1-%d:%s}", nodes, appTenantName)
			}

			histograms := " " + roachtestutil.GetWorkloadHistogramArgs(t, c, nil)
			cmd := fmt.Sprintf(
				"./cockroach workload run kv --tolerate-errors --init --user=%s --password=%s", install.DefaultUser, install.DefaultPassword,
			) +
				histograms + concurrency + splits + duration + readPercent +
				batchSize + blockSize + sequential + url
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
			return nil
		})
		m.Wait()
	}

	for _, opts := range []kvOptions{
		// Standard configs.
		{nodes: 1, cpus: 8, readPercent: 0},
		{nodes: 1, cpus: 8, readPercent: 0, sharedProcessMT: true},
		// CPU overload test, to stress admission control.
		{nodes: 1, cpus: 8, readPercent: 50, concMultiplier: 8192},
		// IO write overload test, to stress admission control.
		//
		// jemallocReleaseFaster is set to true due to OOMs observed in
		// https://github.com/cockroachdb/cockroach/issues/125769. There is
		// unavoidable 20% internal fragmentation in the Pebble block cache, since
		// a 64KB value size causes a 64+KB sstable block, which allocates
		// from the 80KB size class in jemalloc. So the allocated bytes from jemalloc
		// by the block cache are 20% higher than configured. By setting this flag to true,
		// we reduce the (resident-allocated) size in jemalloc.
		//
		// Also reduce the Pebble block cache, since we have seen one OOM despite
		// the jemallocReleaseFaster setting.
		{nodes: 1, cpus: 8, readPercent: 0, concMultiplier: 4096, blockSize: 1 << 16, /* 64 KB */
			jemallocReleaseFaster: true, smallBlockCache: true},
		{nodes: 1, cpus: 8, readPercent: 95},
		{nodes: 1, cpus: 8, readPercent: 95, sharedProcessMT: true},
		{nodes: 1, cpus: 32, readPercent: 0},
		{nodes: 1, cpus: 32, readPercent: 0, sharedProcessMT: true},
		{nodes: 1, cpus: 32, readPercent: 95},
		{nodes: 1, cpus: 32, readPercent: 95, sharedProcessMT: true},
		{nodes: 3, cpus: 8, readPercent: 0},
		{nodes: 3, cpus: 8, readPercent: 0, sharedProcessMT: true},
		{nodes: 3, cpus: 8, readPercent: 95},
		{nodes: 3, cpus: 8, readPercent: 95, sharedProcessMT: true},
		{nodes: 3, cpus: 8, readPercent: 95, tracing: true, owner: registry.OwnerObservability},
		{nodes: 3, cpus: 32, readPercent: 0},
		{nodes: 3, cpus: 32, readPercent: 0, sharedProcessMT: true},
		{nodes: 3, cpus: 32, readPercent: 95},
		{nodes: 3, cpus: 32, readPercent: 95, sharedProcessMT: true},
		{nodes: 3, cpus: 32, readPercent: 0, globalMVCCRangeTombstone: true},
		{nodes: 3, cpus: 32, readPercent: 95, globalMVCCRangeTombstone: true},

		// Configs with expiration-based leases.
		{nodes: 3, cpus: 8, readPercent: 0, expirationLeases: true},
		{nodes: 3, cpus: 8, readPercent: 95, expirationLeases: true},

		// Configs with large block sizes.
		{nodes: 3, cpus: 8, readPercent: 0, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 8, readPercent: 95, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 32, readPercent: 0, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 32, readPercent: 95, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 8, readPercent: 0, blockSize: 1 << 16 /* 64 KB */},
		{nodes: 3, cpus: 8, readPercent: 95, blockSize: 1 << 16 /* 64 KB */},
		{nodes: 3, cpus: 32, readPercent: 0, blockSize: 1 << 16 /* 64 KB */},
		{nodes: 3, cpus: 32, readPercent: 95, blockSize: 1 << 16 /* 64 KB */},

		// Configs with large (64kb only) block sizes and pre-splits. These examine
		// the impact of large block sizes without range splits occurring during
		// the workload run. +100 replicas per-node, with RF=3 and 3 nodes.
		{nodes: 3, cpus: 8, readPercent: 0, blockSize: 1 << 16 /* 64 KB */, splits: 100},
		{nodes: 3, cpus: 8, readPercent: 95, blockSize: 1 << 16 /* 64 KB */, splits: 100},
		{nodes: 3, cpus: 32, readPercent: 0, blockSize: 1 << 16 /* 64 KB */, splits: 100},
		{nodes: 3, cpus: 32, readPercent: 95, blockSize: 1 << 16 /* 64 KB */, splits: 100},

		// Configs with large batch sizes.
		{nodes: 3, cpus: 8, readPercent: 0, batchSize: 16},
		{nodes: 3, cpus: 8, readPercent: 95, batchSize: 16},

		// Configs with large nodes.
		{nodes: 3, cpus: 96, readPercent: 0},
		{nodes: 3, cpus: 96, readPercent: 95},
		{nodes: 4, cpus: 96, readPercent: 50, batchSize: 64},

		// Configs for comparing single store and multi store clusters.
		{nodes: 4, cpus: 8, readPercent: 95},
		{nodes: 4, cpus: 8, readPercent: 95, ssds: 8},
		{nodes: 4, cpus: 8, readPercent: 95, ssds: 8, raid0: true},

		// Configs with encryption.
		{nodes: 1, cpus: 8, readPercent: 0, encryption: true},
		{nodes: 1, cpus: 8, readPercent: 95, encryption: true},
		{nodes: 3, cpus: 8, readPercent: 0, encryption: true},
		{nodes: 3, cpus: 8, readPercent: 95, encryption: true},

		// Configs with a sequential access pattern.
		{nodes: 3, cpus: 32, readPercent: 0, sequential: true},
		{nodes: 3, cpus: 32, readPercent: 95, sequential: true},

		// Configs with reads, that are of limited spans, along with SFU writes.
		{nodes: 1, cpus: 8, readPercent: 95, spanReads: true, disableLoadSplits: true, sequential: true},
		{nodes: 1, cpus: 32, readPercent: 95, spanReads: true, disableLoadSplits: true, sequential: true},

		// Weekly larger scale configurations.
		{nodes: 32, cpus: 8, readPercent: 0, weekly: true, duration: time.Hour},
		{nodes: 32, cpus: 8, readPercent: 95, weekly: true, duration: time.Hour},
	} {
		opts := opts

		var nameParts []string
		var limitedSpanStr string
		if opts.spanReads {
			limitedSpanStr = "limited-spans"
		}
		nameParts = append(nameParts, fmt.Sprintf("kv%d%s", opts.readPercent, limitedSpanStr))
		if opts.weekly {
			nameParts = append(nameParts, "weekly")
		}
		nameParts = append(nameParts, fmt.Sprintf("enc=%t", opts.encryption))
		nameParts = append(nameParts, fmt.Sprintf("nodes=%d", opts.nodes))
		if opts.cpus != 8 { // support legacy test name which didn't include cpu
			nameParts = append(nameParts, fmt.Sprintf("cpu=%d", opts.cpus))
		}
		if opts.batchSize != 0 { // support legacy test name which didn't include batch size
			nameParts = append(nameParts, fmt.Sprintf("batch=%d", opts.batchSize))
		}
		if opts.blockSize != 0 { // support legacy test name which didn't include block size
			nameParts = append(nameParts, fmt.Sprintf("size=%dkb", opts.blockSize>>10))
		}
		if opts.splits != 0 { // support legacy test name which didn't include splits
			nameParts = append(nameParts, fmt.Sprintf("splt=%d", opts.splits))
		}
		if opts.sequential {
			nameParts = append(nameParts, "seq")
		}
		if opts.globalMVCCRangeTombstone {
			nameParts = append(nameParts, "mvcc-range-keys=global")
		}
		if opts.expirationLeases {
			nameParts = append(nameParts, "lease=expiration")
		}
		if opts.concMultiplier != 0 { // support legacy test name which didn't include this multiplier
			nameParts = append(nameParts, fmt.Sprintf("conc=%d", opts.concMultiplier))
		}
		if opts.ssds > 1 {
			nameParts = append(nameParts, fmt.Sprintf("ssds=%d", opts.ssds))
		}
		if opts.raid0 {
			nameParts = append(nameParts, "raid0")
		}
		if opts.disableLoadSplits {
			nameParts = append(nameParts, "no-load-splitting")
		}
		if opts.tracing {
			nameParts = append(nameParts, "tracing")
		}
		if opts.sharedProcessMT {
			nameParts = append(nameParts, "mt-shared-process")
		}
		owner := registry.OwnerTestEng
		if opts.owner != "" {
			owner = opts.owner
		}
		encryption := registry.EncryptionAlwaysDisabled
		if opts.encryption {
			encryption = registry.EncryptionAlwaysEnabled
		}
		// Save some money and CPU quota by using a smaller workload CPU. Only
		// do this for cluster of size 3 or smaller to avoid regressions.
		workloadNodeCPUs := 4
		if opts.nodes > 3 {
			workloadNodeCPUs = opts.cpus
		}
		cSpec := r.MakeClusterSpec(opts.nodes+1, spec.CPU(opts.cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(workloadNodeCPUs), spec.SSD(opts.ssds), spec.RAID0(opts.raid0))

		var clouds registry.CloudSet
		tags := make(map[string]struct{})
		if opts.ssds != 0 {
			// Multi-store tests are only supported on GCE.
			clouds = registry.OnlyGCE
		} else if !opts.weekly && (opts.readPercent == 95 || opts.readPercent == 0) {
			// All the kv0|95 tests should run on AWS.
			clouds = registry.AllClouds
		} else {
			clouds = registry.AllExceptAWS
		}

		suites := registry.Suites(registry.Nightly)
		if opts.weekly {
			suites = registry.Suites(registry.Weekly)
			tags["weekly"] = struct{}{}
		}

		var skipPostValidations registry.PostValidation
		if opts.blockSize == 1<<16 {
			// Large block size variations may timeout waiting for replica divergence
			// post-test validation due to high write volume, see #141007.
			skipPostValidations = registry.PostValidationReplicaDivergence
		}

		r.Add(registry.TestSpec{
			Name:      strings.Join(nameParts, "/"),
			Owner:     owner,
			Benchmark: true,
			Cluster:   cSpec,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runKV(ctx, t, c, opts)
			},
			CompatibleClouds:    clouds,
			Suites:              suites,
			EncryptionSupport:   encryption,
			SkipPostValidations: skipPostValidations,
		})
	}
}

func registerKVContention(r registry.Registry) {
	const nodes = 4
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("kv/contention/nodes=%d", nodes),
		Owner:            registry.OwnerKV,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(nodes+1, spec.WorkloadNode()),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Start the cluster with an extremely high txn liveness threshold.
			// If requests ever get stuck on a transaction that was abandoned
			// then it will take 10m for them to get unstuck, at which point the
			// QPS threshold check in the test is guaranteed to fail.
			settings := install.MakeClusterSettings()
			settings.Env = append(settings.Env, "COCKROACH_TXN_LIVENESS_HEARTBEAT_MULTIPLIER=600")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

			conn := c.Conn(ctx, t.L(), 1)
			// Enable request tracing, which is a good tool for understanding
			// how different transactions are interacting.
			if _, err := conn.Exec(`
				SET CLUSTER SETTING trace.debug.enable = true;
			`); err != nil {
				t.Fatal(err)
			}
			// Drop the deadlock detection delay because the test creates a
			// large number transaction deadlocks.
			if _, err := conn.Exec(`
				SET CLUSTER SETTING kv.lock_table.deadlock_detection_push_delay = '5ms'
			`); err != nil && !strings.Contains(err.Error(), "unknown cluster setting") {
				t.Fatal(err)
			}

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				// Write to a small number of keys to generate a large amount of
				// contention. Use a relatively high amount of concurrency and
				// aim to average one concurrent write for each key in the keyspace.
				const cycleLength = 512
				const concurrency = 128
				const avgConcPerKey = 1
				const batchSize = avgConcPerKey * (cycleLength / concurrency)

				// Split the table so that each node can have a single leaseholder.
				splits := nodes

				// Run the workload for an hour. Add a secondary index to avoid
				// UPSERTs performing blind writes.
				const duration = 1 * time.Hour
				cmd := fmt.Sprintf("./cockroach workload run kv --init --secondary-index --duration=%s "+
					"--cycle-length=%d --concurrency=%d --batch=%d --splits=%d {pgurl%s}",
					duration, cycleLength, concurrency, batchSize, splits, c.CRDBNodes())
				start := timeutil.Now()
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
				end := timeutil.Now()

				// Assert that the average throughput stayed above a certain
				// threshold. In this case, assert that max throughput only
				// dipped below 50 qps for 10% of the time.
				const minQPS = 50
				verifyTxnPerSecond(ctx, c, t, c.Node(1), start, end, minQPS, 0.1)
				return nil
			})
			m.Wait()
		},
	})
}

func registerKVQuiescenceDead(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:                "kv/quiescence/nodes=3",
		Owner:               registry.OwnerKV,
		Cluster:             r.MakeClusterSpec(4, spec.WorkloadNode()),
		CompatibleClouds:    registry.AllExceptAWS,
		Suites:              registry.Suites(registry.Nightly),
		Leases:              registry.EpochLeases,
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			settings := install.MakeClusterSettings(install.ClusterSettingsOption{
				"sql.stats.automatic_collection.enabled": "false",
			})
			c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, c.CRDBNodes())
			m := c.NewMonitor(ctx, c.CRDBNodes())

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
			require.NoError(t, err)

			qps := func(f func()) float64 {

				numInserts := func() float64 {
					var v float64
					if err = db.QueryRowContext(
						ctx, `SELECT value FROM crdb_internal.node_metrics WHERE name = 'sql.insert.count'`,
					).Scan(&v); err != nil {
						t.Fatal(err)
					}
					return v
				}

				tBegin := timeutil.Now()
				before := numInserts()
				f()
				after := numInserts()
				return (after - before) / timeutil.Since(tBegin).Seconds()
			}

			const kv = "./cockroach workload run kv --duration=10m --read-percent=0"

			// Initialize the database with ~10k ranges so that the absence of
			// quiescence hits hard once a node goes down.
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload run kv --init --max-ops=1 --splits 10000 --concurrency 100 {pgurl:1}")
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), kv+" --seed 0 {pgurl:1}")
			// Measure qps with all nodes up (i.e. with quiescence).
			qpsAllUp := qps(func() {
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), kv+" --seed 1 {pgurl:1}")
			})
			// Graceful shut down third node.
			m.ExpectDeath()
			c.Stop(
				ctx, t.L(), option.NewStopOpts(option.Graceful(30)), c.Node(len(c.CRDBNodes())),
			)
			// Measure qps with node down (i.e. without quiescence).
			qpsOneDown := qps(func() {
				// Use a different seed to make sure it's not just stepping into the
				// other earlier kv invocation's footsteps.
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), kv+" --seed 2 {pgurl:1}")
			})

			if minFrac, actFrac := 0.8, qpsOneDown/qpsAllUp; actFrac < minFrac {
				t.Fatalf(
					"QPS dropped from %.2f to %.2f (factor of %.2f, min allowed %.2f)",
					qpsAllUp, qpsOneDown, actFrac, minFrac,
				)
			}
			t.L().Printf("QPS went from %.2f to %2.f with one node down\n", qpsAllUp, qpsOneDown)
		},
	})
}

func registerKVGracefulDraining(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "kv/gracefuldraining",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(7, spec.WorkloadNode()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			nodes := c.Spec().NodeCount - 1
			t.Status("starting cluster")
			// If the test ever fails, the person who investigates the
			// failure will likely be thankful for this additional logging.
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--vmodule=store=2,"+
				"store_rebalancer=2,liveness=2")
			// TODO(kvoli): We are enabling continous profiling here to help debug
			// #131569, disable this once the issue is resolved.
			settings := install.MakeClusterSettings()
			settings.ClusterSettings["server.cpu_profile.duration"] = "5s"
			settings.ClusterSettings["server.cpu_profile.interval"] = "1s"
			settings.ClusterSettings["server.cpu_profile.cpu_usage_combined_threshold"] = "15"
			settings.ClusterSettings["server.cpu_profile.total_dump_size_limit"] = fmt.Sprintf("%d", 256<<20 /* 256MB */)
			c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

			// Don't connect to the node we are going to shut down.
			dbs := make([]*gosql.DB, nodes-1)
			for i := range dbs {
				dbs[i] = c.Conn(ctx, t.L(), i+1)
				//nolint:deferloop TODO(#137605)
				defer dbs[i].Close()
			}

			err := roachtestutil.WaitFor3XReplication(ctx, t.L(), dbs[0])
			require.NoError(t, err)

			t.Status("initializing workload")

			// Initialize the database with a lot of ranges so that there are
			// definitely a large number of leases on the node that we shut down
			// before it starts draining.
			c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach workload init kv --splits 100 {pgurl:1}")

			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.ExpectDeath()

			// specifiedQPS is going to be the --max-rate for the kv workload.
			specifiedQPS := 2000
			if c.IsLocal() {
				specifiedQPS = 100
			}
			// Because we're specifying a --max-rate well less than what cockroach
			// should be capable of, draining one of the three nodes should have no
			// effect on performance at all, meaning that a fairly aggressive
			// threshold here should be ok.
			expectedQPS := float64(specifiedQPS) * .9

			t.Status("starting workload")
			workloadStartTime := timeutil.Now()
			// Three iterations, each iteration has a 3-minute duration.
			// We also warm up the workload for a minute, and let the initial
			// replica movement finish.
			desiredRunDuration := 11 * time.Minute
			m.Go(func(ctx context.Context) error {
				// TODO(baptist): Remove --tolerate-errors once #129427 (ambiguous results)
				// is addressed.
				// Don't connect to the node we are going to shut down.
				cmd := fmt.Sprintf(
					"./cockroach workload run kv --tolerate-errors --duration=%s --read-percent=50 --follower-read-percent=50 --concurrency=200 --max-rate=%d {pgurl%s}",
					desiredRunDuration, specifiedQPS, c.Range(1, nodes-1))
				t.WorkerStatus(cmd)
				defer func() {
					t.WorkerStatus("workload command completed")
					t.WorkerStatus()
				}()
				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
			})
			// Sleep for the first minute to let ranges settle down, leases warm up,
			// and stats populate. It also helps teamcity metrics to pick up what the
			// cluster is doing so that we're not flying blind[1] in the early phases
			// of the test.
			// [1]: https://cockroachlabs.slack.com/archives/C023S0V4YEB/p1726483324593879
			t.L().PrintfCtx(ctx, "sleeping one minute")
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
			}
			// Set up statement bundles for the (now known) top queries, with reasonable
			// criteria. This gives us something to look into should the test fail to
			// meet its qps targets.
			require.NoError(t, roachtestutil.ProfileTopStatements(ctx, c, t.L(), roachtestutil.ProfDbName("kv")))
			defer func() {
				// In cases where the test fails, the supplied context will be
				// cancelled, and we'll be left with squat. Download profiles using a
				// separate context.
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				if err := roachtestutil.DownloadProfiles(ctx, c, t.L(), t.ArtifactsDir()); err != nil {
					t.L().PrintfCtx(ctx, "failed to download stmt bundles: %v", err)
				}
			}()

			verifyQPS := func(ctx context.Context) error {
				if qps := roachtestutil.MeasureQPS(ctx, t, c, base.DefaultMetricsSampleInterval, c.Range(1, nodes-1)); qps < expectedQPS {
					return errors.Newf(
						"QPS of %.2f at time %v is below minimum allowable QPS of %.2f",
						qps, timeutil.Now(), expectedQPS)
				}
				return nil
			}

			t.Status("waiting for perf to stabilize")
			testutils.SucceedsSoon(t, func() error { return verifyQPS(ctx) })

			// Begin the monitoring goroutine to track QPS every second.
			m.Go(func(ctx context.Context) error {
				t.Status("starting watcher to verify QPS during the test")
				defer t.WorkerStatus()
				for {
					// Measure QPS every second throughout the test. verifyQPS takes time
					// to run so we don't sleep between invocations.
					require.NoError(t, verifyQPS(ctx))
					// Stop measuring 10 seconds before we stop the workload.
					if timeutil.Since(workloadStartTime) > desiredRunDuration-10*time.Second {
						return nil
					}
				}
			})
			restartNode := c.Node(nodes)

			t.Status("gracefully draining and restarting nodes")
			// Gracefully shut down the third node, let the cluster run for a
			// while, then restart it. Repeat for a total of 3 times for good
			// measure.
			for i := 0; i < 3; i++ {
				if i > 0 {
					// No need to wait extra during the first iteration: we
					// have already waited for the perf to become stable
					// above.
					t.Status("letting workload run with all nodes")
					select {
					case <-ctx.Done():
						t.Fatalf("context cancelled while waiting")
					case <-time.After(2 * time.Minute):
					}
				}
				drainWithIpTables(ctx, restartNode, c, t)
				m.ResetDeaths()
			}

			// Let the test run for nearly the entire duration of the kv command.
			// The key is that we want the workload command to still be running when
			// we look at the performance below. Given that the workload was set
			// to run for 5 minutes, we should be fine here, however we want to guarantee
			// there's at least 10s left to go. Check this.
			t.Status("checking workload is still running")
			runDuration := timeutil.Since(workloadStartTime)
			if runDuration > desiredRunDuration-10*time.Second {
				t.Fatalf("not enough workload time left to reliably determine performance (%s left)",
					desiredRunDuration-runDuration)
			}

			t.Status("waiting for workload to complete")
			m.Wait()
		},
	})
}

// drainWithIpTables does a graceful drain and allows it to complete. The
// liveness record is updated at the beginning of the drain process, so by time
// the drain completes in ~5s all other nodes should "know" it is draining.
func drainWithIpTables(
	ctx context.Context, restartNode option.NodeListOption, c cluster.Cluster, t test.Test,
) {
	cmd := fmt.Sprintf("./cockroach node drain --certs-dir=%s --port={pgport%s} --self", install.CockroachNodeCertsDir, restartNode)
	c.Run(ctx, option.WithNodes(restartNode), cmd)

	// Simulate a hard network drop to this node prior to shutting it down. This
	// is what we see in some customer environments. As an example, a docker
	// container shutdown will also disappear from the network and drop all
	// packets in both directions.
	// TODO(baptist): Convert this to use a network partitioning utility.
	if !c.IsLocal() {
		c.Run(ctx, option.WithNodes(restartNode), `sudo iptables -A INPUT -p tcp --dport 26257 -j DROP`)
		c.Run(ctx, option.WithNodes(restartNode), `sudo iptables -A OUTPUT -p tcp --dport 26257 -j DROP`)
		// NB: We don't use the original context as it might be cancelled.
		defer c.Run(context.Background(), option.WithNodes(restartNode), `sudo iptables -F`)
	}
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), restartNode)

	t.Status("letting workload run with one node down")
	select {
	case <-ctx.Done():
		t.Fatalf("context cancelled while waiting")
	case <-time.After(1 * time.Minute):
	}

	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.SkipInit = true
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), restartNode)
}

func registerKVSplits(r registry.Registry) {
	for _, item := range []struct {
		quiesce bool
		splits  int
		leases  registry.LeaseType
		timeout time.Duration
		envVars []string
	}{
		// NB: with 500000 splits, this test sometimes fails since it's pushing
		// far past the number of replicas per node we support, at least if the
		// ranges start to unquiesce (which can set off a cascade due to resource
		// exhaustion).
		{true, 300_000, registry.EpochLeases, 2 * time.Hour, nil},
		// This version of the test prevents range quiescence to trigger the
		// badness described above more reliably for when we wish to improve
		// the performance. For now, just verify that 30k unquiesced ranges
		// is tenable.
		{false, 30_000, registry.EpochLeases, 2 * time.Hour, nil},
		// Expiration-based leases prevent quiescence, and are also more expensive
		// to keep alive. Again, just verify that 30k ranges is ok.
		{false, 30_000, registry.ExpirationLeases, 2 * time.Hour, nil},
		// Leader leases without quiescence perform similarly to epoch leases
		// without quiescence. Even though epoch leases are set to reach 30k, they
		// also reach 60k reliably.
		{false, 60_000, registry.LeaderLeases, 2 * time.Hour, nil},
		// Leader leases with quiescence don't quite match epoch leases with
		// quiescence because in leader leases only the followers ever quiesce.
		{true, 90_000, registry.LeaderLeases, 2 * time.Hour, nil},
		// With some additional tuning, leader leases can do even better. The extended interval allow
		// for more flexibility in extending store liveness support, and prevent support withdrawals at
		// higher CPU utilization when goroutine scheduling latency is high.
		{
			true, 120_000, registry.LeaderLeases, 2 * time.Hour,
			[]string{
				"COCKROACH_STORE_LIVENESS_SUPPORT_EXPIRY_INTERVAL=1s",
				"COCKROACH_STORE_LIVENESS_HEARTBEAT_INTERVAL=3s",
				"COCKROACH_STORE_LIVENESS_SUPPORT_DURATION=6s",
			},
		},
	} {
		item := item // for use in closure below
		name := fmt.Sprintf("kv/splits/nodes=3/quiesce=%t/lease=%s", item.quiesce, item.leases)
		if item.envVars != nil {
			name += "/tuned"
		}
		r.Add(registry.TestSpec{
			Name:    name,
			Owner:   registry.OwnerKV,
			Timeout: item.timeout,
			Cluster: r.MakeClusterSpec(4, spec.WorkloadNode()),
			// These tests are carefully tuned to succeed up to certain number of
			// splits; they are flaky in slower environments.
			CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
			Suites:           registry.Suites(registry.Nightly),
			Leases:           item.leases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				nodes := c.Spec().NodeCount - 1

				settings := install.MakeClusterSettings()
				settings.Env = append(settings.Env, "COCKROACH_MEMPROF_INTERVAL=1m", "COCKROACH_DISABLE_QUIESCENCE="+strconv.FormatBool(!item.quiesce))
				settings.Env = append(settings.Env, item.envVars...)
				startOpts := option.NewStartOpts(option.NoBackupSchedule)
				startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--cache=256MiB")
				c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

				t.Status("running workload")
				workloadCtx, workloadCancel := context.WithCancel(ctx)
				m := c.NewMonitor(workloadCtx, c.CRDBNodes())
				m.Go(func(ctx context.Context) error {
					defer workloadCancel()
					concurrency := roachtestutil.IfLocal(c, "", " --concurrency="+fmt.Sprint(nodes*64))
					splits := " --splits=" + roachtestutil.IfLocal(c, "2000", fmt.Sprint(item.splits))
					cmd := fmt.Sprintf(
						"./cockroach workload run kv --init --max-ops=1"+
							concurrency+splits+
							" {pgurl%s}",
						c.CRDBNodes())
					c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
					return nil
				})
				m.Wait()
			},
		})
	}
}

func registerKVScalability(r registry.Registry) {
	runScalability := func(ctx context.Context, t test.Test, c cluster.Cluster, percent int) {
		nodes := len(c.CRDBNodes())

		const maxPerNodeConcurrency = 64
		for i := nodes; i <= nodes*maxPerNodeConcurrency; i += nodes {
			c.Wipe(ctx, c.CRDBNodes())
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("./cockroach workload run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i)+
					" {pgurl%s}",
					percent, c.CRDBNodes())

				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
			})
			m.Wait()
		}
	}

	// TODO(peter): work in progress adaption of `roachprod test kv{0,95}`.
	if false {
		for _, p := range []int{0, 95} {
			p := p
			r.Add(registry.TestSpec{
				Name:             fmt.Sprintf("kv%d/scale/nodes=6", p),
				Owner:            registry.OwnerKV,
				Cluster:          r.MakeClusterSpec(7, spec.CPU(8), spec.WorkloadNode(), spec.WorkloadNodeCPU(8)),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Nightly),
				Leases:           registry.MetamorphicLeases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runScalability(ctx, t, c, p)
				},
			})
		}
	}
}

func registerKVRangeLookups(r registry.Registry) {
	type rangeLookupWorkloadType int
	const (
		splitWorkload rangeLookupWorkloadType = iota
		relocateWorkload
	)

	const (
		nodes = 8
		cpus  = 8
	)

	runRangeLookups := func(ctx context.Context, t test.Test, c cluster.Cluster, workers int, workloadType rangeLookupWorkloadType, maximumRangeLookupsPerSec float64) {
		doneInit := make(chan struct{})
		doneWorkload := make(chan struct{})
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

		t.Status("running workload")

		conns := make([]*gosql.DB, len(c.CRDBNodes()))
		for i := 0; i < len(c.CRDBNodes()); i++ {
			conns[i] = c.Conn(ctx, t.L(), i+1)
		}
		defer func() {
			for i := 0; i < len(c.CRDBNodes()); i++ {
				conns[i].Close()
			}
		}()
		err := roachtestutil.WaitFor3XReplication(ctx, t.L(), conns[0])
		require.NoError(t, err)

		m := c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			defer close(doneWorkload)
			defer close(doneInit)
			cmd := "./cockroach workload init kv --splits=1000 {pgurl:1}"
			if err = c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd); err != nil {
				return err
			}
			concurrency := roachtestutil.IfLocal(c, "", " --concurrency="+fmt.Sprint(len(c.CRDBNodes())*64))
			duration := " --duration=10m"
			readPercent := " --read-percent=50"
			// We run kv with --tolerate-errors, since the relocate workload is
			// expected to create `result is ambiguous (replica removed)` errors.
			cmd = fmt.Sprintf("./cockroach workload run kv --tolerate-errors"+
				concurrency+duration+readPercent+
				" {pgurl%s}", c.CRDBNodes())
			start := timeutil.Now()
			if err = c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd); err != nil {
				return err
			}
			end := timeutil.Now()
			verifyLookupsPerSec(ctx, c, t, c.Node(1), start, end, maximumRangeLookupsPerSec)
			return nil
		})

		<-doneInit
		for i := 0; i < workers; i++ {
			m.Go(func(ctx context.Context) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-doneWorkload:
						return nil
					default:
					}

					conn := conns[c.CRDBNodes().RandNode()[0]-1]
					switch workloadType {
					case splitWorkload:
						_, err := conn.ExecContext(ctx, `
							ALTER TABLE
								kv.kv
							SPLIT AT
								VALUES (CAST(floor(random() * 9223372036854775808) AS INT))
						`)
						if err != nil && !pgerror.IsSQLRetryableError(err) {
							return err
						}
					case relocateWorkload:
						newReplicas := rand.Perm(len(c.CRDBNodes()))[:3]
						_, err := conn.ExecContext(ctx, `
							ALTER TABLE
								kv.kv
							EXPERIMENTAL_RELOCATE
								SELECT ARRAY[$1, $2, $3], CAST(floor(random() * 9223372036854775808) AS INT)
						`, newReplicas[0]+1, newReplicas[1]+1, newReplicas[2]+1)
						if err != nil && !pgerror.IsSQLRetryableError(err) && !kvtestutils.IsExpectedRelocateError(err) {
							return err
						}
					default:
						panic("unexpected")
					}
				}
			})
		}
		m.Wait()
	}
	for _, item := range []struct {
		workers                   int
		workloadType              rangeLookupWorkloadType
		maximumRangeLookupsPerSec float64
	}{
		{2, splitWorkload, 15.0},
		// Relocates are expected to fail periodically when relocating random
		// ranges, so use more workers.
		{4, relocateWorkload, 50.0},
	} {
		// For use in closure.
		item := item
		var workloadName string
		switch item.workloadType {
		case splitWorkload:
			workloadName = "split"
		case relocateWorkload:
			workloadName = "relocate"
		default:
			panic("unexpected")
		}
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("kv50/rangelookups/%s/nodes=%d", workloadName, nodes),
			Owner:            registry.OwnerKV,
			Cluster:          r.MakeClusterSpec(nodes+1, spec.CPU(cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(cpus)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRangeLookups(ctx, t, c, item.workers, item.workloadType, item.maximumRangeLookupsPerSec)
			},
		})
	}
}

// registerKVRestartImpact measures the impact of stopping and then restarting
// a node during a write-heavy workload. Specifically the Raft log on the node
// falls behind when the node is down and when it comes back up it goes into IO
// Overload as it attempts to recover.
func registerKVRestartImpact(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name: "kv/restart/nodes=12",
		// This test is expensive (104vcpu), we run it weekly. Don't use local SSD
		// they are faster and less likely to hit a hard bandwidth limit causing
		// LSM inversion (IO overload).
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Owner:            registry.OwnerAdmissionControl,
		Timeout:          4 * time.Hour,
		Cluster:          r.MakeClusterSpec(13, spec.CPU(8), spec.WorkloadNode(), spec.WorkloadNodeCPU(8), spec.DisableLocalSSD()),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			nodes := len(c.CRDBNodes())
			startOpts := option.NewStartOpts(option.NoBackupSchedule)
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=store_rebalancer=2,allocator=2,allocator_scorer=1,replicate_queue=2,lease=2")
			settings := install.MakeClusterSettings()

			c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

			// Run long enough to create a large amount of pebble data.
			testDuration := 3 * time.Hour
			targetQPS := 5000
			// Having higher concurrency allows a more consistent QPS.
			concurrency := 256
			// We need a lot of ranges so that the individual ranges don't get truncated by Raft.
			splits := 20000

			if c.IsLocal() {
				testDuration = 3 * time.Minute
				targetQPS = 100
				concurrency = 24
				splits = 10
			}

			// We do 90% write and 10% read - this only counts the writes
			expectedQPS := float64(targetQPS) * 0.9
			// Ideally this should be closer to 0.9, but until more issues are fixed
			// we are starting lower. The first 0.9 is for the 10% reads we do.
			passingQPS := expectedQPS * 0.5
			fillDuration := testDuration * 2 / 3  // 2/3 of test time. 2 hours for non-local, 4 minutes for local.
			downtimeDuration := testDuration / 18 // 10 minutes for non-local, 20 sec for local.
			printInterval := testDuration / 72    // Show 72 point results during the run.

			c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf("./cockroach workload init kv --splits=%d {pgurl:1}", splits))

			workloadStartTime := timeutil.Now()
			t.Status(fmt.Sprintf("starting kv workload thread to run for %s", testDuration))

			// Three goroutines run and we wait for all to complete.
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.ExpectDeath()
			m.Go(func(ctx context.Context) error {
				// Don't include the last node when starting the workload since
				// it will stop in the middle. Write enough data per value to
				// make sure we create a large raft backlog.
				cmd := fmt.Sprintf("./cockroach workload run kv --min-block-bytes=8192 --max-block-bytes=8192 "+
					"--duration=%s --concurrency=%d --max-rate=%d --read-percent=10 {pgurl:1-%d}",
					testDuration.String(), concurrency, targetQPS, nodes-1,
				)

				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
			})

			// Begin the monitoring goroutine to track QPS every 5 seconds.
			m.Go(func(ctx context.Context) error {
				// Wait until 5 minutes after the workload began to begin asserting on
				// QPS.
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(5 * time.Minute):
				}

				t.Status(fmt.Sprintf("verify QPS is at least %d during the test, expecting %d", int(passingQPS), int(expectedQPS)))
				lastPrint := timeutil.Now()
				defer t.WorkerStatus()
				for {
					// Measure QPS every few seconds throughout the test. measureQPS takes time
					// to run, so we don't sleep between invocations.
					qps := roachtestutil.MeasureQPS(ctx, t, c, 5*time.Second, c.Range(1, nodes-1))
					if qps < passingQPS {
						return errors.Newf(
							"QPS of %.2f at time %v is below minimum allowable QPS of %.2f",
							qps, timeutil.Now(), passingQPS)
					}
					// Periodically print the current value.
					if timeutil.Since(lastPrint) > printInterval {
						lastPrint = timeutil.Now()
						t.Status(fmt.Sprintf("current QPS %.2f", qps))
					}
					// Stop measuring 10 seconds before the workload ends.
					if timeutil.Since(workloadStartTime) > testDuration-10*time.Second {
						return nil
					}
				}
			})

			// Begin the goroutine which will start and stop the node.
			m.Go(func(ctx context.Context) error {
				// Let some data be written to all nodes in the cluster.
				t.Status(fmt.Sprintf("waiting %s to get sufficient fill", fillDuration))
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(fillDuration):
				}

				// Gracefully shut down the last node to let it transfer leases cleanly.
				// Wait enough time to let it fall behind on Raft. Since there are a lot
				// of ranges, about half will be upreplicated during this time.
				gracefulOpts := option.DefaultStopOpts()
				gracefulOpts.RoachprodOpts.Sig = 15 // SIGTERM for clean shutdown
				gracefulOpts.RoachprodOpts.Wait = true
				c.Stop(ctx, t.L(), gracefulOpts, c.Node(nodes))
				t.Status(fmt.Sprintf("waiting %s after stopping node to allow the node to fall behind", downtimeDuration))
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(downtimeDuration):
				}

				// Start the node again. It will go into an IO Overload scenario.
				return c.StartE(ctx, t.L(), startOpts, settings, c.Node(nodes))
			})

			// Wait for the workload to finish.
			m.Wait()
		},
	})
}
