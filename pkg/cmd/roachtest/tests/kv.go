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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const envKVFlags = "ROACHTEST_KV_FLAGS"

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
		splits    int // 0 implies default, negative implies 0
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
	}
	computeNumSplits := func(opts kvOptions) int {
		// TODO(ajwerner): set this default to a more sane value or remove it and
		// rely on load-based splitting.
		const defaultNumSplits = 1000
		switch {
		case opts.splits == 0:
			return defaultNumSplits
		case opts.splits < 0:
			return 0
		default:
			return opts.splits
		}
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
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))

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
		c.Start(ctx, t.L(), startOpts, settings, c.Range(1, nodes))

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
		m := c.NewMonitor(ctx, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			concurrency := ifLocal(c, "", " --concurrency="+fmt.Sprint(computeConcurrency(opts)))
			splits := " --splits=" + strconv.Itoa(computeNumSplits(opts))
			if opts.duration == 0 {
				opts.duration = 30 * time.Minute
			}
			duration := " --duration=" + ifLocal(c, "10s", opts.duration.String())
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
			histograms := " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
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

			var envFlags string
			if e := os.Getenv(envKVFlags); e != "" {
				envFlags = " " + e
			}

			url := fmt.Sprintf(" {pgurl:1-%d}", nodes)
			if opts.sharedProcessMT {
				url = fmt.Sprintf(" {pgurl:1-%d:%s}", nodes, appTenantName)
			}
			cmd := fmt.Sprintf(
				"./workload run kv --tolerate-errors --init --user=%s --password=%s", install.DefaultUser, install.DefaultPassword,
			) +
				histograms + concurrency + splits + duration + readPercent +
				batchSize + blockSize + sequential + envFlags + url
			c.Run(ctx, c.Node(nodes+1), cmd)
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
		{nodes: 1, cpus: 8, readPercent: 0, concMultiplier: 4096, blockSize: 1 << 16 /* 64 KB */},
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
		{nodes: 3, cpus: 8, readPercent: 0, splits: -1 /* no splits */},
		{nodes: 3, cpus: 8, readPercent: 95, splits: -1 /* no splits */},
		{nodes: 3, cpus: 32, readPercent: 0},
		{nodes: 3, cpus: 32, readPercent: 0, sharedProcessMT: true},
		{nodes: 3, cpus: 32, readPercent: 95},
		{nodes: 3, cpus: 32, readPercent: 95, sharedProcessMT: true},
		{nodes: 3, cpus: 32, readPercent: 0, splits: -1 /* no splits */},
		{nodes: 3, cpus: 32, readPercent: 95, splits: -1 /* no splits */},
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
		{nodes: 1, cpus: 8, readPercent: 95, spanReads: true, splits: -1 /* no splits */, disableLoadSplits: true, sequential: true},
		{nodes: 1, cpus: 32, readPercent: 95, spanReads: true, splits: -1 /* no splits */, disableLoadSplits: true, sequential: true},

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
			nameParts = append(nameParts, fmt.Sprintf("splt=%d", computeNumSplits(opts)))
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
		cSpec := r.MakeClusterSpec(opts.nodes+1, spec.CPU(opts.cpus), spec.SSD(opts.ssds), spec.RAID0(opts.raid0))

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

		r.Add(registry.TestSpec{
			Name:      strings.Join(nameParts, "/"),
			Owner:     owner,
			Benchmark: true,
			Cluster:   cSpec,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runKV(ctx, t, c, opts)
			},
			CompatibleClouds:  clouds,
			Suites:            suites,
			EncryptionSupport: encryption,
		})
	}
}

func registerKVContention(r registry.Registry) {
	const nodes = 4
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("kv/contention/nodes=%d", nodes),
		Owner:            registry.OwnerKV,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(nodes + 1),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))

			// Start the cluster with an extremely high txn liveness threshold.
			// If requests ever get stuck on a transaction that was abandoned
			// then it will take 10m for them to get unstuck, at which point the
			// QPS threshold check in the test is guaranteed to fail.
			settings := install.MakeClusterSettings()
			settings.Env = append(settings.Env, "COCKROACH_TXN_LIVENESS_HEARTBEAT_MULTIPLIER=600")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Range(1, nodes))

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
			m := c.NewMonitor(ctx, c.Range(1, nodes))
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
				cmd := fmt.Sprintf("./workload run kv --init --secondary-index --duration=%s "+
					"--cycle-length=%d --concurrency=%d --batch=%d --splits=%d {pgurl:1-%d}",
					duration, cycleLength, concurrency, batchSize, splits, nodes)
				start := timeutil.Now()
				c.Run(ctx, c.Node(nodes+1), cmd)
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
		Cluster:             r.MakeClusterSpec(4),
		CompatibleClouds:    registry.AllExceptAWS,
		Suites:              registry.Suites(registry.Nightly),
		Leases:              registry.EpochLeases,
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			nodes := c.Spec().NodeCount - 1
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))
			settings := install.MakeClusterSettings(install.ClusterSettingsOption{
				"sql.stats.automatic_collection.enabled": "false",
			})
			c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, c.Range(1, nodes))
			m := c.NewMonitor(ctx, c.Range(1, nodes))

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			err := WaitFor3XReplication(ctx, t, db)
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

			const kv = "./workload run kv --duration=10m --read-percent=0"

			// Initialize the database with ~10k ranges so that the absence of
			// quiescence hits hard once a node goes down.
			c.Run(ctx, c.Node(nodes+1), "./workload run kv --init --max-ops=1 --splits 10000 --concurrency 100 {pgurl:1}")
			c.Run(ctx, c.Node(nodes+1), kv+" --seed 0 {pgurl:1}")
			// Measure qps with all nodes up (i.e. with quiescence).
			qpsAllUp := qps(func() {
				c.Run(ctx, c.Node(nodes+1), kv+" --seed 1 {pgurl:1}")
			})
			// Graceful shut down third node.
			m.ExpectDeath()
			c.Stop(
				ctx, t.L(), option.NewStopOpts(option.Graceful(30)), c.Node(nodes),
			)
			// Measure qps with node down (i.e. without quiescence).
			qpsOneDown := qps(func() {
				// Use a different seed to make sure it's not just stepping into the
				// other earlier kv invocation's footsteps.
				c.Run(ctx, c.Node(nodes+1), kv+" --seed 2 {pgurl:1}")
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
		Name:             "kv/gracefuldraining/nodes=3",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			nodes := c.Spec().NodeCount - 1

			t.Status("starting cluster")
			// If the test ever fails, the person who investigates the
			// failure will likely be thankful for this additional logging.
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--vmodule=store=2,store_rebalancer=2")
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(1, nodes))

			db1 := c.Conn(ctx, t.L(), 1)
			defer db1.Close()
			db2 := c.Conn(ctx, t.L(), 2)
			defer db2.Close()

			err := WaitFor3XReplication(ctx, t, db1)
			require.NoError(t, err)

			t.Status("initializing workload")

			// Initialize the database with a lot of ranges so that there are
			// definitely a large number of leases on the node that we shut down
			// before it starts draining.
			c.Run(ctx, c.Node(1), "./cockroach workload init kv --splits 100 {pgurl:1}")

			m := c.NewMonitor(ctx, c.Nodes(1, nodes))
			m.ExpectDeath()

			// specifiedQPS is going to be the --max-rate for the kv workload.
			specifiedQPS := 1000
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
			desiredRunDuration := 5 * time.Minute
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf(
					"./cockroach workload run kv --duration=%s --read-percent=0 --concurrency=100 --max-rate=%d {pgurl:1-%d}",
					desiredRunDuration, specifiedQPS, nodes-1)
				t.WorkerStatus(cmd)
				defer func() {
					t.WorkerStatus("workload command completed")
					t.WorkerStatus()
				}()
				return c.RunE(ctx, c.Node(nodes+1), cmd)
			})

			verifyQPS := func(ctx context.Context) error {
				if qps := measureQPS(ctx, t, base.DefaultMetricsSampleInterval, db1, db2); qps < expectedQPS {
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

			t.Status("gracefully draining and restarting nodes")
			// Gracefully shut down the third node, let the cluster run for a
			// while, then restart it. Then repeat for good measure.
			for i := 0; i < 2; i++ {
				if i > 0 {
					// No need to wait extra during the first iteration: we
					// have already waited for the perf to become stable
					// above.
					t.Status("letting workload run with all nodes")
					select {
					case <-ctx.Done():
						return
					case <-time.After(1 * time.Minute):
					}
				}
				// Graceful drain: send SIGTERM, which should be sufficient to
				// stop the node. It will be followed by a non-graceful
				// SIGKILL a if the drain process does not finish within 30s
				stopOpts := option.NewStopOpts(option.Graceful(shutdownGracePeriod))
				c.Stop(ctx, t.L(), stopOpts, c.Node(nodes))
				t.Status("letting workload run with one node down")
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Minute):
				}
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(nodes))
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

func registerKVSplits(r registry.Registry) {
	for _, item := range []struct {
		quiesce bool
		splits  int
		leases  registry.LeaseType
		timeout time.Duration
	}{
		// NB: with 500000 splits, this test sometimes fails since it's pushing
		// far past the number of replicas per node we support, at least if the
		// ranges start to unquiesce (which can set off a cascade due to resource
		// exhaustion).
		{true, 300000, registry.EpochLeases, 2 * time.Hour},
		// This version of the test prevents range quiescence to trigger the
		// badness described above more reliably for when we wish to improve
		// the performance. For now, just verify that 30k unquiesced ranges
		// is tenable.
		{false, 30000, registry.EpochLeases, 2 * time.Hour},
		// Expiration-based leases prevent quiescence, and are also more expensive
		// to keep alive. Again, just verify that 30k ranges is ok.
		{false, 30000, registry.ExpirationLeases, 2 * time.Hour},
	} {
		item := item // for use in closure below
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("kv/splits/nodes=3/quiesce=%t/lease=%s", item.quiesce, item.leases),
			Owner:            registry.OwnerKV,
			Timeout:          item.timeout,
			Cluster:          r.MakeClusterSpec(4),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           item.leases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				nodes := c.Spec().NodeCount - 1
				c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))

				settings := install.MakeClusterSettings()
				settings.Env = append(settings.Env, "COCKROACH_MEMPROF_INTERVAL=1m", "COCKROACH_DISABLE_QUIESCENCE="+strconv.FormatBool(!item.quiesce))
				startOpts := option.DefaultStartOpts()
				startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--cache=256MiB")
				c.Start(ctx, t.L(), startOpts, settings, c.Range(1, nodes))

				t.Status("running workload")
				workloadCtx, workloadCancel := context.WithCancel(ctx)
				m := c.NewMonitor(workloadCtx, c.Range(1, nodes))
				m.Go(func(ctx context.Context) error {
					defer workloadCancel()
					concurrency := ifLocal(c, "", " --concurrency="+fmt.Sprint(nodes*64))
					splits := " --splits=" + ifLocal(c, "2000", fmt.Sprint(item.splits))
					cmd := fmt.Sprintf(
						"./workload run kv --init --max-ops=1"+
							concurrency+splits+
							" {pgurl:1-%d}",
						nodes)
					c.Run(ctx, c.Node(nodes+1), cmd)
					return nil
				})
				m.Wait()
			},
		})
	}
}

func registerKVScalability(r registry.Registry) {
	runScalability := func(ctx context.Context, t test.Test, c cluster.Cluster, percent int) {
		nodes := c.Spec().NodeCount - 1

		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))

		const maxPerNodeConcurrency = 64
		for i := nodes; i <= nodes*maxPerNodeConcurrency; i += nodes {
			i := i // capture loop variable
			c.Wipe(ctx, c.Range(1, nodes))
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, nodes))

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("./workload run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i)+
					" {pgurl:1-%d}",
					percent, nodes)

				return c.RunE(ctx, c.Node(nodes+1), cmd)
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
				Cluster:          r.MakeClusterSpec(7, spec.CPU(8)),
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
		nodes := c.Spec().NodeCount - 1
		doneInit := make(chan struct{})
		doneWorkload := make(chan struct{})
		c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(nodes+1))
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, nodes))

		t.Status("running workload")

		conns := make([]*gosql.DB, nodes)
		for i := 0; i < nodes; i++ {
			conns[i] = c.Conn(ctx, t.L(), i+1)
		}
		defer func() {
			for i := 0; i < nodes; i++ {
				conns[i].Close()
			}
		}()
		err := WaitFor3XReplication(ctx, t, conns[0])
		require.NoError(t, err)

		m := c.NewMonitor(ctx, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			defer close(doneWorkload)
			defer close(doneInit)
			cmd := "./workload init kv --splits=1000 {pgurl:1}"
			if err = c.RunE(ctx, c.Node(nodes+1), cmd); err != nil {
				return err
			}
			concurrency := ifLocal(c, "", " --concurrency="+fmt.Sprint(nodes*64))
			duration := " --duration=10m"
			readPercent := " --read-percent=50"
			// We run kv with --tolerate-errors, since the relocate workload is
			// expected to create `result is ambiguous (replica removed)` errors.
			cmd = fmt.Sprintf("./workload run kv --tolerate-errors"+
				concurrency+duration+readPercent+
				" {pgurl:1-%d}", nodes)
			start := timeutil.Now()
			if err = c.RunE(ctx, c.Node(nodes+1), cmd); err != nil {
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

					conn := conns[c.Range(1, nodes).RandNode()[0]-1]
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
						newReplicas := rand.Perm(nodes)[:3]
						_, err := conn.ExecContext(ctx, `
							ALTER TABLE
								kv.kv
							EXPERIMENTAL_RELOCATE
								SELECT ARRAY[$1, $2, $3], CAST(floor(random() * 9223372036854775808) AS INT)
						`, newReplicas[0]+1, newReplicas[1]+1, newReplicas[2]+1)
						if err != nil && !pgerror.IsSQLRetryableError(err) && !kv.IsExpectedRelocateError(err) {
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
			Cluster:          r.MakeClusterSpec(nodes+1, spec.CPU(cpus)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRangeLookups(ctx, t, c, item.workers, item.workloadType, item.maximumRangeLookupsPerSec)
			},
		})
	}
}

// measureQPS will measure the approx QPS at the time this command is run. The
// duration is the interval to measure over. Setting too short of an interval
// can mean inaccuracy in results. Setting too long of an interval may mean the
// impact is blurred out.
func measureQPS(
	ctx context.Context, t test.Test, duration time.Duration, dbs ...*gosql.DB,
) float64 {

	currentQPS := func() uint64 {
		var value uint64
		var wg sync.WaitGroup
		wg.Add(len(dbs))

		// Count the inserts before sleeping.
		for _, db := range dbs {
			db := db
			go func() {
				defer wg.Done()
				var v float64
				if err := db.QueryRowContext(
					ctx, `SELECT value FROM crdb_internal.node_metrics WHERE name = 'sql.insert.count'`,
				).Scan(&v); err != nil {
					t.Fatal(err)
				}
				atomic.AddUint64(&value, uint64(v))
			}()
		}
		wg.Wait()
		return value
	}

	// Measure the current time and the QPS now.
	startTime := timeutil.Now()
	beforeQPS := currentQPS()
	// Wait for the duration minus the first query time.
	select {
	case <-ctx.Done():
		return 0
	case <-time.After(duration - timeutil.Since(startTime)):
		return float64(currentQPS()-beforeQPS) / duration.Seconds()
	}
}

// registerKVRestartImpact measures the impact of stopping and then restarting a
// node during a write-heavy workload. Specifically the Raft log on the node
// falls behind when the node is down and when it comes back up it goes into IO
// Overload as it attempts to recover. Note that this test stops the replicate
// queue during the test to help isolate the impact of Raft backlog vs snapshot
// transfers.
func registerKVRestartImpact(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name: "kv/restart/nodes=12",
		// This test is expensive (104vcpu), we run it weekly.
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(13, spec.CPU(8)),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			nodes := c.Spec().NodeCount - 1
			workloadNode := c.Spec().NodeCount
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(1, nodes))

			// The duration of the outage.
			duration, err := time.ParseDuration(ifLocal(c, "20s", "10m"))
			assert.NoError(t, err)
			// Set the duration of the entire test to be 3x the outage duration.
			testDuration := 3 * duration

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			t.Status(fmt.Sprintf("initializing kv dataset <%s", 3*time.Minute))
			// We need a lot of ranges so that the individual ranges don't get truncated by Raft.
			splits := ifLocal(c, " --splits=3", " --splits=20000")
			c.Run(ctx, c.Node(workloadNode), "./cockroach workload init kv "+splits+" {pgurl:1}")

			t.Status(fmt.Sprintf("starting kv workload thread to run for %s", testDuration))
			m := c.NewMonitor(ctx, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				testDurationStr := " --duration=" + testDuration.String()
				concurrency := ifLocal(c, "  --concurrency=8", " --concurrency=64")
				// Don't include the last node when starting the workload since it will
				// stop in the middle, even with tolerate-errors set, it is still better
				// not to use. Write enough data per value to make sure we create a
				// large raft backlog.
				c.Run(ctx, c.Node(workloadNode),
					"./cockroach workload run kv --min-block-bytes=8192 --max-block-bytes=8192 --tolerate-errors --read-percent=50 "+
						testDurationStr+concurrency+fmt.Sprintf(" {pgurl:1-%d}", nodes-1),
				)
				return nil
			})

			// Let some data be written to all nodes in the cluster.
			t.Status(fmt.Sprintf("waiting %s to establish a base QPS", duration))
			time.Sleep(duration)
			qpsInitial := measureQPS(ctx, t, 5*time.Second, db)
			t.Status(fmt.Sprintf("initial (single node) qps: %.0f", qpsInitial))

			// Disable replicate queue on all nodes. This allows the test to reproduce
			// the issue without a lot of fill beforehand. The system won't try and
			// upreplicate these ranges somewhere else. We want to measure the impact
			// of raft catchup, not snapshot movement.
			setReplicateQueueEnabled := func(enabled bool) {
				for n := 1; n <= nodes; n++ {
					conn := c.Conn(ctx, t.L(), n)
					defer conn.Close()
					_, err := conn.ExecContext(ctx,
						`SELECT crdb_internal.kv_set_queue_active('replicate', $1)`, enabled)
					require.NoError(t, err)
				}
			}
			setReplicateQueueEnabled(false)

			// Gracefully shut down the last node to let it transfer leases cleanly.
			// Wait enough time to let it fall behind on Raft. Since there are a lot
			// of ranges, only a small number will be upreplicated during this time.
			gracefulOpts := option.DefaultStopOpts()
			gracefulOpts.RoachprodOpts.Sig = 15 // SIGTERM
			gracefulOpts.RoachprodOpts.Wait = true
			c.Stop(ctx, t.L(), gracefulOpts, c.Node(nodes))
			t.Status(fmt.Sprintf("waiting %x after stopping node to allow the node to fall behind", duration))
			time.Sleep(duration)

			// Start the node again. It will attempt to catch up and go into an IO
			// Overload scenario. Re-enable the replicate queue now so that leases
			// begin to transfer.
			t.Status("restarting stopped node and the replicate queue")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(nodes))
			setReplicateQueueEnabled(true)
			t.Status(fmt.Sprintf("waiting %s for the workload to finish and measuring the impact of the outage", duration))

			// Wait for IO overload and enough leases to be transferred back.
			if !c.IsLocal() {
				time.Sleep(3 * time.Minute)
			}
			qpsFinal := measureQPS(ctx, t, 5*time.Second, db)
			t.Status(fmt.Sprintf("post outage qps: %.0f", qpsFinal))

			// Pass the test if the QPS is within a factor of 2. Often the qpsFinal is
			// 0 in most rus, so avoid a divide by 0 error.
			assert.Greater(t, qpsFinal/qpsInitial, 0.5)

			// Wait for the workload to finish.
			m.Wait()
		},
	})
}
