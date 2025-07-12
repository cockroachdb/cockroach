// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// registerDiskStalledWALFailover registers the disk stall WAL failover tests.
// These tests assert that a storage engine configured with WAL failover
// survives a temporary disk stall through failing over to a secondary disk.
func registerDiskStalledWALFailover(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:                "disk-stalled/wal-failover/among-stores",
		Owner:               registry.OwnerStorage,
		Cluster:             r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode(), spec.ReuseNone(), spec.SSD(2)),
		CompatibleClouds:    registry.OnlyGCE,
		Suites:              registry.Suites(registry.Nightly),
		Timeout:             3 * time.Hour,
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		// Encryption is implemented within the virtual filesystem layer,
		// just like disk-health monitoring. It's important to exercise
		// encryption-at-rest to ensure there is not unmonitored I/O within
		// the encryption-at-rest implementation that could indefinitely
		// stall the process during a disk stall.
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDiskStalledWALFailover(ctx, t, c)
		},
	})
}

func runDiskStalledWALFailover(ctx context.Context, t test.Test, c cluster.Cluster) {
	startSettings := install.MakeClusterSettings()
	// Set a high value for the max sync durations to avoid the disk
	// stall detector fataling the node.
	const maxSyncDur = 60 * time.Second
	startSettings.Env = append(startSettings.Env,
		"COCKROACH_AUTO_BALLAST=false",
		fmt.Sprintf("COCKROACH_LOG_MAX_SYNC_DURATION=%s", maxSyncDur),
		fmt.Sprintf("COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT=%s", maxSyncDur))

	t.Status("setting up disk staller")
	s := roachtestutil.MakeDmsetupDiskStaller(t, c)
	s.Setup(ctx)
	defer s.Cleanup(ctx)

	t.Status("starting cluster")
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.WALFailover = "among-stores"
	startOpts.RoachprodOpts.StoreCount = 2
	c.Start(ctx, t.L(), startOpts, startSettings, c.CRDBNodes())

	// Open a SQL connection to n1, the node that will be stalled.
	n1Conn := c.Conn(ctx, t.L(), 1)
	defer n1Conn.Close()
	require.NoError(t, n1Conn.PingContext(ctx))
	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), n1Conn))
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Nodes(2))
	require.NoError(t, err)
	adminURL := adminUIAddrs[0]
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)
	_, err = n1Conn.ExecContext(ctx, `USE kv;`)
	require.NoError(t, err)

	t.Status("starting workload")
	workloadStartAt := timeutil.Now()
	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 0 `+
			`--duration 60m --concurrency 4096 --ramp=1m --max-rate 4096 --tolerate-errors `+
			` --min-block-bytes=2048 --max-block-bytes=2048 --timeout 1s `+
			`{pgurl:1-3}`)
		return nil
	})

	const pauseBetweenStalls = 10 * time.Minute
	t.Status("pausing ", pauseBetweenStalls, " before simulated disk stall on n1")
	ticker := time.NewTicker(time.Second)
	nextStallAt := workloadStartAt.Add(pauseBetweenStalls)
	defer ticker.Stop()

	progressEvery := roachtestutil.Every(time.Minute)
	for timeutil.Since(workloadStartAt) < time.Hour+5*time.Minute {
		select {
		case <-ctx.Done():
			t.Fatalf("context done before finished workload: %s", ctx.Err())
		case now := <-ticker.C:
			if now.Before(nextStallAt) {
				if progressEvery.ShouldLog() {
					t.Status("pausing ", nextStallAt.Sub(now), " before next simulated disk stall on n1")
				}
				continue
			}
			func() {
				t.Status("Stalling disk on n1")
				stopStall := time.After(30 * time.Second)
				s.Stall(ctx, c.Node(1))
				t.Status("Stalled disk on n1")
				// NB: We use a background context in the defer'ed unstall command,
				// otherwise on test failure our Unstall calls will be ignored. Leaving
				// the disk stalled will prevent artifact collection, making debugging
				// difficult.
				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					t.Status("Unstalling disk on n1")
					if err = s.Unstall(ctx, c.Node(1)); err != nil {
						t.Fatal(err)
					}
					t.Status("Unstalled disk on n1")
				}()

				t.Status("waiting for 30s to elapse before unstalling")
				select {
				case <-ctx.Done():
					t.Fatalf("context done while stall induced: %s", ctx.Err())
				case <-stopStall:
					// Return from the anonymous function, allowing the
					// defer to unstall the node.
					return
				}
			}()
			nextStallAt = now.Add(pauseBetweenStalls)
		}
	}
	t.Status("exited stall loop")

	time.Sleep(1 * time.Second)
	exit, ok := getProcessExitMonotonic(ctx, t, c, 1)
	if ok && exit > 0 {
		t.Fatal("process exited unexpectedly")
	}

	data := mustGetMetrics(ctx, c, t, adminURL, install.SystemInterfaceName,
		workloadStartAt.Add(5*time.Minute),
		timeutil.Now().Add(-time.Minute),
		[]tsQuery{
			{name: "cr.node.sql.exec.latency-p99.99", queryType: total, sources: []string{"2"}},
			{name: "cr.store.storage.wal.failover.secondary.duration", queryType: total, sources: []string{"1"}},
		})

	for _, dp := range data.Results[0].Datapoints {
		if dur := time.Duration(dp.Value); dur > time.Second {
			t.Errorf("unexpectedly high p99.99 latency %s at %s", dur, timeutil.Unix(0, dp.TimestampNanos).Format(time.RFC3339))
		}
	}

	// Over the course of the 1h test, we expect ~6 stalls each lasting 30s. Assert that
	// the total time spent writing to the secondary is at least 1 minute.
	durInFailover := time.Duration(data.Results[1].Datapoints[len(data.Results[0].Datapoints)-1].Value)
	t.L().PrintfCtx(ctx, "duration s1 spent writing to secondary %s", durInFailover)
	if durInFailover < 60*time.Second {
		t.Errorf("expected s1 to spend at least 60s writing to secondary, but spent %s", durInFailover)
	}
	// Wait for the workload to finish (if it hasn't already).
	m.Wait()

	// Shut down the nodes, allowing any devices to be unmounted during cleanup.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.CRDBNodes())
}

// registerDiskStalledDetection registers the disk stall detection tests. These
// tests assert that a disk stall is detected and the process crashes
// appropriately.
func registerDiskStalledDetection(r registry.Registry) {
	stallers := map[string]func(test.Test, cluster.Cluster) diskStaller{
		"dmsetup": func(t test.Test, c cluster.Cluster) diskStaller { return roachtestutil.MakeDmsetupDiskStaller(t, c) },
		"cgroup/read-write/logs-too=false": func(t test.Test, c cluster.Cluster) diskStaller {
			return roachtestutil.MakeCgroupDiskStaller(t, c, true, false)
		},
		"cgroup/read-write/logs-too=true": func(t test.Test, c cluster.Cluster) diskStaller {
			return roachtestutil.MakeCgroupDiskStaller(t, c, true, true)
		},
		"cgroup/write-only/logs-too=true": func(t test.Test, c cluster.Cluster) diskStaller {
			return roachtestutil.MakeCgroupDiskStaller(t, c, false, true)
		},
	}

	for name, makeStaller := range stallers {
		r.Add(registry.TestSpec{
			Name:  fmt.Sprintf("disk-stalled/detection/%s", name),
			Owner: registry.OwnerStorage,
			// Use PDs in an attempt to work around flakes encountered when using SSDs.
			// See #97968.
			Cluster:             r.MakeClusterSpec(4, spec.WorkloadNode(), spec.ReuseNone(), spec.DisableLocalSSD()),
			CompatibleClouds:    registry.OnlyGCE,
			Suites:              registry.Suites(registry.Nightly),
			Timeout:             30 * time.Minute,
			SkipPostValidations: registry.PostValidationNoDeadNodes,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDiskStalledDetection(ctx, t, c, makeStaller(t, c), true /* doStall */)
			},
			// Encryption is implemented within the virtual filesystem layer,
			// just like disk-health monitoring. It's important to exercise
			// encryption-at-rest to ensure there is not unmonitored I/O within
			// the encryption-at-rest implementation that could indefinitely
			// stall the process during a disk stall.
			EncryptionSupport: registry.EncryptionMetamorphic,
			Leases:            registry.MetamorphicLeases,
		})
	}
}

func runDiskStalledDetection(
	ctx context.Context, t test.Test, c cluster.Cluster, s diskStaller, doStall bool,
) {
	const maxSyncDur = 10 * time.Second

	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ExtraArgs = []string{
		"--store", s.DataDir(),
		"--log", fmt.Sprintf(`{sinks: {stderr: {filter: INFO}}, file-defaults: {dir: "%s"}}`, s.LogDir()),
	}
	startSettings := install.MakeClusterSettings()
	startSettings.Env = append(startSettings.Env,
		"COCKROACH_AUTO_BALLAST=false",
		fmt.Sprintf("COCKROACH_LOG_MAX_SYNC_DURATION=%s", maxSyncDur),
		fmt.Sprintf("COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT=%s", maxSyncDur))

	t.Status("setting up disk staller")
	s.Setup(ctx)

	// NB: We use a background context in the defer'ed cleanup command,
	// otherwise on test failure our c.Run calls will be ignored. Leaving
	// the disk stalled will prevent artifact collection, making debugging
	// difficult.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.Cleanup(ctx)
	}()

	t.Status("starting cluster")
	c.Start(ctx, t.L(), startOpts, startSettings, c.CRDBNodes())

	// Assert the process monotonic times are as expected.
	var ok bool
	var start, exit time.Duration
	start, ok = getProcessStartMonotonic(ctx, t, c, 1)
	if !ok {
		t.Fatal("unable to retrieve process start time; did Cockroach not start?")
	}
	if exit, ok = getProcessExitMonotonic(ctx, t, c, 1); ok && exit > 0 {
		t.Fatalf("process has an exit monotonic time of %d; did Cockroach already exit?", exit)
	}

	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Nodes(2))
	require.NoError(t, err)
	adminURL := adminUIAddrs[0]

	// Open SQL connections—one to n1, the node that will be stalled, and one to
	// n2 that should remain open and active for the remainder.
	n1Conn := c.Conn(ctx, t.L(), 1)
	defer n1Conn.Close()
	n2conn := c.Conn(ctx, t.L(), 2)
	defer n2conn.Close()
	require.NoError(t, n1Conn.PingContext(ctx))

	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), n2conn))

	c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	_, err = n2conn.ExecContext(ctx, `USE kv;`)
	require.NoError(t, err)

	t.Status("starting workload")
	workloadStartAt := timeutil.Now()
	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		// NB: Since we stall node 1, we run the workload only on nodes 2-3 so
		// the post-stall QPS isn't affected by the fact that 1/3rd of workload
		// workers just can't connect to a working node.
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload run kv --read-percent 50 `+
			`--duration 10m --concurrency 256 --max-rate 2048 --tolerate-errors `+
			` --min-block-bytes=512 --max-block-bytes=512 `+
			`{pgurl:2-3}`)
		return nil
	})

	// Wait between [3m,6m) before stalling the disk.
	pauseDur := 3*time.Minute + time.Duration(rand.Intn(3))*time.Minute
	pauseBeforeStall := time.After(pauseDur)
	t.Status("pausing ", pauseDur, " before inducing write stall")
	select {
	case <-ctx.Done():
		t.Fatalf("context done before stall: %s", ctx.Err())
	case <-pauseBeforeStall:
	}

	stallAt := timeutil.Now()
	response := mustGetMetrics(ctx, c, t, adminURL, install.SystemInterfaceName, workloadStartAt, stallAt, []tsQuery{
		{name: "cr.node.sql.query.count", queryType: total},
	})
	cum := response.Results[0].Datapoints
	totalQueriesPreStall := sumCounterIncreases(cum)
	t.L().PrintfCtx(ctx, "%.2f queries completed before stall", totalQueriesPreStall)

	t.Status("inducing write stall")
	if doStall {
		m.ExpectDeath()
	}
	s.Stall(ctx, c.Node(1))

	// Wait twice the maximum sync duration and check if our SQL connection to
	// node 1 is still alive. It should've been terminated.
	{
		t.Status("waiting ", 2*maxSyncDur, " before checking SQL conn to n1")
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * maxSyncDur):
		}
		t.Status("pinging SQL connection to n1")
		err := n1Conn.PingContext(ctx)
		t.L().PrintfCtx(ctx, "pinging n1's connection: %v", err)
		if doStall && err == nil {
			t.Fatal("connection to n1 is still alive")
		} else if !doStall && err != nil {
			t.Fatalf("connection to n1 is dead: %s", err)
		}
	}

	// Let the workload continue after the stall.
	workloadContinuedAt := timeutil.Now()
	workloadAfterDur := 10*time.Minute - workloadContinuedAt.Sub(workloadStartAt)
	t.Status("letting workload continue for ", workloadAfterDur, " with n1 stalled")
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(workloadAfterDur):
	}

	{
		now := timeutil.Now()
		response := mustGetMetrics(ctx, c, t, adminURL, install.SystemInterfaceName, workloadContinuedAt, now, []tsQuery{
			{name: "cr.node.sql.query.count", queryType: total},
		})
		cum := response.Results[0].Datapoints
		totalQueriesPostStall := sumCounterIncreases(cum)
		preStallQPS := totalQueriesPreStall / stallAt.Sub(workloadStartAt).Seconds()
		postStallQPS := totalQueriesPostStall / workloadAfterDur.Seconds()
		t.L().PrintfCtx(ctx, "%.2f total queries committed after stall\n", totalQueriesPostStall)
		t.L().PrintfCtx(ctx, "pre-stall qps: %.2f, post-stall qps: %.2f\n", preStallQPS, postStallQPS)
		if postStallQPS < preStallQPS/2 {
			t.Fatalf("post-stall QPS %.2f is less than 50%% of pre-stall QPS %.2f", postStallQPS, preStallQPS)
		}
	}

	{
		t.Status("counting kv rows")
		var rowCount int
		require.NoError(t, n2conn.QueryRowContext(ctx, `SELECT count(v) FROM kv`).Scan(&rowCount))
		t.L().PrintfCtx(ctx, "Scan found %d rows.\n", rowCount)
	}

	// Unstall the stalled node. It should be able to be reaped.
	// Note we only log errors since cgroup unstall is expected to fail due to
	// nodes panicking from a detected disk stall.
	if err = s.Unstall(ctx, c.Node(1)); err != nil {
		t.L().Printf("failed to unstall disk: %v", err)
	}
	time.Sleep(1 * time.Second)
	exit, ok = getProcessExitMonotonic(ctx, t, c, 1)
	if doStall {
		if !ok {
			t.Fatalf("unable to retrieve process exit time; stall went undetected")
		}
		t.L().PrintfCtx(ctx, "node exited at %s after test start\n", exit-start)
	} else if ok && exit > 0 {
		t.Fatal("no stall induced, but process exited")
	}
	// Wait for the workload to finish (if it hasn't already).
	m.Wait()

	// Shut down the nodes, allowing any devices to be unmounted during cleanup.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.CRDBNodes())
}

// registerDiskStalledWALFailoverWithProgress registers a test that induces
// WAL failover while the workload is running. This test is similar to
// disk-stalled/wal-failover/among-stores, but allows some progress to be
// made while are in failover. Specifically, we'll oscillate both the
// workload and failover states in the following pattern with some jitter in
// the timing of each operation:
//
// Time (minutes)    0    1    2    3    4    5    6    7    8    9    10   11   12   13    14    15
// Workload          |----|----|----|       |----|----|----|      |----|----|----|    |----|----|----|
// Disk Stalls         |----|----|----|    |----|----|----|  |----|----|----|
//
// Note that:
// Every 4th run, the workload will run without any disk stalls.
// Each workload and stall phase is 3m.
// Each operation has a min 30s + random 0-2m wait after both operations finish.
//
// The workload run in this test is meant to ramp up to 50% disk bandwidth.
// See: https://cloud.google.com/compute/docs/disks/performance for estimations on disk performance.
// For a 100GB pd-ssd disk we get an estimated max performance of:
// - 6K IOPS (3K baseline + 30 ops * 100GB disk).
// - 288 MiB/s (240 MiB/s baseline + 0.48 * 100GB disk).
func registerDiskStalledWALFailoverWithProgress(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "disk-stalled/wal-failover/among-stores/with-progress",
		Owner: registry.OwnerStorage,
		Cluster: r.MakeClusterSpec(4,
			spec.CPU(16),
			spec.WorkloadNode(),
			spec.ReuseNone(),
			spec.DisableLocalSSD(),
			spec.GCEVolumeCount(2),
			spec.GCEVolumeType("pd-ssd"),
			spec.VolumeSize(100),
		),
		CompatibleClouds:    registry.OnlyGCE,
		Suites:              registry.Suites(registry.Nightly),
		Timeout:             2 * time.Hour,
		SkipPostValidations: registry.PostValidationNoDeadNodes,
		EncryptionSupport:   registry.EncryptionMetamorphic,
		Leases:              registry.MetamorphicLeases,
		Monitor:             true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDiskStalledWALFailoverWithProgress(ctx, t, c)
		},
	})
}

func runDiskStalledWALFailoverWithProgress(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		testDuration = 1 * time.Hour
		// We'll issue short stalls every 5s to keep us in the failover state.
		stallInterval = 5 * time.Second
		shortStallDur = 200 * time.Millisecond
		// For each loop, each operation will start after a random wait between [30s, 150s).
		operationWaitBase = 30 * time.Second
		waitJitterMax     = 2 * time.Minute
		operationDur      = 3 * time.Minute
		// QPS sampling parameters.
		sampleInterval = 10 * time.Second
		errorTolerance = 0.2 // 20% tolerance for throughput variation.
	)

	t.Status("setting up disk staller")
	// Use CgroupDiskStaller with readsToo=false to only stall writes.
	s := roachtestutil.MakeCgroupDiskStaller(t, c, false /* readsToo */, false /* logsToo */)
	s.Setup(ctx)
	// NB: We use a background context in the defer'ed cleanup command,
	// otherwise on test failure our c.Run calls will be ignored. Leaving
	// the disk stalled will prevent artifact collection, making debugging
	// difficult.
	defer s.Cleanup(context.Background())

	t.Status("starting cluster")
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.WALFailover = "among-stores"
	startOpts.RoachprodOpts.StoreCount = 2
	startSettings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, startSettings, c.CRDBNodes())

	// Open a SQL connection to n1, the node that will be stalled.
	n1Conn := c.Conn(ctx, t.L(), 1)
	defer n1Conn.Close()
	require.NoError(t, n1Conn.PingContext(ctx))
	// Wait for upreplication.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), n1Conn))
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Nodes(2))
	require.NoError(t, err)
	adminURL := adminUIAddrs[0]
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)
	_, err = n1Conn.ExecContext(ctx, `USE kv;`)
	require.NoError(t, err)

	t.Status("starting oscillating workload and disk stall pattern")
	testStartedAt := timeutil.Now()
	g := t.NewGroup(task.WithContext(ctx))

	// Setup stats collector.
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0]).
		WithNodeExporter(c.CRDBNodes().InstallNodes()).
		WithCluster(c.CRDBNodes().InstallNodes())
	err = c.StartGrafana(ctx, t.L(), promCfg)
	require.NoError(t, err)
	cleanupFunc := func() {
		if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
		}
	}
	defer cleanupFunc()

	promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
	require.NoError(t, err)
	statCollector := clusterstats.NewStatsCollector(ctx, promClient)

	// Track mean throughput for each iteration.
	var iterationMeans []float64

	iteration := 1
	for timeutil.Since(testStartedAt) < testDuration {
		if t.Failed() {
			t.Fatalf("test failed, stopping further iterations")
		}

		workloadWaitDur := operationWaitBase + time.Duration(rand.Int63n(int64(waitJitterMax)))
		t.Status("next workload run in ", workloadWaitDur)

		// Channels to signal workload state.
		workloadStarted := make(chan struct{})
		workloadFinished := make(chan struct{})

		g.Go(func(ctx context.Context, _ *logger.Logger) error {
			select {
			case <-ctx.Done():
				t.Fatalf("context done before workload started: %s", ctx.Err())
			case <-time.After(workloadWaitDur):
				t.Status("starting workload")
				close(workloadStarted)
				workloadCmd := `./cockroach workload run kv --read-percent 0 ` +
					fmt.Sprintf(`--duration %s --concurrency 4096 --max-rate=2048 --tolerate-errors `, operationDur.String()) +
					`--min-block-bytes=4096 --max-block-bytes=4096 --timeout 1s {pgurl:1-3}`
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), workloadCmd)
				close(workloadFinished)
				return nil
			}
			return nil
		}, task.Name("workload-run"))

		// Collecting QPS samples while the workload is running and verify
		// that the throughput is within errorTolerance of the mean.
		var samples []float64
		g.Go(func(ctx context.Context, _ *logger.Logger) error {

			// Wait for workload to start.
			select {
			case <-ctx.Done():
				t.Fatalf("context done before workload started: %s", ctx.Err())
			case <-workloadStarted:
			}

			// Wait 20s after workload starts before beginning sampling.
			select {
			case <-ctx.Done():
				t.Fatalf("context done before workload started: %s", ctx.Err())
			case <-time.After(30 * time.Second):
				t.Status("starting QPS sampling")
			}

			// We want to stop sampling 10s before workload ends to avoid sampling during shutdown.
			// We'll take approx. 14 samples with this configuration.
			samplingDuration := operationDur - 40*time.Second // 30s initial wait + 10s buffer at workload end
			sampleCount := int(samplingDuration / sampleInterval)

			sampleTimer := time.NewTicker(sampleInterval)
			defer sampleTimer.Stop()

			done := false
			for i := 0; i < sampleCount && !done; i++ {
				select {
				case <-ctx.Done():
					t.Fatalf("context done while sampling: %s", ctx.Err())
				case <-workloadFinished:
					done = true
				case <-sampleTimer.C:
					metric := `rate(sql_select_count[30s]) + rate(sql_insert_count[30s]) + rate(sql_update_count[30s])`
					stats, err := statCollector.CollectPoint(ctx, t.L(), timeutil.Now(), metric)
					if err != nil {
						t.Errorf("failed to collect throughput stats: %v", err)
						continue
					}
					var clusterQPS float64
					if nodeStats, ok := stats["node"]; ok {
						for _, stat := range nodeStats {
							clusterQPS += stat.Value
						}
					} else {
						t.Status("no node stats found for throughput metric ", metric)
						continue
					}
					t.Status("sampled cluster QPS: ", clusterQPS)
					samples = append(samples, clusterQPS)
				}
			}

			t.Status(fmt.Sprintf("workload finished, %d samples collected", len(samples)))
			return nil
		}, task.Name("qps-sampling"))

		// Every 4th iteration, we'll skip the disk stall phase.
		if iteration%4 != 0 {
			// Calculate next stall phase with jitter.
			diskStallWaitDur := operationWaitBase + time.Duration(rand.Int63n(int64(waitJitterMax)))
			t.Status("next stall phase in ", diskStallWaitDur)

			g.Go(func(ctx context.Context, _ *logger.Logger) error {
				select {
				case <-ctx.Done():
					t.Fatalf("context done before stall started: %s", ctx.Err())
				case <-time.After(diskStallWaitDur):
					t.Status("starting disk stall")
				}
				// Execute short 200ms stalls every 5s for 3 minutes.
				s.StallCycle(ctx, c.Node(1), shortStallDur, stallInterval)
				select {
				case <-ctx.Done():
					t.Fatalf("context done while stall induced: %s", ctx.Err())
				case <-time.After(operationDur):
					if err = s.Unstall(ctx, c.Node(1)); err != nil {
						t.Fatal(err)
					}
					t.Status("disk stalls stopped")
				}
				return nil
			}, task.Name("disk-stall-phase"))
		} else {
			t.Status("skipping disk stall phase for this iteration")
		}

		// Wait for all goroutines to complete.
		g.Wait()

		// Validate throughput samples are within tolerance.
		meanThroughput := roachtestutil.GetMeanOverLastN(len(samples), samples)
		t.Status("mean throughput for iteration", iteration, ": ", meanThroughput)
		for _, sample := range samples {
			require.InEpsilonf(t, meanThroughput, sample, errorTolerance,
				"sample %f is not within tolerance of mean %f", sample, meanThroughput)
		}
		iterationMeans = append(iterationMeans, meanThroughput)
		iteration++
	}

	t.Status("exited control loop")

	time.Sleep(1 * time.Second)
	exit, ok := getProcessExitMonotonic(ctx, t, c, 1)
	if ok && exit > 0 {
		t.Fatal("process exited unexpectedly")
	}

	// Validate overall throughput consistency across iterations.
	overallMean := roachtestutil.GetMeanOverLastN(len(iterationMeans), iterationMeans)
	for _, mean := range iterationMeans {
		require.InEpsilonf(t, overallMean, mean, errorTolerance,
			"iteration mean %f is not within tolerance of overall mean %f", mean, overallMean)
	}

	data := mustGetMetrics(ctx, c, t, adminURL, install.SystemInterfaceName,
		testStartedAt.Add(5*time.Minute),
		timeutil.Now().Add(-time.Minute),
		[]tsQuery{
			{name: "cr.store.storage.wal.failover.secondary.duration", queryType: total, sources: []string{"1"}},
		})

	// Over the course of the 1h test, we expect many short stalls. Assert that
	// the total time spent writing to the secondary is at least 10m.
	durInFailover := time.Duration(data.Results[0].Datapoints[len(data.Results[0].Datapoints)-1].Value)
	t.L().PrintfCtx(ctx, "duration s1 spent writing to secondary %s", durInFailover)
	if durInFailover < 10*time.Minute {
		t.Errorf("expected s1 to spend at least 10m writing to secondary, but spent %s", durInFailover)
	}
}

func getProcessStartMonotonic(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) (since time.Duration, ok bool) {
	return getProcessMonotonicTimestamp(ctx, t, c, nodeID, "ActiveEnterTimestampMonotonic")
}

func getProcessExitMonotonic(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int,
) (since time.Duration, ok bool) {
	return getProcessMonotonicTimestamp(ctx, t, c, nodeID, "ActiveExitTimestampMonotonic")
}

func getProcessMonotonicTimestamp(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeID int, prop string,
) (time.Duration, bool) {
	details, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(nodeID)), fmt.Sprintf(
		"systemctl show %s --property=%s", roachtestutil.SystemInterfaceSystemdUnitName(), prop))
	require.NoError(t, err)
	require.NoError(t, details.Err)
	parts := strings.Split(details.Stdout, "=")
	if len(parts) < 2 {
		return 0, false
	}
	s := strings.TrimSpace(parts[1])
	if s == "" {
		return 0, false
	}
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("unable to parse monotonic timestamp %q: %s", parts[1], err)
	}
	if u == 0 {
		return 0, true
	}
	return time.Duration(u) * time.Microsecond, true
}

type diskStaller = roachtestutil.DiskStaller
