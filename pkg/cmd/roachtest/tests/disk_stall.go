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
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		Cluster:             r.MakeClusterSpec(4, spec.CPU(16), spec.ReuseNone(), spec.SSD(2)),
		CompatibleClouds:    registry.AllExceptAWS,
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
			runDiskStalledWALFailover(ctx, t, c, "among-stores")
		},
	})
}

func runDiskStalledWALFailover(
	ctx context.Context, t test.Test, c cluster.Cluster, failoverFlag string,
) {
	startSettings := install.MakeClusterSettings()
	// Set a high value for the max sync durations to avoid the disk
	// stall detector fataling the node.
	const maxSyncDur = 60 * time.Second
	startSettings.Env = append(startSettings.Env,
		"COCKROACH_AUTO_BALLAST=false",
		fmt.Sprintf("COCKROACH_LOG_MAX_SYNC_DURATION=%s", maxSyncDur),
		fmt.Sprintf("COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT=%s", maxSyncDur))

	t.Status("setting up disk staller")
	s := &dmsetupDiskStaller{t: t, c: c, logsToo: true}
	s.Setup(ctx)
	defer s.Cleanup(ctx)

	t.Status("starting cluster")
	startOpts := option.DefaultStartOpts()
	if failoverFlag == "among-stores" {
		startOpts.RoachprodOpts.StoreCount = 2
	}
	startOpts.RoachprodOpts.ExtraArgs = []string{
		// Adopt buffering of the file logging to ensure that we don't block on
		// flushing logs to the stalled device.
		"--log", fmt.Sprintf(`{file-defaults: {dir: "%s", buffered-writes: false, buffering: {max-staleness: 1s, flush-trigger-size: 256KiB, max-buffer-size: 50MiB}}}`, s.LogDir()),
		"--wal-failover=" + failoverFlag,
	}
	c.Start(ctx, t.L(), startOpts, startSettings, c.Range(1, 3))

	// Open a SQL connection to n1, the node that will be stalled.
	n1Conn := c.Conn(ctx, t.L(), 1)
	defer n1Conn.Close()
	require.NoError(t, n1Conn.PingContext(ctx))
	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), n1Conn))
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Nodes(2))
	require.NoError(t, err)
	adminURL := adminUIAddrs[0]
	c.Run(ctx, option.WithNodes(c.Node(4)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)
	_, err = n1Conn.ExecContext(ctx, `USE kv;`)
	require.NoError(t, err)

	t.Status("starting workload")
	workloadStartAt := timeutil.Now()
	m := c.NewMonitor(ctx, c.Range(1, 3))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, option.WithNodes(c.Node(4)), `./cockroach workload run kv --read-percent 0 `+
			`--duration 60m --concurrency 4096 --max-rate 4096 --tolerate-errors `+
			` --min-block-bytes=2048 --max-block-bytes=2048 --timeout 1s `+
			`{pgurl:1-3}`)
		return nil
	})
	defer m.Wait()

	const pauseBetweenStalls = 10 * time.Minute
	t.Status("pausing ", pauseBetweenStalls, " before simulated disk stall on n1")
	ticker := time.NewTicker(time.Second)
	nextStallAt := workloadStartAt.Add(pauseBetweenStalls)
	defer ticker.Stop()

	progressEvery := log.Every(time.Minute)
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
				s.Stall(ctx, c.Node(1))
				// NB: We use a background context in the defer'ed unstall command,
				// otherwise on test failure our Unstall calls will be ignored. Leaving
				// the disk stalled will prevent artifact collection, making debugging
				// difficult.
				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					s.Unstall(ctx, c.Node(1))
				}()

				select {
				case <-ctx.Done():
					t.Fatalf("context done while stall induced: %s", ctx.Err())
				case <-time.After(30 * time.Second):
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
		t.Fatal("process exited unexectedly")
	}

	data := mustGetMetrics(ctx, c, t, adminURL,
		workloadStartAt.Add(time.Minute),
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

	// Shut down the nodes, allowing any devices to be unmounted during cleanup.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Range(1, 3))
}

// registerDiskStalledDetection registers the disk stall detection tests. These
// tests assert that a disk stall is detected and the process crashes
// appropriately.
func registerDiskStalledDetection(r registry.Registry) {
	stallers := map[string]func(test.Test, cluster.Cluster) diskStaller{
		"dmsetup": func(t test.Test, c cluster.Cluster) diskStaller { return &dmsetupDiskStaller{t: t, c: c} },
		"cgroup/read-write/logs-too=false": func(t test.Test, c cluster.Cluster) diskStaller {
			return &cgroupDiskStaller{t: t, c: c, readOrWrite: []bandwidthReadWrite{writeBandwidth, readBandwidth}}
		},
		"cgroup/read-write/logs-too=true": func(t test.Test, c cluster.Cluster) diskStaller {
			return &cgroupDiskStaller{t: t, c: c, readOrWrite: []bandwidthReadWrite{writeBandwidth, readBandwidth}, logsToo: true}
		},
		"cgroup/write-only/logs-too=true": func(t test.Test, c cluster.Cluster) diskStaller {
			return &cgroupDiskStaller{t: t, c: c, readOrWrite: []bandwidthReadWrite{writeBandwidth}, logsToo: true}
		},
	}

	for name, makeStaller := range stallers {
		name, makeStaller := name, makeStaller
		r.Add(registry.TestSpec{
			Name:  fmt.Sprintf("disk-stalled/detection/%s", name),
			Owner: registry.OwnerStorage,
			// Use PDs in an attempt to work around flakes encountered when using SSDs.
			// See #97968.
			Cluster:             r.MakeClusterSpec(4, spec.ReuseNone(), spec.DisableLocalSSD()),
			CompatibleClouds:    registry.AllExceptAWS,
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
	defer s.Cleanup(ctx)

	t.Status("starting cluster")
	c.Start(ctx, t.L(), startOpts, startSettings, c.Range(1, 3))

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
	require.NoError(t, WaitFor3XReplication(ctx, t, t.L(), n2conn))

	c.Run(ctx, option.WithNodes(c.Node(4)), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

	_, err = n2conn.ExecContext(ctx, `USE kv;`)
	require.NoError(t, err)

	t.Status("starting workload")
	workloadStartAt := timeutil.Now()
	m := c.NewMonitor(ctx, c.Range(1, 3))
	m.Go(func(ctx context.Context) error {
		// NB: Since we stall node 1, we run the workload only on nodes 2-3 so
		// the post-stall QPS isn't affected by the fact that 1/3rd of workload
		// workers just can't connect to a working node.
		c.Run(ctx, option.WithNodes(c.Node(4)), `./cockroach workload run kv --read-percent 50 `+
			`--duration 10m --concurrency 256 --max-rate 2048 --tolerate-errors `+
			` --min-block-bytes=512 --max-block-bytes=512 `+
			`{pgurl:2-3}`)
		return nil
	})
	defer m.Wait()

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
	response := mustGetMetrics(ctx, c, t, adminURL, workloadStartAt, stallAt, []tsQuery{
		{name: "cr.node.sql.query.count", queryType: total},
	})
	cum := response.Results[0].Datapoints
	totalQueriesPreStall := cum[len(cum)-1].Value - cum[0].Value
	t.L().PrintfCtx(ctx, "%.2f queries completed before stall", totalQueriesPreStall)

	t.Status("inducing write stall")
	if doStall {
		m.ExpectDeath()
	}
	s.Stall(ctx, c.Node(1))
	// NB: We use a background context in the defer'ed unstall command,
	// otherwise on test failure our c.Run calls will be ignored. Leaving
	// the disk stalled will prevent artifact collection, making debugging
	// difficult.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.Unstall(ctx, c.Node(1))
	}()

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
		t.L().PrintfCtx(ctx, "pinging n1's connection: %s", err)
		if doStall && err == nil {
			t.Fatal("connection to n1 is still alive")
		} else if !doStall && err != nil {
			t.Fatalf("connection to n1 is dead: %s", err)
		}
	}

	// Let the workload continue after the stall.
	workloadAfterDur := 10*time.Minute - timeutil.Since(workloadStartAt)
	t.Status("letting workload continue for ", workloadAfterDur, " with n1 stalled")
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(workloadAfterDur):
	}

	{
		now := timeutil.Now()
		response := mustGetMetrics(ctx, c, t, adminURL, workloadStartAt, now, []tsQuery{
			{name: "cr.node.sql.query.count", queryType: total},
		})
		cum := response.Results[0].Datapoints
		totalQueriesPostStall := cum[len(cum)-1].Value - totalQueriesPreStall
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
	s.Unstall(ctx, c.Node(1))
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

	// Shut down the nodes, allowing any devices to be unmounted during cleanup.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Range(1, 3))
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

type diskStaller interface {
	Setup(ctx context.Context)
	Cleanup(ctx context.Context)
	Stall(ctx context.Context, nodes option.NodeListOption)
	Unstall(ctx context.Context, nodes option.NodeListOption)
	DataDir() string
	LogDir() string
}

type dmsetupDiskStaller struct {
	t test.Test
	c cluster.Cluster
	// If logsToo=true the logs directory will be updated to be a symlink
	// pointing into the store directory.
	logsToo bool
}

var _ diskStaller = (*dmsetupDiskStaller)(nil)

func (s *dmsetupDiskStaller) device() string { return getDevice(s.t, s.c) }

func (s *dmsetupDiskStaller) Setup(ctx context.Context) {
	dev := s.device()
	// snapd will run "snapd auto-import /dev/dm-0" via udev triggers when
	// /dev/dm-0 is created. This possibly interferes with the dmsetup create
	// reload, so uninstall snapd.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo apt-get purge -y snapd`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo umount -f /mnt/data1 || true`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup remove_all`)
	err := s.c.RunE(ctx, option.WithNodes(s.c.All()), `echo "0 $(sudo blockdev --getsz `+dev+`) linear `+dev+` 0" | `+
		`sudo dmsetup create data1`)
	if err != nil {
		// This has occasionally been seen to fail with "Device or resource busy",
		// with no clear explanation. Try to find out who it is.
		s.c.Run(ctx, option.WithNodes(s.c.All()), "sudo bash -c 'ps aux; dmsetup status; mount; lsof'")
		s.t.Fatal(err)
	}
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo mount /dev/mapper/data1 /mnt/data1`)

	if s.logsToo {
		s.c.Run(ctx, option.WithNodes(s.c.All()), "mkdir -p {store-dir}/logs")
		s.c.Run(ctx, option.WithNodes(s.c.All()), "rm -f logs && ln -s {store-dir}/logs logs || true")
	}
}

func (s *dmsetupDiskStaller) Cleanup(ctx context.Context) {
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup resume data1`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo umount /mnt/data1`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup remove_all`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo mount /mnt/data1`)
	// Reinstall snapd in case subsequent tests need it.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo apt-get install -y snapd`)
}

func (s *dmsetupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, option.WithNodes(nodes), `sudo dmsetup suspend --noflush --nolockfs data1`)
}

func (s *dmsetupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, option.WithNodes(nodes), `sudo dmsetup resume data1`)
}

func (s *dmsetupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *dmsetupDiskStaller) LogDir() string  { return "logs" }

type cgroupDiskStaller struct {
	t           test.Test
	c           cluster.Cluster
	readOrWrite []bandwidthReadWrite
	logsToo     bool
}

var _ diskStaller = (*cgroupDiskStaller)(nil)

func (s *cgroupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *cgroupDiskStaller) LogDir() string {
	return "logs"
}
func (s *cgroupDiskStaller) Setup(ctx context.Context) {
	if s.logsToo {
		s.c.Run(ctx, option.WithNodes(s.c.All()), "mkdir -p {store-dir}/logs")
		s.c.Run(ctx, option.WithNodes(s.c.All()), "rm -f logs && ln -s {store-dir}/logs logs || true")
	}
}
func (s *cgroupDiskStaller) Cleanup(ctx context.Context) {}

func (s *cgroupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	// Shuffle the order of read and write stall initiation.
	rand.Shuffle(len(s.readOrWrite), func(i, j int) {
		s.readOrWrite[i], s.readOrWrite[j] = s.readOrWrite[j], s.readOrWrite[i]
	})
	for _, rw := range s.readOrWrite {
		// NB: I don't understand why, but attempting to set a
		// bytesPerSecond={0,1} results in Invalid argument from the io.max
		// cgroupv2 API.
		if err := s.setThroughput(ctx, nodes, rw, throughput{limited: true, bytesPerSecond: 4}); err != nil {
			s.t.Fatal(err)
		}
	}
}

func (s *cgroupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	for _, rw := range s.readOrWrite {
		err := s.setThroughput(ctx, nodes, rw, throughput{limited: false})
		s.t.L().PrintfCtx(ctx, "error unstalling the disk; stumbling on: %v", err)
		// NB: We log the error and continue on because unstalling may not
		// succeed if the process has successfully exited.
	}
}

func (s *cgroupDiskStaller) device() (major, minor int) {
	// TODO(jackson): Programmatically determine the device major,minor numbers.
	// eg,:
	//    deviceName := getDevice(s.t, s.c)
	//    `cat /proc/partitions` and find `deviceName`
	switch s.c.Cloud() {
	case spec.GCE:
		// ls -l /dev/sdb
		// brw-rw---- 1 root disk 8, 16 Mar 27 22:08 /dev/sdb
		return 8, 16
	default:
		s.t.Fatalf("unsupported cloud %q", s.c.Cloud())
		return 0, 0
	}
}

type throughput struct {
	limited        bool
	bytesPerSecond int
}

type bandwidthReadWrite int8

const (
	readBandwidth bandwidthReadWrite = iota
	writeBandwidth
)

func (rw bandwidthReadWrite) cgroupV2BandwidthProp() string {
	switch rw {
	case readBandwidth:
		return "rbps"
	case writeBandwidth:
		return "wbps"
	default:
		panic("unreachable")
	}
}

func (s *cgroupDiskStaller) setThroughput(
	ctx context.Context, nodes option.NodeListOption, rw bandwidthReadWrite, bw throughput,
) error {
	maj, min := s.device()
	cockroachIOController := filepath.Join("/sys/fs/cgroup/system.slice", roachtestutil.SystemInterfaceSystemdUnitName()+".service", "io.max")

	bytesPerSecondStr := "max"
	if bw.limited {
		bytesPerSecondStr = fmt.Sprintf("%d", bw.bytesPerSecond)
	}
	return s.c.RunE(ctx, option.WithNodes(nodes), "sudo", "/bin/bash", "-c", fmt.Sprintf(
		`'echo %d:%d %s=%s > %s'`,
		maj,
		min,
		rw.cgroupV2BandwidthProp(),
		bytesPerSecondStr,
		cockroachIOController,
	))
}

func getDevice(t test.Test, c cluster.Cluster) string {
	s := c.Spec()
	switch c.Cloud() {
	case spec.GCE:
		switch s.LocalSSD {
		case spec.LocalSSDDisable:
			return "/dev/sdb"
		case spec.LocalSSDPreferOn, spec.LocalSSDDefault:
			// TODO(jackson): These spec values don't guarantee that we are actually
			// using local SSDs, just that we might've.
			return "/dev/nvme0n1"
		default:
			t.Fatalf("unsupported LocalSSD enum %v", s.LocalSSD)
			return ""
		}
	case spec.AWS:
		return "/dev/nvme1n1"
	default:
		t.Fatalf("unsupported cloud %q", c.Cloud())
		return ""
	}
}
