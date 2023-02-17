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
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const maxSyncDur = 10 * time.Second

// registerDiskStalledDetection registers the disk stall test.
func registerDiskStalledDetection(r registry.Registry) {
	stallers := map[string]func(test.Test, cluster.Cluster) diskStaller{
		"dmsetup": func(t test.Test, c cluster.Cluster) diskStaller { return &dmsetupDiskStaller{t: t, c: c} },
		"cgroup/read-write/logs-too=false": func(t test.Test, c cluster.Cluster) diskStaller {
			return &cgroupDiskStaller{t: t, c: c, readOrWrite: []string{"write", "read"}}
		},
		"cgroup/read-write/logs-too=true": func(t test.Test, c cluster.Cluster) diskStaller {
			return &cgroupDiskStaller{t: t, c: c, readOrWrite: []string{"write", "read"}, logsToo: true}
		},
		"cgroup/write-only/logs-too=true": func(t test.Test, c cluster.Cluster) diskStaller {
			return &cgroupDiskStaller{t: t, c: c, readOrWrite: []string{"write"}, logsToo: true}
		},
	}
	for name, makeStaller := range stallers {
		name, makeStaller := name, makeStaller
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("disk-stalled/%s", name),
			Owner:   registry.OwnerStorage,
			Cluster: r.MakeClusterSpec(4, spec.ReuseNone()),
			Timeout: 20 * time.Minute,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDiskStalledDetection(ctx, t, c, makeStaller(t, c), true /* doStall */)
			},
			// Encryption is implemented within the virtual filesystem layer,
			// just like disk-health monitoring. It's important to exercise
			// encryption-at-rest to ensure there is not unmonitored I/O within
			// the encryption-at-rest implementation that could indefinitely
			// stall the process during a disk stall.
			EncryptionSupport: registry.EncryptionMetamorphic,
		})
	}

	for _, stallLogDir := range []bool{false, true} {
		for _, stallDataDir := range []bool{false, true} {
			// Grab copies of the args because we'll pass them into a closure.
			// Everyone's favorite bug to write in Go.
			stallLogDir := stallLogDir
			stallDataDir := stallDataDir
			r.Add(registry.TestSpec{
				Name: fmt.Sprintf(
					"disk-stalled/fuse/log=%t,data=%t",
					stallLogDir, stallDataDir,
				),
				Owner:   registry.OwnerStorage,
				Cluster: r.MakeClusterSpec(4, spec.ReuseNone()),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runDiskStalledDetection(ctx, t, c, &fuseDiskStaller{
						t:         t,
						c:         c,
						stallLogs: stallLogDir,
						stallData: stallDataDir,
					}, stallLogDir || stallDataDir /* doStall */)
				},
				EncryptionSupport: registry.EncryptionMetamorphic,
			})
		}
	}
}

func runDiskStalledDetection(
	ctx context.Context, t test.Test, c cluster.Cluster, s diskStaller, doStall bool,
) {
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
	c.Put(ctx, t.Cockroach(), "./cockroach")
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

	c.Run(ctx, c.Node(4), `./cockroach workload init kv --splits 1000 {pgurl:1}`)
	// Open SQL connections—one to n1, the node that will be stalled, and one to
	// n2 that should remain open and active for the remainder.
	n1Conn := c.Conn(ctx, t.L(), 1)
	defer n1Conn.Close()
	n2conn := c.Conn(ctx, t.L(), 2)
	defer n2conn.Close()
	require.NoError(t, n1Conn.PingContext(ctx))
	_, err = n2conn.ExecContext(ctx, `USE kv;`)
	require.NoError(t, err)

	// Wait for upreplication.
	require.NoError(t, WaitFor3XReplication(ctx, t, n2conn))

	t.Status("starting workload")
	workloadStartAt := timeutil.Now()
	m := c.NewMonitor(ctx, c.Range(1, 3))
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(4), `./cockroach workload run kv --read-percent 50 `+
			`--duration 10m --concurrency 256 --max-rate 2048 --tolerate-errors `+
			` --min-block-bytes=512 --max-block-bytes=512 `+
			`{pgurl:1-3}`)
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
	response := mustGetMetrics(t, adminURL, workloadStartAt, stallAt, []tsQuery{
		{name: "cr.node.txn.commits", queryType: total},
	})
	cum := response.Results[0].Datapoints
	totalTxnsPreStall := cum[len(cum)-1].Value - cum[0].Value
	t.L().PrintfCtx(ctx, "%.2f transactions completed before stall", totalTxnsPreStall)

	t.Status("inducing write stall")
	if doStall {
		m.ExpectDeath()
	}
	s.Stall(ctx, c.Node(1))
	defer s.Unstall(ctx, c.Node(1))

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
	{
		workloadPauseDur := 10*time.Minute - timeutil.Since(workloadStartAt)
		t.Status("letting workload continue for ", workloadPauseDur, " with n1 stalled")
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(workloadPauseDur):
		}
	}

	{
		now := timeutil.Now()
		response := mustGetMetrics(t, adminURL, workloadStartAt, now, []tsQuery{
			{name: "cr.node.txn.commits", queryType: total},
		})
		cum := response.Results[0].Datapoints
		totalTxnsPostStall := cum[len(cum)-1].Value - totalTxnsPreStall
		preStallTPS := totalTxnsPreStall / stallAt.Sub(workloadStartAt).Seconds()
		postStallTPS := totalTxnsPostStall / now.Sub(stallAt).Seconds()
		t.L().PrintfCtx(ctx, "%.2f total transactions committed after stall\n", totalTxnsPostStall)
		t.L().PrintfCtx(ctx, "pre-stall tps: %.2f, post-stall tps: %.2f\n", preStallTPS, postStallTPS)
		if postStallTPS < preStallTPS/2 {
			t.Fatalf("post-stall TPS %.2f is less than 50%% of pre-stall TPS %.2f", postStallTPS, preStallTPS)
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
	details, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(nodeID),
		"systemctl show cockroach.service --property="+prop)
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
}

var _ diskStaller = (*dmsetupDiskStaller)(nil)

func (s *dmsetupDiskStaller) device() string { return getDevice(s.t, s.c.Spec()) }

func (s *dmsetupDiskStaller) Setup(ctx context.Context) {
	dev := s.device()
	s.c.Run(ctx, s.c.All(), `sudo umount -f /mnt/data1 || true`)
	s.c.Run(ctx, s.c.All(), `sudo dmsetup remove_all`)
	s.c.Run(ctx, s.c.All(), `echo "0 $(sudo blockdev --getsz `+dev+`) linear `+dev+` 0" | `+
		`sudo dmsetup create data1`)
	s.c.Run(ctx, s.c.All(), `sudo mount /dev/mapper/data1 /mnt/data1`)
}

func (s *dmsetupDiskStaller) Cleanup(ctx context.Context) {
	s.c.Run(ctx, s.c.All(), `sudo umount /mnt/data1`)
	s.c.Run(ctx, s.c.All(), `sudo dmsetup remove_all`)
	s.c.Run(ctx, s.c.All(), `sudo mount /mnt/data1`)
}

func (s *dmsetupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, nodes, `sudo dmsetup suspend --noflush --nolockfs data1`)
}

func (s *dmsetupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, nodes, `sudo dmsetup resume data1`)
}

func (s *dmsetupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *dmsetupDiskStaller) LogDir() string  { return "logs" }

type cgroupDiskStaller struct {
	t           test.Test
	c           cluster.Cluster
	readOrWrite []string
	logsToo     bool
}

var _ diskStaller = (*cgroupDiskStaller)(nil)

func (s *cgroupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *cgroupDiskStaller) LogDir() string {
	return "logs"
}
func (s *cgroupDiskStaller) Setup(ctx context.Context) {
	if s.logsToo {
		s.c.Run(ctx, s.c.All(), "mkdir -p {store-dir}/logs")
		s.c.Run(ctx, s.c.All(), "rm -f logs && ln -s {store-dir}/logs logs || true")
	}
}
func (s *cgroupDiskStaller) Cleanup(ctx context.Context) {}

func (s *cgroupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	// Shuffle the order of read and write stall initiation.
	rand.Shuffle(len(s.readOrWrite), func(i, j int) {
		s.readOrWrite[i], s.readOrWrite[j] = s.readOrWrite[j], s.readOrWrite[i]
	})
	for _, rw := range s.readOrWrite {
		s.setThroughput(ctx, nodes, rw, 1)
	}
}

func (s *cgroupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	for _, rw := range s.readOrWrite {
		s.setThroughput(ctx, nodes, rw, 0)
	}
}

func (s *cgroupDiskStaller) device() (major, minor int) {
	// TODO(jackson): Programmatically determine the device major,minor numbers.
	// eg,:
	//    deviceName := getDevice(s.t, s.c.Spec())
	//    `cat /proc/partitions` and find `deviceName`
	switch s.c.Spec().Cloud {
	case spec.GCE:
		// ls -l /dev/nvme0n1
		// brw-rw---- 1 root disk 259, 0 Jan 26 20:05 /dev/nvme0n1
		return 259, 0
	default:
		s.t.Fatalf("unsupported cloud %q", s.c.Spec().Cloud)
		return 0, 0
	}
}

func (s *cgroupDiskStaller) setThroughput(
	ctx context.Context, nodes option.NodeListOption, readOrWrite string, bytesPerSecond int,
) {
	major, minor := s.device()
	s.c.Run(ctx, nodes, "sudo", "/bin/bash", "-c", fmt.Sprintf(
		"'echo %d:%d %d > /sys/fs/cgroup/blkio/blkio.throttle.%s_bps_device'",
		major,
		minor,
		bytesPerSecond,
		readOrWrite,
	))
}

// fuseDiskStaller uses a FUSE filesystem (charybdefs) to insert an artificial
// delay on all I/O.
type fuseDiskStaller struct {
	t         test.Test
	c         cluster.Cluster
	stallLogs bool
	stallData bool
}

var _ diskStaller = (*fuseDiskStaller)(nil)

func (s *fuseDiskStaller) DataDir() string {
	if s.stallData {
		return "{store-dir}/faulty"
	}
	return "{store-dir}/real"
}

func (s *fuseDiskStaller) LogDir() string {
	if s.stallLogs {
		return "{store-dir}/faulty/logs"
	}
	return "{store-dir}/real/logs"
}

func (s *fuseDiskStaller) Setup(ctx context.Context) {
	if s.c.IsLocal() && runtime.GOOS != "linux" {
		s.t.Fatalf("must run on linux os, found %s", runtime.GOOS)
	}
	s.t.Status("setting up charybdefs")
	require.NoError(s.t, s.c.Install(ctx, s.t.L(), s.c.All(), "charybdefs"))
	s.c.Run(ctx, s.c.All(), "sudo umount -f {store-dir}/faulty || true")
	s.c.Run(ctx, s.c.All(), "mkdir -p {store-dir}/{real,faulty} || true")
	s.c.Run(ctx, s.c.All(), "rm -f logs && ln -s {store-dir}/real/logs logs || true")
	s.c.Run(ctx, s.c.All(), "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real")
	s.c.Run(ctx, s.c.All(), "sudo mkdir -p {store-dir}/real/logs")
	s.c.Run(ctx, s.c.All(), "sudo chmod -R 777 {store-dir}/{real,faulty}")
}

func (s *fuseDiskStaller) Cleanup(ctx context.Context) {
	s.c.Run(ctx, s.c.All(), "sudo umount -f {store-dir}/faulty || true")
}

func (s *fuseDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	// Stall for 2x the max sync duration. The tool expects an integer
	// representing the delay time, in microseconds.
	stallMicros := (2 * maxSyncDur).Microseconds()
	s.c.Run(ctx, nodes, "charybdefs-nemesis", "--delay", strconv.FormatInt(stallMicros, 10))
}

func (s *fuseDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, nodes, "charybdefs-nemesis --clear")
}

func getDevice(t test.Test, s spec.ClusterSpec) string {
	switch s.Cloud {
	case spec.GCE:
		return "/dev/nvme0n1"
	case spec.AWS:
		return "/dev/nvme1n1"
	default:
		t.Fatalf("unsupported cloud %q", s.Cloud)
		return ""
	}
}
