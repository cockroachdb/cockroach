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
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const maxSyncDur = 10 * time.Second

// registerDiskStalledDetection registers the disk stall test.
func registerDiskStalledDetection(r registry.Registry) {
	stallers := map[string]func(test.Test, cluster.Cluster) diskStaller{
		"dmsetup": func(t test.Test, c cluster.Cluster) diskStaller { return &dmsetupDiskStaller{t: t, c: c} },
		"cgroup":  func(t test.Test, c cluster.Cluster) diskStaller { return &cgroupDiskStaller{t: t, c: c} },
	}
	for name, makeStaller := range stallers {
		name, makeStaller := name, makeStaller
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("disk-stalled/%s", name),
			Owner:   registry.OwnerStorage,
			Cluster: r.MakeClusterSpec(1),
			Timeout: 10 * time.Minute,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runDiskStalledDetection(ctx, t, c, makeStaller(t, c), true /* doStall */)
			},
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
				Cluster: r.MakeClusterSpec(1),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runDiskStalledDetection(ctx, t, c, &fuseDiskStaller{
						t:         t,
						c:         c,
						stallLogs: stallLogDir,
						stallData: stallDataDir,
					}, stallLogDir || stallDataDir /* doStall */)
				},
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

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), startOpts, startSettings, c.All())
	defer c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.All())
	if err != nil {
		t.Fatal(err)
	}
	uptime, err := getUptime(ctx, adminUIAddrs[0])
	if err != nil {
		t.Fatal(err)
	} else if uptime == 0 {
		t.Fatal("uptime is zero")
	}

	// TODO(jackson): Add a concurrent workload.

	// Wait between [1m,5m) before stalling the disk.
	pauseDur := time.Duration(rand.Intn(4))*time.Minute + time.Minute
	pauseBeforeStall := time.After(pauseDur)
	t.Status("pausing ", pauseDur, " before inducing write stall")
	select {
	case <-ctx.Done():
		t.Fatalf("context done before stall: %s", ctx.Err())
	case <-pauseBeforeStall:
	}

	t.Status("inducing write stall")
	stalledAt := timeutil.Now()
	s.Stall(ctx, c.All())
	defer s.Unstall(ctx, c.All())

	t.Status("pausing ", 2*maxSyncDur, " before checking for termination")
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(2 * maxSyncDur):
	}
	uptime, err = getUptime(ctx, adminUIAddrs[0])
	t.L().PrintfCtx(ctx, "node uptime is %s\n", uptime)
	if doStall && err == nil && timeutil.Now().Add(-uptime).Before(stalledAt) {
		t.Fatalf("node's uptime of %s indicates it's been up since before the stall at %s", uptime, stalledAt)
	} else if !doStall && err != nil {
		t.Fatalf("no stall induced; no err expected; got: %s", err)
	}
}

func getUptime(ctx context.Context, adminUIAddr string) (uptime time.Duration, err error) {
	var nodeDiagnostics struct {
		Node struct {
			Uptime string `json:"uptime"`
		} `json:"node"`
	}
	if err := retry.ForDuration(10*time.Second, func() error {
		resp, err := httputil.Get(ctx, "http://"+adminUIAddr+"/_status/diagnostics/local")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		return json.NewDecoder(resp.Body).Decode(&nodeDiagnostics)
	}); err != nil {
		return 0, err
	}
	u, err := strconv.Atoi(nodeDiagnostics.Node.Uptime)
	if err != nil {
		return 0, err
	}
	return time.Second * time.Duration(u), nil
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
	s.c.Run(ctx, s.c.All(), `sudo umount /mnt/data1`)
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
	t test.Test
	c cluster.Cluster
}

var _ diskStaller = (*cgroupDiskStaller)(nil)

func (s *cgroupDiskStaller) DataDir() string             { return "{store-dir}" }
func (s *cgroupDiskStaller) LogDir() string              { return "logs" }
func (s *cgroupDiskStaller) Setup(ctx context.Context)   {}
func (s *cgroupDiskStaller) Cleanup(ctx context.Context) {}

func (s *cgroupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	s.setThroughput(ctx, nodes, "write", 1)
}

func (s *cgroupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	s.setThroughput(ctx, nodes, "write", 0)
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
	if err := s.c.Install(ctx, s.t.L(), s.c.All(), "charybdefs"); err != nil {
		s.t.Fatal(err)
	}
	s.c.Run(ctx, s.c.All(), "sudo umount -f {store-dir}/faulty || true")
	s.c.Run(ctx, s.c.All(), "mkdir -p {store-dir}/{real,faulty} || true")
	s.c.Run(ctx, s.c.All(), "rm -f logs && ln -s {store-dir}/real/logs logs || true")
	s.c.Run(ctx, s.c.All(), "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real")
	s.c.Run(ctx, s.c.All(), "sudo mkdir -p {store-dir}/real/logs")
	s.c.Run(ctx, s.c.All(), "sudo chmod -R 777 {store-dir}/{real,faulty}")
}

func (s *fuseDiskStaller) Cleanup(ctx context.Context) {
	s.c.Run(ctx, s.c.All(), "sudo umount {store-dir}/faulty")
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
