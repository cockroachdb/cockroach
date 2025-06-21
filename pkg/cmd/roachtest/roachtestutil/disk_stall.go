// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type DiskStaller interface {
	Setup(ctx context.Context)
	Cleanup(ctx context.Context)
	Stall(ctx context.Context, nodes option.NodeListOption)
	StallForDuration(ctx context.Context, nodes option.NodeListOption, duration time.Duration)
	Slow(ctx context.Context, nodes option.NodeListOption, bytesPerSecond int)
	Unstall(ctx context.Context, nodes option.NodeListOption)
	DataDir() string
	LogDir() string
}

type NoopDiskStaller struct{}

var _ DiskStaller = NoopDiskStaller{}

func (n NoopDiskStaller) Cleanup(ctx context.Context)                            {}
func (n NoopDiskStaller) DataDir() string                                        { return "{store-dir}" }
func (n NoopDiskStaller) LogDir() string                                         { return "logs" }
func (n NoopDiskStaller) Setup(ctx context.Context)                              {}
func (n NoopDiskStaller) Slow(_ context.Context, _ option.NodeListOption, _ int) {}
func (n NoopDiskStaller) Stall(_ context.Context, _ option.NodeListOption)       {}
func (n NoopDiskStaller) StallForDuration(
	_ context.Context, _ option.NodeListOption, _ time.Duration,
) {
}
func (n NoopDiskStaller) Unstall(_ context.Context, _ option.NodeListOption) {}

type Fataler interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	L() *logger.Logger
}

type cgroupDiskStaller struct {
	f           Fataler
	c           cluster.Cluster
	readOrWrite []bandwidthReadWrite
	logsToo     bool
}

var _ DiskStaller = (*cgroupDiskStaller)(nil)

func MakeCgroupDiskStaller(f Fataler, c cluster.Cluster, readsToo bool, logsToo bool) DiskStaller {
	bwRW := []bandwidthReadWrite{writeBandwidth}
	if readsToo {
		bwRW = append(bwRW, readBandwidth)
	}
	return &cgroupDiskStaller{f: f, c: c, readOrWrite: bwRW, logsToo: logsToo}
}

func (s *cgroupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *cgroupDiskStaller) LogDir() string {
	return "logs"
}
func (s *cgroupDiskStaller) Setup(ctx context.Context) {
	if _, ok := s.c.Spec().ReusePolicy.(spec.ReusePolicyNone); !ok {
		// Safety measure.
		s.f.Fatalf("cluster needs ReusePolicyNone to support disk stalls")
	}
	if s.logsToo {
		s.c.Run(ctx, option.WithNodes(s.c.All()), "mkdir -p {store-dir}/logs")
		s.c.Run(ctx, option.WithNodes(s.c.All()), "rm -f logs && ln -s {store-dir}/logs logs || true")
	}
}
func (s *cgroupDiskStaller) Cleanup(ctx context.Context) {}

func (s *cgroupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	// NB: I don't understand why, but attempting to set a bytesPerSecond={0,1}
	// results in Invalid argument from the io.max cgroupv2 API.
	s.Slow(ctx, nodes, 4)
}

func (s *cgroupDiskStaller) Slow(
	ctx context.Context, nodes option.NodeListOption, bytesPerSecond int,
) {
	// Shuffle the order of read and write stall initiation.
	rand.Shuffle(len(s.readOrWrite), func(i, j int) {
		s.readOrWrite[i], s.readOrWrite[j] = s.readOrWrite[j], s.readOrWrite[i]
	})
	for _, rw := range s.readOrWrite {
		if err := s.setThroughput(ctx, nodes, rw, throughput{limited: true, bytesPerSecond: bytesPerSecond}); err != nil {
			s.f.Fatal(err)
		}
	}
}

func (s *cgroupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	for _, rw := range s.readOrWrite {
		err := s.setThroughput(ctx, nodes, rw, throughput{limited: false})
		if err != nil {
			s.f.L().PrintfCtx(ctx, "error unstalling the disk; stumbling on: %v", err)
		}
		// NB: We log the error and continue on because unstalling may not
		// succeed if the process has successfully exited.
	}
}

// StallForDuration stalls the disk for the specified duration. This avoids creating
// multiple SSH connections in quick succession, which can cause flakiness.
func (s *cgroupDiskStaller) StallForDuration(
	ctx context.Context, nodes option.NodeListOption, duration time.Duration,
) {
	maj, min := s.device(nodes)
	cockroachIOController := filepath.Join("/sys/fs/cgroup/system.slice", SystemInterfaceSystemdUnitName()+".service", "io.max")

	// Shuffle the order of read and write stall initiation.
	rand.Shuffle(len(s.readOrWrite), func(i, j int) {
		s.readOrWrite[i], s.readOrWrite[j] = s.readOrWrite[j], s.readOrWrite[i]
	})

	// Prepare bandwidth limit strings for stall and unstall.
	var limits []string
	for _, rw := range s.readOrWrite {
		limits = append(limits, fmt.Sprintf("%s=4", rw.cgroupV2BandwidthProp()))
	}
	stallLimits := strings.Join(limits, " ")

	// Prepare max bandwidth strings for unstall.
	var maxLimits []string
	for _, rw := range s.readOrWrite {
		maxLimits = append(maxLimits, fmt.Sprintf("%s=max", rw.cgroupV2BandwidthProp()))
	}
	unstallLimits := strings.Join(maxLimits, " ")

	// Build the command to stall the disk for the specified duration and then unstall it.
	cmd := fmt.Sprintf(`'
echo %d:%d %s > %s
sleep %f
echo %d:%d %s > %s
'`,
		maj, min, stallLimits, cockroachIOController,
		duration.Seconds(),
		maj, min, unstallLimits, cockroachIOController)

	err := s.c.RunE(ctx, option.WithNodes(nodes), "sudo", "/bin/bash", "-c", cmd)
	if err != nil {
		s.f.L().PrintfCtx(ctx, "error in StallForDuration: %v", err)
	}
}

func (s *cgroupDiskStaller) device(nodes option.NodeListOption) (major, minor int) {
	// TODO(jackson): Programmatically determine the device major,minor numbers.
	// eg,:
	//    deviceName := getDevice(s.t, s.c)
	//    `cat /proc/partitions` and find `deviceName`
	res, err := s.c.RunWithDetailsSingleNode(context.TODO(), s.f.L(), option.WithNodes(nodes[:1]), "lsblk | grep /mnt/data1 | awk '{print $2}'")
	if err != nil {
		s.f.Fatalf("error when determining block device: %s", err)
		return 0, 0
	}
	parts := strings.Split(strings.TrimSpace(res.Stdout), ":")
	if len(parts) != 2 {
		s.f.Fatalf("unexpected output from lsblk: %s", res.Stdout)
		return 0, 0
	}
	major, err = strconv.Atoi(parts[0])
	if err != nil {
		s.f.Fatalf("error when determining block device: %s", err)
		return 0, 0
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		s.f.Fatalf("error when determining block device: %s", err)
		return 0, 0
	}
	return major, minor
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
	maj, min := s.device(nodes)
	cockroachIOController := filepath.Join("/sys/fs/cgroup/system.slice", SystemInterfaceSystemdUnitName()+".service", "io.max")

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

func GetDiskDevice(f Fataler, c cluster.Cluster, nodes option.NodeListOption) string {
	res, err := c.RunWithDetailsSingleNode(context.TODO(), f.L(), option.WithNodes(nodes[:1]), "lsblk | grep /mnt/data1 | awk '{print $1}'")
	if err != nil {
		f.Fatalf("error when determining block device: %s", err)
		return ""
	}
	return "/dev/" + strings.TrimSpace(res.Stdout)
}

type dmsetupDiskStaller struct {
	f Fataler
	c cluster.Cluster

	dev string // set in Setup; s.device() doesn't work when volume is not set up
}

var _ DiskStaller = (*dmsetupDiskStaller)(nil)

func (s *dmsetupDiskStaller) device(nodes option.NodeListOption) string {
	return GetDiskDevice(s.f, s.c, nodes)
}

func (s *dmsetupDiskStaller) Setup(ctx context.Context) {
	if _, ok := s.c.Spec().ReusePolicy.(spec.ReusePolicyNone); !ok {
		// We disable journaling and do all kinds of things below.
		s.f.Fatalf("cluster needs ReusePolicyNone to support disk stalls")
	}
	s.dev = s.device(s.c.All())
	// snapd will run "snapd auto-import /dev/dm-0" via udev triggers when
	// /dev/dm-0 is created. This possibly interferes with the dmsetup create
	// reload, so uninstall snapd.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo apt-get purge -y snapd`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo umount -f /mnt/data1 || true`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup remove_all`)
	// See https://github.com/cockroachdb/cockroach/issues/129619#issuecomment-2316147244.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo tune2fs -O ^has_journal `+s.dev)
	err := s.c.RunE(ctx, option.WithNodes(s.c.All()), `echo "0 $(sudo blockdev --getsz `+s.dev+`) linear `+s.dev+` 0" | `+
		`sudo dmsetup create data1`)
	if err != nil {
		// This has occasionally been seen to fail with "Device or resource busy",
		// with no clear explanation. Try to find out who it is.
		s.c.Run(ctx, option.WithNodes(s.c.All()), "sudo bash -c 'ps aux; dmsetup status; mount; lsof'")
		s.f.Fatal(err)
	}
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo mount /dev/mapper/data1 /mnt/data1`)
}

func (s *dmsetupDiskStaller) Cleanup(ctx context.Context) {
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup resume data1`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo umount /mnt/data1`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo dmsetup remove_all`)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo tune2fs -O has_journal `+s.dev)
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo mount /mnt/data1`)
	// Reinstall snapd in case subsequent tests need it.
	s.c.Run(ctx, option.WithNodes(s.c.All()), `sudo apt-get install -y snapd`)
}

func (s *dmsetupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, option.WithNodes(nodes), `sudo dmsetup suspend --noflush --nolockfs data1`)
}

func (s *dmsetupDiskStaller) Slow(
	ctx context.Context, nodes option.NodeListOption, bytesPerSecond int,
) {
	// TODO(baptist): Consider https://github.com/kawamuray/ddi.
	s.f.Fatal("Slow is not supported for dmsetupDiskStaller")
}

func (s *dmsetupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	s.c.Run(ctx, option.WithNodes(nodes), `sudo dmsetup resume data1`)
}

// StallForDuration stalls the disk for the specified duration. This avoids creating
// multiple SSH connections in quick succession, which can cause flakiness.
func (s *dmsetupDiskStaller) StallForDuration(
	ctx context.Context, nodes option.NodeListOption, duration time.Duration,
) {
	cmd := fmt.Sprintf(`'
sudo dmsetup suspend --noflush --nolockfs data1
sleep %f
sudo dmsetup resume data1
'`, duration.Seconds())

	err := s.c.RunE(ctx, option.WithNodes(nodes), "sudo", "/bin/bash", "-c", cmd)
	if err != nil {
		s.f.L().PrintfCtx(ctx, "error in StallForDuration: %v", err)
	}
}

func (s *dmsetupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *dmsetupDiskStaller) LogDir() string  { return "logs" }

func MakeDmsetupDiskStaller(f Fataler, c cluster.Cluster) DiskStaller {
	return &dmsetupDiskStaller{f: f, c: c}
}
