// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestutil

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type DiskStaller interface {
	Setup(ctx context.Context)
	Cleanup(ctx context.Context)
	Stall(ctx context.Context, nodes option.NodeListOption)
	Unstall(ctx context.Context, nodes option.NodeListOption)
	DataDir() string
	LogDir() string
}

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
		s.f.Fatalf("unsupported cloud %q", s.c.Cloud())
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

func GetDiskDevice(f Fataler, c cluster.Cluster) string {
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
			f.Fatalf("unsupported LocalSSD enum %v", s.LocalSSD)
			return ""
		}
	case spec.AWS:
		return "/dev/nvme1n1"
	default:
		f.Fatalf("unsupported cloud %q", c.Cloud())
		return ""
	}
}
