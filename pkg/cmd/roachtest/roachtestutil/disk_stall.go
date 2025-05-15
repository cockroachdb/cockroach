// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// TODO(darryl): Once the failure injection library is a first class citizen of roachtest,
// i.e. synced with the monitor, test + cluster spec validation, observability into failure
// modes, etc. we can remove this interface entirely.
type DiskStaller interface {
	Setup(ctx context.Context)
	Cleanup(ctx context.Context)
	Stall(ctx context.Context, nodes option.NodeListOption)
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
func (n NoopDiskStaller) Unstall(_ context.Context, _ option.NodeListOption)     {}

type Fataler interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	L() *logger.Logger
}

type cgroupDiskStaller struct {
	*failures.Failer
	f          Fataler
	c          cluster.Cluster
	stallReads bool
	stallLogs  bool
}

var _ DiskStaller = (*cgroupDiskStaller)(nil)

func MakeCgroupDiskStaller(
	f Fataler, c cluster.Cluster, stallReads bool, stallLogs bool,
) DiskStaller {
	diskStaller, err := c.GetFailer(f.L(), c.CRDBNodes(), failures.CgroupsDiskStallName)
	if err != nil {
		f.Fatalf("failed to get failer: %s", err)
	}
	return &cgroupDiskStaller{Failer: diskStaller, f: f, c: c, stallReads: stallReads, stallLogs: stallLogs}
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
	if err := s.Failer.Setup(ctx, s.f.L(), failures.DiskStallArgs{
		StallLogs: s.stallLogs,
		Nodes:     s.c.CRDBNodes().InstallNodes(),
	}); err != nil {
		s.f.Fatalf("failed to setup disk stall: %s", err)
	}
}
func (s *cgroupDiskStaller) Cleanup(ctx context.Context) {
	err := s.Failer.Cleanup(ctx, s.f.L())
	if err != nil {
		s.f.Fatalf("failed to cleanup disk stall: %s", err)
	}
}

func (s *cgroupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	if err := s.Failer.Inject(ctx, s.f.L(), failures.DiskStallArgs{
		StallLogs:   s.stallLogs,
		StallWrites: true,
		StallReads:  s.stallReads,
		Nodes:       nodes.InstallNodes(),
	}); err != nil {
		s.f.Fatalf("failed to stall disk: %s", err)
	}
}

func (s *cgroupDiskStaller) Slow(
	ctx context.Context, nodes option.NodeListOption, bytesPerSecond int,
) {
	if err := s.Failer.Inject(ctx, s.f.L(), failures.DiskStallArgs{
		StallLogs:   s.stallLogs,
		StallWrites: true,
		StallReads:  s.stallReads,
		Nodes:       nodes.InstallNodes(),
		Throughput:  bytesPerSecond,
	}); err != nil {
		s.f.Fatalf("failed to slow disk: %s", err)
	}
}

func (s *cgroupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	if err := s.Failer.Recover(ctx, s.f.L()); err != nil {
		s.f.Fatalf("failed to unstall disk: %s", err)
	}
}

type dmsetupDiskStaller struct {
	*failures.Failer
	f Fataler
	c cluster.Cluster
}

var _ DiskStaller = (*dmsetupDiskStaller)(nil)

func MakeDmsetupDiskStaller(f Fataler, c cluster.Cluster) DiskStaller {
	diskStaller, err := c.GetFailer(f.L(), c.CRDBNodes(), failures.DmsetupDiskStallName)
	if err != nil {
		f.Fatalf("failed to get failer: %s", err)
	}
	return &dmsetupDiskStaller{Failer: diskStaller, f: f, c: c}
}

func (s *dmsetupDiskStaller) Setup(ctx context.Context) {
	if _, ok := s.c.Spec().ReusePolicy.(spec.ReusePolicyNone); !ok {
		// We disable journaling and do all kinds of things below.
		s.f.Fatalf("cluster needs ReusePolicyNone to support disk stalls")
	}
	if err := s.Failer.Setup(ctx, s.f.L(), failures.DiskStallArgs{Nodes: s.c.CRDBNodes().InstallNodes()}); err != nil {
		s.f.Fatalf("failed to setup disk stall: %s", err)
	}
}

func (s *dmsetupDiskStaller) Cleanup(ctx context.Context) {
	if err := s.Failer.Cleanup(ctx, s.f.L()); err != nil {
		s.f.Fatalf("failed to cleanup disk stall: %s", err)
	}
}

func (s *dmsetupDiskStaller) Stall(ctx context.Context, nodes option.NodeListOption) {
	if err := s.Failer.Inject(ctx, s.f.L(), failures.DiskStallArgs{
		Nodes: nodes.InstallNodes(),
	}); err != nil {
		s.f.Fatalf("failed to stall disk: %s", err)
	}
}

func (s *dmsetupDiskStaller) Slow(
	ctx context.Context, nodes option.NodeListOption, bytesPerSecond int,
) {
	// TODO(baptist): Consider https://github.com/kawamuray/ddi.
	s.f.Fatal("Slow is not supported for dmsetupDiskStaller")
}

func (s *dmsetupDiskStaller) Unstall(ctx context.Context, nodes option.NodeListOption) {
	if err := s.Failer.Recover(ctx, s.f.L()); err != nil {
		s.f.Fatalf("failed to unstall disk: %s", err)
	}
}

func (s *dmsetupDiskStaller) DataDir() string { return "{store-dir}" }
func (s *dmsetupDiskStaller) LogDir() string  { return "logs" }
