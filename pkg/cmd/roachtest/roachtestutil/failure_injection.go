// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// MakeNetworkPartitionFailer creates a Failer for network partition injection.
// Returns the failer and pre-configured args ready for Setup/Inject.
func MakeNetworkPartitionFailer(
	l *logger.Logger,
	c cluster.Cluster,
	source, dest option.NodeListOption,
	partitionType failures.PartitionType,
) (*failures.Failer, failures.NetworkPartitionArgs, error) {
	failer, err := c.GetFailer(l, c.CRDBNodes(), failures.IPTablesNetworkPartitionName, false)
	if err != nil {
		return nil, failures.NetworkPartitionArgs{}, err
	}
	args := failures.NetworkPartitionArgs{
		Partitions: []failures.NetworkPartition{{
			Source:      source.InstallNodes(),
			Destination: dest.InstallNodes(),
			Type:        partitionType,
		}},
	}
	return failer, args, nil
}

// MakeBidirectionalPartitionFailer creates a Failer for bidirectional network
// partition injection between source and destination nodes.
func MakeBidirectionalPartitionFailer(
	l *logger.Logger, c cluster.Cluster, source, dest option.NodeListOption,
) (*failures.Failer, failures.NetworkPartitionArgs, error) {
	return MakeNetworkPartitionFailer(l, c, source, dest, failures.Bidirectional)
}

// MakeIncomingPartitionFailer creates a Failer that drops incoming traffic
// on source nodes from destination nodes.
func MakeIncomingPartitionFailer(
	l *logger.Logger, c cluster.Cluster, source, dest option.NodeListOption,
) (*failures.Failer, failures.NetworkPartitionArgs, error) {
	return MakeNetworkPartitionFailer(l, c, source, dest, failures.Incoming)
}

// MakeOutgoingPartitionFailer creates a Failer that drops outgoing traffic
// from source nodes to destination nodes.
func MakeOutgoingPartitionFailer(
	l *logger.Logger, c cluster.Cluster, source, dest option.NodeListOption,
) (*failures.Failer, failures.NetworkPartitionArgs, error) {
	return MakeNetworkPartitionFailer(l, c, source, dest, failures.Outgoing)
}

// MakeNetworkLatencyFailer creates a Failer for network latency injection.
// The delay is applied to traffic from source to destination nodes.
func MakeNetworkLatencyFailer(
	l *logger.Logger, c cluster.Cluster, source, dest option.NodeListOption, delay time.Duration,
) (*failures.Failer, failures.NetworkLatencyArgs, error) {
	failer, err := c.GetFailer(l, c.All(), failures.NetworkLatencyName, false)
	if err != nil {
		return nil, failures.NetworkLatencyArgs{}, err
	}
	args := failures.NetworkLatencyArgs{
		ArtificialLatencies: []failures.ArtificialLatency{{
			Source:      source.InstallNodes(),
			Destination: dest.InstallNodes(),
			Delay:       delay,
		}},
	}
	return failer, args, nil
}

// MakeProcessKillFailer creates a Failer for process kill injection.
// If graceful is true, sends SIGTERM and allows the process to drain before
// killing with SIGKILL after gracePeriod. If graceful is false, sends SIGKILL
// immediately and gracePeriod is ignored.
func MakeProcessKillFailer(
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	graceful bool,
	gracePeriod time.Duration,
) (*failures.Failer, failures.ProcessKillArgs, error) {
	failer, err := c.GetFailer(l, c.CRDBNodes(), failures.ProcessKillFailureName, false)
	if err != nil {
		return nil, failures.ProcessKillArgs{}, err
	}
	args := failures.ProcessKillArgs{
		Nodes:            nodes.InstallNodes(),
		GracefulShutdown: graceful,
		GracePeriod:      gracePeriod,
	}
	return failer, args, nil
}

// MakeVMResetFailer creates a Failer for VM reset injection.
// If stopProcesses is true, the cockroach processes are stopped before resetting the VM.
func MakeVMResetFailer(
	l *logger.Logger, c cluster.Cluster, nodes option.NodeListOption, stopProcesses bool,
) (*failures.Failer, failures.ResetVMArgs, error) {
	failer, err := c.GetFailer(l, c.CRDBNodes(), failures.ResetVMFailureName, false)
	if err != nil {
		return nil, failures.ResetVMArgs{}, err
	}
	args := failures.ResetVMArgs{
		Nodes:         nodes.InstallNodes(),
		StopProcesses: stopProcesses,
	}
	return failer, args, nil
}

// MakeCgroupDiskStallFailer creates a Failer for cgroup disk stall injection.
// This uses cgroups v2 to throttle I/O to extremely low values.
func MakeCgroupDiskStallFailer(
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	stallWrites, stallReads, stallLogs bool,
) (*failures.Failer, failures.DiskStallArgs, error) {
	failer, err := c.GetFailer(l, c.CRDBNodes(), failures.CgroupsDiskStallName, false)
	if err != nil {
		return nil, failures.DiskStallArgs{}, err
	}
	args := failures.DiskStallArgs{
		StallLogs:    stallLogs,
		StallReads:   stallReads,
		StallWrites:  stallWrites,
		Nodes:        nodes.InstallNodes(),
		RestartNodes: true,
	}
	return failer, args, nil
}

// MakeDmsetupDiskStallFailer creates a Failer for dmsetup disk stall injection.
// This uses dmsetup to completely suspend I/O to the data disk.
//
// Note: MakeDmsetupDiskStaller in disk_stall.go wraps this function to provide
// the legacy DiskStaller interface for backward compatibility with existing tests.
func MakeDmsetupDiskStallFailer(
	l *logger.Logger, c cluster.Cluster, nodes option.NodeListOption, disableStateValidation bool,
) (*failures.Failer, failures.DiskStallArgs, error) {
	failer, err := c.GetFailer(l, c.CRDBNodes(), failures.DmsetupDiskStallName, disableStateValidation)
	if err != nil {
		return nil, failures.DiskStallArgs{}, err
	}
	args := failures.DiskStallArgs{
		Nodes:        nodes.InstallNodes(),
		StallWrites:  true,
		RestartNodes: true,
	}
	return failer, args, nil
}
