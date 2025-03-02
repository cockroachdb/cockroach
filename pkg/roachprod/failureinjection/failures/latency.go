// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"time"
)

// NetworkLatency is a failure mode that injects artificial network latency between nodes
// using tc and iptables.
type NetworkLatency struct {
	GenericFailure
	classesInUse []int
}

// ArtificialLatency represents the one way artificial (added) latency from one group of nodes to another.
type ArtificialLatency struct {
	Source      install.Nodes
	Destination install.Nodes
	Delay       time.Duration
}

type NetworkLatencyArgs struct {
	ArtificialLatencies []ArtificialLatency
}

func registerNetworkLatencyFailure(r *FailureRegistry) {
	r.add(NetworkLatencyName, NetworkLatencyArgs{}, MakeNetworkLatencyFailure)
}

func MakeNetworkLatencyFailure(
	clusterName string, l *logger.Logger, secure bool,
) (FailureMode, error) {
	c, err := roachprod.GetClusterFromCache(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}

	genericFailure := GenericFailure{c: c, runTitle: "latency"}
	return &NetworkLatency{GenericFailure: genericFailure, classesInUse: make([]int, 0)}, nil
}

const NetworkLatencyName = "network-latency"

func (f *NetworkLatency) Description() string {
	return NetworkLatencyName
}

func (f *NetworkLatency) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	_ = f.Cleanup(ctx, l, args)
	interfaces, err := f.NetworkInterfaces(ctx, l)
	if err != nil {
		return err
	}
	var cmd string
	for _, iface := range interfaces {
		cmd += fmt.Sprintf(setupQdiscsCmd, iface)

	}
	l.Printf("Setting up disk devices with cmd: %s", cmd)
	if err := f.Run(ctx, l, f.c.Nodes, cmd); err != nil {
		return err
	}
	return nil
}

const (
	setupQdiscsCmd = `
set -e
sudo tc qdisc replace dev %[1]s root handle 1: htb default 12	
`
	addFilterCmd = `
sudo tc class add dev %[1]s parent 1: classid 1:%[2]d htb rate 10000mbit
sudo tc qdisc add dev %[1]s parent 1:%[2]d handle %[3]d: netem delay %[4]s
sudo tc filter add dev %[1]s parent 1: protocol ip u32 \
match ip dst {ip:%[5]d}/32 \
match ip dport {pgport:%[5]d} 0xffff \
flowid 1:%[2]d
sudo tc filter add dev %[1]s parent 1: protocol ip u32 \
match ip dst {ip:%[5]d:public}/32 \
match ip dport {pgport:%[5]d} 0xffff \
flowid 1:%[2]d
`
	removeFilterCmd = `
sudo tc qdisc del dev %s root
`
)

func (f *NetworkLatency) constructAddNetworkLatencyCmd(targetNode install.Node, interfaces []string, addedLatency time.Duration) string {
	class := 1
	if len(f.classesInUse) > 0 {
		class += f.classesInUse[len(f.classesInUse)-1]
	}
	handle := 10 * class
	f.classesInUse = append(f.classesInUse, class)
	var filters string
	for _, iface := range interfaces {
		filters += fmt.Sprintf(addFilterCmd, iface, class, handle, addedLatency, targetNode)
	}

	return filters
}

// TODO: make this remove only the ip rules added before
func constructRemoveNetworkLatencyCmd(targetNode install.Node, interfaces []string) string {
	var filters string
	for _, iface := range interfaces {
		filters = filters + fmt.Sprintf(removeFilterCmd, iface)
	}

	return filters
}

func (f *NetworkLatency) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	latencies := args.(NetworkLatencyArgs).ArtificialLatencies
	for _, latency := range latencies {
		for _, dest := range latency.Destination {
			interfaces, err := f.NetworkInterfaces(ctx, l)
			if err != nil {
				return err
			}
			cmd := f.constructAddNetworkLatencyCmd(dest, interfaces, latency.Delay)
			l.Printf("Adding artificial latency from nodes %d to node %d with cmd: %s", latency.Source, dest, cmd)
			if err := f.Run(ctx, l, latency.Source, cmd); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *NetworkLatency) Restore(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	//latencies := args.(NetworkLatencyArgs).ArtificialLatencies
	//for _, latency := range latencies {
	//	for _, dest := range latency.Destination {
	//		interfaces, err := f.NetworkInterfaces(ctx, l)
	//		if err != nil {
	//			return err
	//		}
	//		cmd := constructRemoveNetworkLatencyCmd(dest, interfaces)
	//		l.Printf("Removing artificial latency from nodes %d to node %d with cmd: %s", latency.Source, dest, cmd)
	//		if err := f.Run(ctx, l, latency.Source, cmd); err != nil {
	//			return err
	//		}
	//	}
	//}
	//return nil
	return f.Cleanup(ctx, l, args)
}

func (f *NetworkLatency) Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	interfaces, err := f.NetworkInterfaces(ctx, l)
	if err != nil {
		return err
	}
	var cmd string
	for _, iface := range interfaces {
		cmd += fmt.Sprintf(removeFilterCmd, iface)

	}
	l.Printf("Removing all tc filters with: %s", cmd)
	if err := f.Run(ctx, l, f.c.Nodes, cmd); err != nil {
		l.Printf("WARN: removing tc filter failed: %v", err)
	}
	f.classesInUse = []int{}
	return nil
}

func (f *NetworkLatency) WaitForFailureToPropagate(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	// TODO
	return nil
}

func (f *NetworkLatency) WaitForFailureToRestore(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	// TODO
	return nil
}
