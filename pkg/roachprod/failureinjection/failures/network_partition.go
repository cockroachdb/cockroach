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
)

type PartitionType int

const (
	// Bidirectional drops traffic in both directions.
	Bidirectional PartitionType = iota
	// Incoming drops incoming traffic on the source from the destination
	Incoming
	// Outgoing drops outgoing traffic from the source to the destination
	Outgoing
)

type NetworkPartition struct {
	// Source is a list of nodes that will have the network partition created as
	// described by Type.
	Source      install.Nodes
	Destination install.Nodes
	// Type describes the network partition being created.
	Type PartitionType
}
type NetworkPartitionArgs struct {
	// List of network partitions to create.
	//
	// TODO: Add support for more complex network partitions, e.g. "soft" partitions
	// where not all packets are dropped.
	Partitions []NetworkPartition

	// List of nodes to drop iptables rules for when restoring. If empty, all nodes
	// will have their rules dropped.
	NodesToRestore install.Nodes
}

type IPTablesPartitionFailure struct {
	GenericFailure
}

func MakeIPTablesPartitionFailure(
	clusterName string, l *logger.Logger, secure bool,
) (FailureMode, error) {
	c, err := roachprod.GetClusterFromCache(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}

	genericFailure := GenericFailure{c: c, runTitle: "iptables"}
	return &IPTablesPartitionFailure{genericFailure}, nil
}

const IPTablesNetworkPartitionName = "iptables-network-partition"

func registerIPTablesPartitionFailure(r *FailureRegistry) {
	r.add(IPTablesNetworkPartitionName, NetworkPartitionArgs{}, MakeIPTablesPartitionFailure)
}

func (f *IPTablesPartitionFailure) Description() string {
	return IPTablesNetworkPartitionName
}

func (f *IPTablesPartitionFailure) Setup(_ context.Context, _ *logger.Logger, _ FailureArgs) error {
	// iptables is already installed by default on Ubuntu.
	return nil
}

// When dropping both input and output, make sure we drop packets in both
// directions for both the inbound and outbound TCP connections, such that we
// get a proper black hole. Only dropping one direction for both of INPUT and
// OUTPUT will still let e.g. TCP retransmits through, which may affect the
// TCP stack behavior and is not representative of real network outages.
//
// For the asymmetric partitions, only drop packets in one direction since
// this is representative of accidental firewall rules we've seen cause such
// outages in the wild.
const (
	partitionTemplateWrapper = `
# ensure any failure fails the entire script.
set -e;
# Setting default filter policy
sudo iptables -P INPUT ACCEPT;
sudo iptables -P OUTPUT ACCEPT;
%s
sudo iptables-save
`

	// Drop all incoming and outgoing traffic to the ip address.
	bidirectionalPartitionCmd = `
sudo iptables %[1]s INPUT  -s {ip:%[2]d} -p tcp --dport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s OUTPUT -d {ip:%[2]d} -p tcp --dport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s INPUT  -s {ip:%[2]d} -p tcp --sport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s OUTPUT -d {ip:%[2]d} -p tcp --sport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s INPUT  -s {ip:%[2]d:public} -p tcp --dport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s OUTPUT -d {ip:%[2]d:public} -p tcp --dport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s INPUT  -s {ip:%[2]d:public} -p tcp --sport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s OUTPUT -d {ip:%[2]d:public} -p tcp --sport {pgport:%[2]d} -j DROP;
`
	// Drop all incoming traffic from the ip address.
	asymmetricInputPartitionCmd = `
sudo iptables %[1]s INPUT -s {ip:%[2]d} -p tcp --dport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s INPUT -s {ip:%[2]d:public} -p tcp --dport {pgport:%[2]d} -j DROP;
`

	// Drop all outgoing traffic to the ip address.
	asymmetricOutputPartitionCmd = `
sudo iptables %[1]s OUTPUT -d {ip:%[2]d} -p tcp --dport {pgport:%[2]d} -j DROP;
sudo iptables %[1]s OUTPUT -d {ip:%[2]d:public} -p tcp --dport {pgport:%[2]d} -j DROP;
`
)

func constructIPTablesRule(partitionCmd string, targetNode install.Node, addRule bool) string {
	addOrDropRule := "-D"
	if addRule {
		addOrDropRule = "-A"
	}
	return fmt.Sprintf(partitionTemplateWrapper, fmt.Sprintf(partitionCmd, addOrDropRule, targetNode))
}

func (f *IPTablesPartitionFailure) Inject(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	partitions := args.(NetworkPartitionArgs).Partitions
	for _, partition := range partitions {
		for _, destinationNode := range partition.Destination {
			var cmd string
			switch partition.Type {
			case Bidirectional:
				cmd = constructIPTablesRule(bidirectionalPartitionCmd, destinationNode, true /* addRule */)
				l.Printf("Dropping packets between nodes %d and node %d with cmd: %s", partition.Source, destinationNode, cmd)
			case Incoming:
				cmd = constructIPTablesRule(asymmetricInputPartitionCmd, destinationNode, true /* addRule */)
				l.Printf("Dropping packets from node %d to nodes %d with cmd: %s", destinationNode, partition.Source, cmd)
			case Outgoing:
				cmd = constructIPTablesRule(asymmetricOutputPartitionCmd, destinationNode, true /* addRule */)
				l.Printf("Dropping packets from nodes %d to node %d with cmd: %s", partition.Source, destinationNode, cmd)
			default:
				panic("unhandled default case")
			}
			if err := f.Run(ctx, l, partition.Source, cmd); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *IPTablesPartitionFailure) Restore(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	partitions := args.(NetworkPartitionArgs).Partitions
	for _, partition := range partitions {
		for _, destinationNode := range partition.Destination {
			var cmd string
			switch partition.Type {
			case Bidirectional:
				cmd = constructIPTablesRule(bidirectionalPartitionCmd, destinationNode, false /* addRule */)
				l.Printf("Resuming packets between nodes %d and node %d with cmd: %s", partition.Source, destinationNode, cmd)
			case Incoming:
				cmd = constructIPTablesRule(asymmetricInputPartitionCmd, destinationNode, false /* addRule */)
				l.Printf("Resuming packets from node %d to nodes %d with cmd: %s", destinationNode, partition.Source, cmd)
			case Outgoing:
				cmd = constructIPTablesRule(asymmetricOutputPartitionCmd, destinationNode, false /* addRule */)
				l.Printf("Resuming packets from nodes %d to node %d with cmd: %s", partition.Source, destinationNode, cmd)
			default:
				panic("unhandled default case")
			}
			if err := f.Run(ctx, l, partition.Source, cmd); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *IPTablesPartitionFailure) Cleanup(
	_ context.Context, _ *logger.Logger, _ FailureArgs,
) error {
	return nil
}

func (f *IPTablesPartitionFailure) WaitForFailureToPropagate(
	_ context.Context, _ *logger.Logger, _ FailureArgs,
) error {
	// IPTables rules are applied immediately, so we don't need to wait for them to propagate.
	return nil
}

func (f *IPTablesPartitionFailure) WaitForFailureToRestore(
	_ context.Context, _ *logger.Logger, _ FailureArgs,
) error {
	// IPTables rules are applied immediately, so we don't need to wait for them to propagate.
	return nil
}
