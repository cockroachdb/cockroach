// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
)

type PartitionType int

func (p PartitionType) String() string {
	switch p {
	case Bidirectional:
		return "Bidirectional"
	case Incoming:
		return "Incoming"
	case Outgoing:
		return "Outgoing"
	default:
		panic(fmt.Sprintf("unknown PartitionType: %d", p))
	}
}

const (
	// Bidirectional drops traffic in both directions.
	Bidirectional PartitionType = iota
	// Incoming drops incoming traffic on the source from the peer nodes
	Incoming
	// Outgoing drops outgoing traffic from the source to the peer nodes
	Outgoing
)

var AllPartitionTypes = []PartitionType{
	Bidirectional,
	Incoming,
	Outgoing,
}

type NetworkPartition struct {
	// Source is a list of nodes that will create the iptables rules to simulate
	// a network partition as described by Type.
	Source install.Nodes
	Peer   install.Nodes
	// Type describes the network partition being created.
	Type PartitionType
}
type NetworkPartitionArgs struct {
	// List of network partitions to create.
	//
	// TODO: Add support for more complex network partitions, e.g. "soft" partitions
	// where not all packets are dropped.
	Partitions []NetworkPartition
}

type IPTablesPartitionFailure struct {
	GenericFailure
}

func MakeIPTablesPartitionFailure(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions,
) (FailureMode, error) {
	genericFailure, err := makeGenericFailure(clusterName, l, clusterOpts, IPTablesNetworkPartitionName)
	if err != nil {
		return nil, err
	}
	return &IPTablesPartitionFailure{*genericFailure}, nil
}

const IPTablesNetworkPartitionName = "iptables-network-partition"

func registerIPTablesPartitionFailure(r *FailureRegistry) {
	r.add(IPTablesNetworkPartitionName, NetworkPartitionArgs{}, MakeIPTablesPartitionFailure)
}

func (f *IPTablesPartitionFailure) SupportedDeploymentMode(mode roachprodutil.DeploymentMode) bool {
	// IPTablesPartitionFailure should ideally be a process scoped failure, i.e. we can define individual
	// partitions from process to process, but iptables rules apply to the entire VM. This
	// makes adding partitions in separate process deployments tricky, as adding a rule to a VM
	// will apply to all processes.
	// TODO(darryl): Add support for IPTablesPartitionFailure in SeparateProcessDeployments.
	// Two possible approaches to consider:
	// 1. Enforce that IPTablesPartitionFailure failures are VM scoped, i.e. we add partitions between machines
	// not processes. One downside to this is that a common simplification roachprod makes is to
	// deploy SQL servers on the same machine as the KV pods which is not realistic.
	// 2. Switch to a more complex network rules alternative that will allow us to mark packets
	// with associated PIDs. The performance impact of this would have to be investigated.
	return mode != roachprodutil.SeparateProcessDeployment
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

	// Drop all incoming and outgoing traffic to peer in both directions.
	bidirectionalPartitionCmd = `
RULE_TYPE=%[1]s
PEER_IP={ip:%[2]d}
PEER_PORT={pgport:%[2]d}
SOURCE_PORT={pgport:%[3]d}

# Drop all incoming traffic from the peer in both directions
sudo iptables ${RULE_TYPE} INPUT -p tcp -s ${PEER_IP} --dport ${SOURCE_PORT} -j DROP;
sudo iptables ${RULE_TYPE} INPUT -p tcp -s ${PEER_IP} --sport ${PEER_PORT} -j DROP;
# Drop all outgoing traffic to the peer in both directions
sudo iptables ${RULE_TYPE} OUTPUT -p tcp -d ${PEER_IP} --dport ${PEER_PORT} -j DROP;
sudo iptables ${RULE_TYPE} OUTPUT -p tcp -d ${PEER_IP} --sport ${SOURCE_PORT} -j DROP;
`
	// Drop all incoming traffic from the peer.
	asymmetricInputPartitionCmd = `
RULE_TYPE=%[1]s
PEER_IP={ip:%[2]d}
PEER_PORT={pgport:%[2]d}
SOURCE_PORT={pgport:%[3]d}

sudo iptables ${RULE_TYPE} INPUT -p tcp -s ${PEER_IP} --dport ${SOURCE_PORT} -j DROP;
`

	// Drop all outgoing traffic to the peer.
	asymmetricOutputPartitionCmd = `
RULE_TYPE=%[1]s
PEER_IP={ip:%[2]d}
PEER_PORT={pgport:%[2]d}
SOURCE_PORT={pgport:%[3]d}

sudo iptables ${RULE_TYPE} OUTPUT -p tcp -d ${PEER_IP} --dport ${PEER_PORT} -j DROP;
`
)

func constructIPTablesRule(
	partitionCmd string, addRule bool, sourceNode, targetNode install.Node,
) string {
	addOrDropRule := "-D"
	if addRule {
		addOrDropRule = "-A"
	}
	return fmt.Sprintf(partitionTemplateWrapper, fmt.Sprintf(
		partitionCmd, addOrDropRule, targetNode, sourceNode,
	))
}

func (f *IPTablesPartitionFailure) Inject(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	partitions := args.(NetworkPartitionArgs).Partitions
	for _, partition := range partitions {
		for _, peerNode := range partition.Peer {
			var cmd string
			switch partition.Type {
			case Bidirectional:
				cmd = constructIPTablesRule(bidirectionalPartitionCmd, true /* addRule */, partition.Source[0], peerNode)
				l.Printf("Dropping packets between nodes %d and node %d", partition.Source, peerNode)
			case Incoming:
				cmd = constructIPTablesRule(asymmetricInputPartitionCmd, true /* addRule */, partition.Source[0], peerNode)
				l.Printf("Dropping packets from node %d to nodes %d", peerNode, partition.Source)
			case Outgoing:
				cmd = constructIPTablesRule(asymmetricOutputPartitionCmd, true /* addRule */, partition.Source[0], peerNode)
				l.Printf("Dropping packets from nodes %d to node %d", partition.Source, peerNode)
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

func (f *IPTablesPartitionFailure) Recover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	partitions := args.(NetworkPartitionArgs).Partitions
	for _, partition := range partitions {

		for _, peerNode := range partition.Peer {
			var cmd string
			switch partition.Type {
			case Bidirectional:
				cmd = constructIPTablesRule(bidirectionalPartitionCmd, false /* addRule */, partition.Source[0], peerNode)
				l.Printf("Resuming packets between nodes %d and node %d", partition.Source, peerNode)
			case Incoming:
				cmd = constructIPTablesRule(asymmetricInputPartitionCmd, false /* addRule */, partition.Source[0], peerNode)
				l.Printf("Resuming packets from node %d to nodes %d", peerNode, partition.Source)
			case Outgoing:
				cmd = constructIPTablesRule(asymmetricOutputPartitionCmd, false /* addRule */, partition.Source[0], peerNode)
				l.Printf("Resuming packets from nodes %d to node %d", partition.Source, peerNode)
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
	// TODO(Darryl): Monitor cluster (e.g. for replica convergence) and block until it's stable.
	return nil
}

func (f *IPTablesPartitionFailure) WaitForFailureToRecover(
	_ context.Context, _ *logger.Logger, _ FailureArgs,
) error {
	// TODO(Darryl): Monitor cluster (e.g. for replica convergence) and block until it's stable.
	return nil
}
