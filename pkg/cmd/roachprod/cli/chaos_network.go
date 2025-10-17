// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// buildChaosNetworkCmd creates the network chaos command
func (cr *commandRegistry) buildChaosNetworkCmd() *cobra.Command {
	networkCmd := &cobra.Command{
		Use:   "network [command]",
		Short: "Network failure-related commands",
		Long:  `Network failure-related commands for testing cluster behavior under network issues.`,
	}

	// Add subcommands
	networkCmd.AddCommand(cr.buildNetworkPartitionCmd())
	networkCmd.AddCommand(cr.buildNetworkLatencyCmd())

	return networkCmd
}

// buildNetworkPartitionCmd creates the network partition command
func (cr *commandRegistry) buildNetworkPartitionCmd() *cobra.Command {
	partitionCmd := &cobra.Command{
		Use:   "partition [command]",
		Short: "Network partition-related commands",
		Long: `Network partition-related commands for creating network partitions between nodes.

Network partitions use iptables to drop packets between specified nodes, simulating
network failures. Partitions can be bidirectional (both directions) or asymmetric
(one direction only).
`,
	}

	// Add subcommands
	partitionCmd.AddCommand(cr.buildNetworkPartitionBidirectionalCmd())
	partitionCmd.AddCommand(cr.buildNetworkPartitionAsymmetricCmd())

	return partitionCmd
}

// buildNetworkPartitionBidirectionalCmd creates the bidirectional partition command
func (cr *commandRegistry) buildNetworkPartitionBidirectionalCmd() *cobra.Command {
	var srcNodes, destNodes []int32

	cmd := &cobra.Command{
		Use:   "bidirectional <cluster>",
		Short: "Drops traffic in both directions between source and destination nodes",
		Long: `Drops traffic in both directions between source and destination nodes using iptables.

Creates a complete network partition where nodes in the source set cannot communicate
with nodes in the destination set in either direction. This simulates a complete
network split.

Examples:
  # Partition node 1 from nodes 2 and 3
  roachprod chaos network partition bidirectional mycluster --src 1 --dest 2,3

  # Partition nodes 1-2 from nodes 3-4
  roachprod chaos network partition bidirectional mycluster --src 1,2 --dest 3,4

  # Run partition for 10 minutes before cleanup
  roachprod chaos network partition bidirectional mycluster \
    --src 1 --dest 2 --wait-before-cleanup 10m
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			clusterName := args[0]
			src := parseInt32SliceToNodes(srcNodes)
			dest := parseInt32SliceToNodes(destNodes)
			if err := validateClusterAndNodes(clusterName, src, dest); err != nil {
				return err
			}

			// Build failure args
			failureArgs := failures.NetworkPartitionArgs{
				Partitions: []failures.NetworkPartition{{
					Source:      src,
					Destination: dest,
					Type:        failures.Bidirectional,
				}},
			}

			config.Logger.Printf("Creating bidirectional partition: %s <-> %s",
				formatNodeList(src), formatNodeList(dest))

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry
			failer, err := createFailer(
				clusterName,
				failures.IPTablesNetworkPartitionName,
				opts.Stage,
				getClusterOptions()...,
			)
			if err != nil {
				return err
			}

			return runFailureLifecycle(
				context.Background(),
				config.Logger,
				failer,
				failureArgs,
				opts,
			)
		}),
	}

	cmd.Flags().Int32SliceVarP(&srcNodes, "src", "s", nil,
		"source nodes that will have the network partition created")
	cmd.Flags().Int32SliceVarP(&destNodes, "dest", "d", nil,
		"destination nodes that will have the network partition created")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dest")

	return cmd
}

// buildNetworkPartitionAsymmetricCmd creates the asymmetric partition command
func (cr *commandRegistry) buildNetworkPartitionAsymmetricCmd() *cobra.Command {
	var srcNodes, destNodes []int32
	var direction string

	cmd := &cobra.Command{
		Use:   "asymmetric <cluster>",
		Short: "Creates an asymmetric network partition with directional traffic dropping",
		Long: `Creates an asymmetric network partition with directional traffic dropping using iptables.

An asymmetric partition drops traffic in only one direction, which can simulate
firewall misconfigurations or one-way network failures seen in production.

Direction options:
  incoming: drops incoming traffic on the source from the destination
  outgoing: drops outgoing traffic from the source to the destination

Examples:
  # Drop incoming traffic on node 1 from nodes 2,3
  roachprod chaos network partition asymmetric mycluster \
    --src 1 --dest 2,3 --direction incoming

  # Drop outgoing traffic from nodes 1,2 to nodes 3,4
  roachprod chaos network partition asymmetric mycluster \
    --src 1,2 --dest 3,4 --direction outgoing

  # Run partition until interrupted (Ctrl+C)
  roachprod chaos network partition asymmetric mycluster \
    --src 1 --dest 2 --direction incoming --run-forever
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			var partitionType failures.PartitionType
			switch direction {
			case "incoming":
				partitionType = failures.Incoming
			case "outgoing":
				partitionType = failures.Outgoing
			default:
				return errors.Newf("--direction must be 'incoming' or 'outgoing', got: %s", direction)
			}

			clusterName := args[0]
			src := parseInt32SliceToNodes(srcNodes)
			dest := parseInt32SliceToNodes(destNodes)
			if err := validateClusterAndNodes(clusterName, src, dest); err != nil {
				return err
			}

			// Build failure args
			failureArgs := failures.NetworkPartitionArgs{
				Partitions: []failures.NetworkPartition{{
					Source:      src,
					Destination: dest,
					Type:        partitionType,
				}},
			}

			if partitionType == failures.Incoming {
				config.Logger.Printf("Creating asymmetric partition: %s <-X- %s (dropping incoming)",
					formatNodeList(src), formatNodeList(dest))
			} else {
				config.Logger.Printf("Creating asymmetric partition: %s -X-> %s (dropping outgoing)",
					formatNodeList(src), formatNodeList(dest))
			}

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry
			failer, err := createFailer(
				clusterName,
				failures.IPTablesNetworkPartitionName,
				opts.Stage,
				getClusterOptions()...,
			)
			if err != nil {
				return err
			}

			// Execute lifecycle
			return runFailureLifecycle(
				context.Background(),
				config.Logger,
				failer,
				failureArgs,
				opts,
			)
		}),
	}

	cmd.Flags().Int32SliceVarP(&srcNodes, "src", "s", nil,
		"source nodes that will have the network partition created")
	cmd.Flags().Int32SliceVarP(&destNodes, "dest", "d", nil,
		"destination nodes that will have the network partition created")
	cmd.Flags().StringVar(&direction, "direction", "",
		"direction from which to drop traffic (incoming|outgoing)")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dest")
	_ = cmd.MarkFlagRequired("direction")

	return cmd
}

// buildNetworkLatencyCmd creates the network latency command
func (cr *commandRegistry) buildNetworkLatencyCmd() *cobra.Command {
	var srcNodes, destNodes []int32
	var delay time.Duration

	cmd := &cobra.Command{
		Use:   "latency <cluster>",
		Short: "Injects artificial network latency between nodes using tc and iptables",
		Long: `Injects artificial network latency between nodes using tc (traffic control) and iptables.

This command adds a specified delay to network packets traveling from source nodes
to destination nodes. The delay is one-way; for round-trip latency, you may need
to run the command twice with source and destination swapped.

The latency is implemented using Linux tc (traffic control) with netem (network
emulation) qdisc, which provides realistic network delay simulation.

Examples:
  # Add 100ms latency from node 1 to nodes 2,3
  roachprod chaos network latency mycluster --src 1 --dest 2,3 --delay 100ms

  # Add 1 second latency between node groups
  roachprod chaos network latency mycluster --src 1,2 --dest 3,4 --delay 1s

  # Run with custom cleanup time
  roachprod chaos network latency mycluster \
    --src 1 --dest 2 --delay 500ms --wait-before-cleanup 15m
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			if delay <= 0 {
				return errors.New("--delay must be greater than 0")
			}

			clusterName := args[0]
			src := parseInt32SliceToNodes(srcNodes)
			dest := parseInt32SliceToNodes(destNodes)
			if err := validateClusterAndNodes(clusterName, src, dest); err != nil {
				return err
			}

			// Build failure args
			failureArgs := failures.NetworkLatencyArgs{
				ArtificialLatencies: []failures.ArtificialLatency{{
					Source:      src,
					Destination: dest,
					Delay:       delay,
				}},
			}

			config.Logger.Printf("Injecting %s latency: %s -> %s",
				delay, formatNodeList(src), formatNodeList(dest))

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry
			failer, err := createFailer(
				clusterName,
				failures.NetworkLatencyName,
				opts.Stage,
				getClusterOptions()...,
			)
			if err != nil {
				return err
			}

			// Execute lifecycle
			return runFailureLifecycle(
				context.Background(),
				config.Logger,
				failer,
				failureArgs,
				opts,
			)
		}),
	}

	cmd.Flags().Int32SliceVarP(&srcNodes, "src", "s", nil,
		"source nodes that will experience the injected latency")
	cmd.Flags().Int32SliceVarP(&destNodes, "dest", "d", nil,
		"destination nodes that will experience the injected latency")
	cmd.Flags().DurationVar(&delay, "delay", 0,
		"delay to inject between source and destination nodes (e.g. 100ms, 1s)")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dest")
	_ = cmd.MarkFlagRequired("delay")

	return cmd
}
