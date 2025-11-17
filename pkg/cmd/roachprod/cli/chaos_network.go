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

// buildChaosNetworkPartitionCmd creates the network-partition chaos command
func (cr *commandRegistry) buildChaosNetworkPartitionCmd() *cobra.Command {
	var srcNodes, destNodes []int32
	var partitionType string
	var direction string

	cmd := &cobra.Command{
		Use:   "network-partition <cluster>",
		Short: "Creates network partitions between nodes using iptables",
		Long: `Creates network partitions between nodes using iptables.

Network partitions drop packets between specified nodes, simulating network failures.
Partitions can be bidirectional (both directions) or asymmetric (one direction only).

Partition types:
  bidirectional: drops traffic in both directions between source and destination
  asymmetric: drops traffic in one direction only (use --direction to specify)

Direction options (for asymmetric partitions):
  incoming: drops incoming traffic on the source from the destination
  outgoing: drops outgoing traffic from the source to the destination (default)

Examples:
  # Create bidirectional partition between node 1 and nodes 2,3
  roachprod chaos network-partition mycluster --src 1 --dest 2,3 --type bidirectional

  # Create asymmetric partition dropping outgoing traffic from node 1 to nodes 2,3
  roachprod chaos network-partition mycluster --src 1 --dest 2,3 --type asymmetric

  # Create asymmetric partition dropping incoming traffic on node 1 from nodes 2,3
  roachprod chaos network-partition mycluster --src 1 --dest 2,3 --type asymmetric --direction incoming

  # Run partition for 10 minutes before cleanup
  roachprod chaos network-partition mycluster --src 1 --dest 2 --type bidirectional --wait-before-cleanup 10m
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			// Validate partition type
			var failurePartitionType failures.PartitionType
			switch partitionType {
			case "bidirectional":
				failurePartitionType = failures.Bidirectional
			case "asymmetric":
				switch direction {
				case "incoming":
					failurePartitionType = failures.Incoming
				case "outgoing":
					failurePartitionType = failures.Outgoing
				default:
					return errors.Newf("--direction must be 'incoming' or 'outgoing', got: %s", direction)
				}
			default:
				return errors.Newf("--type must be 'bidirectional' or 'asymmetric', got: %s", partitionType)
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
					Type:        failurePartitionType,
				}},
			}

			// Log the partition being created
			if failurePartitionType == failures.Bidirectional {
				config.Logger.Printf("Creating bidirectional partition: %s <-> %s",
					formatNodeList(src), formatNodeList(dest))
			} else if failurePartitionType == failures.Incoming {
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
				opts,
				getClusterOptions()...,
			)
			if err != nil {
				return err
			}

			return runFailureLifecycle(
				context.Background(),
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
	cmd.Flags().StringVar(&partitionType, "type", "",
		"partition type: bidirectional (both directions) or asymmetric (one direction)")
	cmd.Flags().StringVar(&direction, "direction", "outgoing",
		"direction for asymmetric partitions: incoming or outgoing (default: outgoing)")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dest")
	_ = cmd.MarkFlagRequired("type")

	return cmd
}

// buildChaosNetworkLatencyCmd creates the network-latency chaos command
func (cr *commandRegistry) buildChaosNetworkLatencyCmd() *cobra.Command {
	var srcNodes, destNodes []int32
	var delay time.Duration

	cmd := &cobra.Command{
		Use:   "network-latency <cluster>",
		Short: "Injects artificial network latency between nodes using tc and iptables",
		Long: `Injects artificial network latency between nodes using tc (traffic control) and iptables.

This command adds a specified delay to network packets traveling from source nodes
to destination nodes. The delay is one-way; for round-trip latency, you may need
to run the command twice with source and destination swapped.

The latency is implemented using Linux tc (traffic control) with netem (network
emulation) qdisc, which provides realistic network delay simulation.

Examples:
  # Add 100ms latency from node 1 to nodes 2,3
  roachprod chaos network-latency mycluster --src 1 --dest 2,3 --delay 100ms

  # Add 1 second latency between node groups
  roachprod chaos network-latency mycluster --src 1,2 --dest 3,4 --delay 1s

  # Run with custom cleanup time
  roachprod chaos network-latency mycluster \
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
				opts,
				getClusterOptions()...,
			)
			if err != nil {
				return err
			}

			// Execute lifecycle
			return runFailureLifecycle(
				context.Background(),
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
	cmd.Flags().DurationVar(&delay, "delay", time.Millisecond*100,
		"delay to inject between source and destination nodes (e.g. 100ms, 1s), default 100ms")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dest")

	return cmd
}
