// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"strconv"
	"strings"
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
  roachprod chaos network-partition mycluster --src 1 --dest 2 --type bidirectional --wait-before-recover 10m
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
			c, err := validateNodesInCluster(clusterName, src, "source")
			if err != nil {
				return err
			}
			if _, err := validateNodesInCluster(clusterName, dest, "destination"); err != nil {
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

			// Create failer from registry using computed Secure value from cluster
			failer, err := createFailer(
				clusterName,
				failures.IPTablesNetworkPartitionName,
				opts,
				getClusterOptions(c.Secure)...,
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

// buildLatencyRule parses and validates a single latency rule from string inputs.
func buildLatencyRule(
	clusterName, srcNodesStr, destNodesStr, delayStr string,
) (failures.ArtificialLatency, error) {
	// Parse source nodes
	srcParts := strings.Split(srcNodesStr, ",")
	srcNodes := make([]int32, 0, len(srcParts))
	for _, s := range srcParts {
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
		if err != nil {
			return failures.ArtificialLatency{}, errors.Wrapf(err, "invalid source node in --src %q", srcNodesStr)
		}
		srcNodes = append(srcNodes, int32(n))
	}

	// Parse destination nodes
	destParts := strings.Split(destNodesStr, ",")
	destNodes := make([]int32, 0, len(destParts))
	for _, s := range destParts {
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
		if err != nil {
			return failures.ArtificialLatency{}, errors.Wrapf(err, "invalid destination node in --dest %q", destNodesStr)
		}
		destNodes = append(destNodes, int32(n))
	}

	// Parse delay
	delay, err := time.ParseDuration(delayStr)
	if err != nil {
		return failures.ArtificialLatency{}, errors.Wrapf(err, "invalid delay %q", delayStr)
	}
	if delay <= 0 {
		return failures.ArtificialLatency{}, errors.Newf("--delay must be greater than 0, got %s", delayStr)
	}

	// Validate nodes in cluster
	src := parseInt32SliceToNodes(srcNodes)
	dest := parseInt32SliceToNodes(destNodes)
	if _, err := validateNodesInCluster(clusterName, src, "source"); err != nil {
		return failures.ArtificialLatency{}, err
	}
	if _, err := validateNodesInCluster(clusterName, dest, "destination"); err != nil {
		return failures.ArtificialLatency{}, err
	}

	return failures.ArtificialLatency{
		Source:      src,
		Destination: dest,
		Delay:       delay,
	}, nil
}

// buildChaosNetworkLatencyCmd creates the network-latency chaos command
func (cr *commandRegistry) buildChaosNetworkLatencyCmd() *cobra.Command {
	var srcNodesList, destNodesList, delayList []string

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

  # Add multiple latency rules at once (specify each flag multiple times)
  roachprod chaos network-latency mycluster \
    --src 1,2,3 --dest 4,5,6 --delay 32ms \
    --src 4,5,6 --dest 1,2,3 --delay 32ms \
    --src 1,2,3 --dest 7,8,9 --delay 43ms

  # Run with custom cleanup time
  roachprod chaos network-latency mycluster \
    --src 1 --dest 2 --delay 500ms --wait-before-recover 15m
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			// Validate that src, dest, and delay lists have the same length,
			// since we will assume the nth index of each corresponds to a single latency rule.
			if len(srcNodesList) != len(destNodesList) || len(srcNodesList) != len(delayList) {
				return errors.Newf("must provide equal number of --src, --dest, and --delay flags (got %d, %d, %d)",
					len(srcNodesList), len(destNodesList), len(delayList))
			}

			clusterName := args[0]
			var artificialLatencies []failures.ArtificialLatency

			// Parse each latency rule
			for i := 0; i < len(srcNodesList); i++ {
				rule, err := buildLatencyRule(clusterName, srcNodesList[i], destNodesList[i], delayList[i])
				if err != nil {
					return err
				}
				artificialLatencies = append(artificialLatencies, rule)

				config.Logger.Printf("Rule %d: Injecting %s latency: %s -> %s",
					i+1, rule.Delay, formatNodeList(rule.Source), formatNodeList(rule.Destination))
			}

			// Build failure args with all latency rules
			failureArgs := failures.NetworkLatencyArgs{
				ArtificialLatencies: artificialLatencies,
			}

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Redundant validation, since it was done above, but we need to know if the
			// cluster is secure or not which can't be easily done without constructing
			// the cluster.
			c, err := validateNodesInCluster(clusterName, artificialLatencies[0].Source, "source")
			if err != nil {
				return err
			}

			// Create failer from registry using computed Secure value from cluster
			failer, err := createFailer(
				clusterName,
				failures.NetworkLatencyName,
				opts,
				getClusterOptions(c.Secure)...,
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

	cmd.Flags().StringArrayVarP(&srcNodesList, "src", "s", nil,
		"source nodes that will experience the injected latency (can be specified multiple times)")
	cmd.Flags().StringArrayVarP(&destNodesList, "dest", "d", nil,
		"destination nodes that will experience the injected latency (can be specified multiple times)")
	cmd.Flags().StringArrayVar(&delayList, "delay", nil,
		"delay to inject between source and destination nodes (e.g. 100ms, 1s) (can be specified multiple times)")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dest")
	_ = cmd.MarkFlagRequired("delay")

	return cmd
}
