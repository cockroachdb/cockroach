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

// buildChaosDiskStallCmd creates the disk-stall chaos command
func (cr *commandRegistry) buildChaosDiskStallCmd() *cobra.Command {
	var nodes []int32
	var stallType string
	var restartNodes bool
	var cycle, skipFailurePropagation bool
	var cycleStallDuration, cycleUnstallDuration time.Duration

	cmd := &cobra.Command{
		Use:   "disk-stall <cluster>",
		Short: "Injects disk stalls to test storage resilience",
		Long: `Injects disk stalls to test storage resilience.

Disk stall types:
  cgroup:  Uses cgroups v2 to throttle disk I/O operations.
           Supports partial throttling and selective stalling
           (reads only, writes only, or including logs).

  dmsetup: Uses device-mapper linear target with suspend/resume.
           Completely freezes All disk I/O (reads and writes).
           Requires node restart during setup to disable filesystem journaling.
           More closely simulates a hardware disk failure.

By default, stalls are maintained for 5 minutes. Use --run-forever to keep
the stall active until manually stopped with Ctrl+C.

Examples:
  # Stall disk writes on nodes 1,2,3 using cgroup
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1,2,3

  # Completely freeze disk I/O using dmsetup (simulates hardware failure)
  roachprod chaos disk-stall mycluster --type dmsetup --nodes 1,2

  # Cycle stalling on/off with custom durations
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 \
    --cycle --cycle-stall-duration 10s --cycle-unstall-duration 5s

  # Stall until manually interrupted
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 --run-forever

  # Stall for 15 minutes before recovery
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 --wait-before-recover 15m

  # For a secure cluster, specify the local path to certificates
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 --certs-dir /path/to/certs --secure
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			clusterName := args[0]
			nodeList := parseInt32SliceToNodes(nodes)

			// Validate cluster and nodes
			c, err := validateNodesInCluster(clusterName, nodeList, "target")
			if err != nil {
				return err
			}

			// Validate type
			var failureName string
			var failureArgs failures.DiskStallArgs

			switch stallType {
			case "cgroup":
				failureName = failures.CgroupsDiskStallName
				// Cgroup defaults: stall writes completely
				// These can be exposed as flags in the future:
				// - StallReads (default: false)
				// - StallWrites (default: true)
				// - StallLogs (default: false)
				// - Throughput (default: 0 = full stall, can be 2+ for throttling)
				failureArgs = failures.DiskStallArgs{
					StallReads:             false,
					StallWrites:            true,
					StallLogs:              false,
					Throughput:             0, // Full stall
					RestartNodes:           restartNodes,
					Nodes:                  nodeList,
					Cycle:                  cycle,
					CycleStallDuration:     cycleStallDuration,
					CycleUnstallDuration:   cycleUnstallDuration,
					SkipFailurePropagation: true,
				}
				config.Logger.Printf("Stalling disk I/O on nodes %s using cgroup",
					formatNodeList(nodeList))

			case "dmsetup":
				failureName = failures.DmsetupDiskStallName
				// Dmsetup freezes all I/O completely - no granular control like cgroup.
				// It suspends the device-mapper target, blocking all reads and writes.
				failureArgs = failures.DiskStallArgs{
					StallWrites:            true, // Always true for dmsetup (stalls everything)
					RestartNodes:           restartNodes,
					Nodes:                  nodeList,
					Cycle:                  cycle,
					CycleStallDuration:     cycleStallDuration,
					CycleUnstallDuration:   cycleUnstallDuration,
					SkipFailurePropagation: skipFailurePropagation,
				}
				config.Logger.Printf("Stalling disk I/O on nodes %s using dmsetup device-mapper",
					formatNodeList(nodeList))

			default:
				return errors.Newf("--type must be 'cgroup' or 'dmsetup', got: %s", stallType)
			}

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry using computed Secure value from cluster
			failer, err := createFailer(
				clusterName,
				failureName,
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

	cmd.Flags().Int32SliceVarP(&nodes, "nodes", "n", nil,
		"nodes on which to inject the disk stall")
	cmd.Flags().StringVar(&stallType, "type", "cgroup",
		"disk staller type: cgroup (throttles I/O via cgroups v2) or dmsetup (freezes I/O via device-mapper)")
	cmd.Flags().BoolVar(&restartNodes, "restart-nodes", true,
		"allow the failure mode to restart nodes as needed")
	cmd.Flags().BoolVar(&cycle, "cycle", false,
		"repeatedly stall and unstall the disk until recovery")
	cmd.Flags().DurationVar(&cycleStallDuration, "cycle-stall-duration", 5*time.Second,
		"duration of each stall cycle")
	cmd.Flags().DurationVar(&cycleUnstallDuration, "cycle-unstall-duration", 5*time.Second,
		"duration of each unstall cycle")
	cmd.Flags().BoolVar(&skipFailurePropagation, "skip-failure-propagation", true,
		"skip waiting for node unavailability after inject")

	_ = cmd.MarkFlagRequired("nodes")
	initFlagInsecureForCmd(cmd)

	return cmd
}
