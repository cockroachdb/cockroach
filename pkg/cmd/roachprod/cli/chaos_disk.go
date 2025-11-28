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
	var cycle bool
	var cycleStallDuration, cycleUnstallDuration time.Duration

	cmd := &cobra.Command{
		Use:   "disk-stall <cluster>",
		Short: "Injects disk stalls to test storage resilience",
		Long: `Injects disk stalls using cgroup to throttle disk I/O.

Disk stall type:
  cgroup: Uses cgroups v2 to throttle disk I/O operations

By default, cgroup stalls write operations completely. This simulates scenarios where
the disk becomes slow or unresponsive, allowing you to test how CockroachDB handles
degraded storage performance.

The stall can be temporary (default 5 minutes) or you can use --run-forever to keep
it active until manually stopped with Ctrl+C.

Examples:
  # Stall disk I/O on nodes 1,2,3 for 5 minutes (default)
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1,2,3

  # Cycle stalling on/off with custom durations
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 \
    --cycle --cycle-stall-duration 10s --cycle-unstall-duration 5s

  # Stall until manually interrupted
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 --run-forever

  # Stall for 15 minutes before recovery
  roachprod chaos disk-stall mycluster --type cgroup --nodes 1 --wait-before-cleanup 15m

	# Stall for 30s before recovery on an insecure cluster
	roachprod chaos disk-stall mycluster --type cgroup --nodes 1 --wait-before-cleanup 30s --insecure
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			clusterName := args[0]
			nodeList := parseInt32SliceToNodes(nodes)

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
					StallReads:           false,
					StallWrites:          true,
					StallLogs:            false,
					Throughput:           0, // Full stall
					RestartNodes:         restartNodes,
					Nodes:                nodeList,
					Cycle:                cycle,
					CycleStallDuration:   cycleStallDuration,
					CycleUnstallDuration: cycleUnstallDuration,
				}
				config.Logger.Printf("Stalling disk I/O on nodes %s using cgroup",
					formatNodeList(nodeList))

			default:
				return errors.Newf("--type must be 'cgroup', got: %s", stallType)
			}

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry
			failer, err := createFailer(
				clusterName,
				failureName,
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

	cmd.Flags().Int32SliceVarP(&nodes, "nodes", "n", nil,
		"nodes on which to inject the disk stall")
	cmd.Flags().StringVar(&stallType, "type", "",
		"disk staller type: cgroup (uses cgroups v2 to throttle I/O)")
	cmd.Flags().BoolVar(&restartNodes, "restart-nodes", true,
		"allow the failure mode to restart nodes as needed")
	cmd.Flags().BoolVar(&cycle, "cycle", false,
		"repeatedly stall and unstall the disk until recovery")
	cmd.Flags().DurationVar(&cycleStallDuration, "cycle-stall-duration", 5*time.Second,
		"duration of each stall cycle")
	cmd.Flags().DurationVar(&cycleUnstallDuration, "cycle-unstall-duration", 5*time.Second,
		"duration of each unstall cycle")

	_ = cmd.MarkFlagRequired("nodes")
	_ = cmd.MarkFlagRequired("type")
	initFlagInsecureForCmd(cmd)

	return cmd
}
