// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/spf13/cobra"
)

// buildChaosResetVMCmd creates the reset-vm chaos command
func (cr *commandRegistry) buildChaosResetVMCmd() *cobra.Command {
	var nodes []int32
	var stopProcesses bool

	cmd := &cobra.Command{
		Use:   "reset-vm <cluster>",
		Short: "Resets VMs on specified nodes to test recovery behavior",
		Long: `Resets VMs on specified nodes to test recovery behavior.

This command resets the virtual machines on the specified nodes, simulating
a hard VM restart. This is useful for testing how the cluster handles sudden
node failures at the infrastructure level.

The reset operation:
1. Optionally stops cockroach processes gracefully before reset (--stop-processes)
2. Resets the VMs on the specified nodes
3. Waits for the nodes to become unavailable
4. On recovery, restarts the cockroach processes
5. Waits for the nodes to stabilize

Note: This command is designed for cloud VMs and may not work with all providers.
Some providers may take several minutes to complete VM reset operations.

Examples:
  # Reset VMs on nodes 1,2,3
  roachprod chaos reset-vm mycluster --nodes 1,2,3

  # Reset VM on node 1 after gracefully stopping processes
  roachprod chaos reset-vm mycluster --nodes 1 --stop-processes

  # Reset VM and wait indefinitely before recovery
  roachprod chaos reset-vm mycluster --nodes 1 --run-forever

  # Reset VM with custom wait time before cleanup
  roachprod chaos reset-vm mycluster --nodes 1 --wait-before-recover 15m

  # For a secure cluster, specify the local path to certificates
  roachprod chaos reset-vm mycluster --nodes 1 --certs-dir /path/to/certs --secure
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

			// Build failure args
			failureArgs := failures.ResetVMArgs{
				Nodes:         nodeList,
				StopProcesses: stopProcesses,
			}

			if stopProcesses {
				config.Logger.Printf("Resetting VMs on nodes %s (with graceful process stop)",
					formatNodeList(nodeList))
			} else {
				config.Logger.Printf("Resetting VMs on nodes %s",
					formatNodeList(nodeList))
			}

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry using computed Secure value from cluster
			failer, err := createFailer(
				clusterName,
				failures.ResetVMFailureName,
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
		"nodes on which to reset the VMs")
	cmd.Flags().BoolVar(&stopProcesses, "stop-processes", false,
		"gracefully stop cockroach processes before resetting the VMs")

	_ = cmd.MarkFlagRequired("nodes")
	initFlagInsecureForCmd(cmd)
	return cmd
}
