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

// buildChaosProcessKillCmd creates the process-kill chaos command
func (cr *commandRegistry) buildChaosProcessKillCmd() *cobra.Command {
	var nodes []int32
	var gracefulShutdown bool
	var signal int
	var graceDuration time.Duration

	cmd := &cobra.Command{
		Use:   "process-kill <cluster>",
		Short: "Kills the cockroach process on specified nodes",
		Long: `Kills the cockroach process on specified nodes to test recovery behavior.

This command sends a signal to terminate the cockroach process on the specified
nodes. By default, it sends SIGKILL for immediate termination. Use --graceful
to send SIGTERM instead, allowing the node to drain before exiting.

After the configured wait period (or interrupt), the command restarts the
cockroach process on the affected nodes and waits for them to stabilize.

Shutdown modes:
  default (SIGKILL): Immediate process termination, no drain
  graceful (SIGTERM): Allows node to drain connections before exit
  custom signal: Use --signal to specify any signal number

Examples:
  # Kill cockroach process on nodes 1,2,3 immediately for insecure cluster
  roachprod chaos process-kill mycluster --nodes 1,2,3 --insecure

  # Gracefully shutdown node 1 (SIGTERM with drain)
  roachprod chaos process-kill mycluster --nodes 1 --graceful

  # Kill with custom signal (e.g., SIGQUIT=3)
  roachprod chaos process-kill mycluster --nodes 1 --signal 3
`,
		Args: cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			// Validate flags
			if gracefulShutdown && signal != 0 {
				return errors.New("--graceful and --signal are mutually exclusive")
			}

			clusterName := args[0]
			nodeList := parseInt32SliceToNodes(nodes)

			// Validate cluster and nodes
			if err := validateNodesInCluster(clusterName, nodeList, "target"); err != nil {
				return err
			}

			// Build failure args
			var signalPtr *int
			if signal != 0 {
				signalPtr = &signal
			}

			failureArgs := failures.ProcessKillArgs{
				Nodes:            nodeList,
				GracefulShutdown: gracefulShutdown,
				GracePeriod:      graceDuration,
				Signal:           signalPtr,
			}

			if gracefulShutdown {
				config.Logger.Printf("Gracefully shutting down cockroach on nodes %s (SIGTERM)",
					formatNodeList(nodeList))
			} else if signal != 0 {
				config.Logger.Printf("Killing cockroach on nodes %s with signal %d",
					formatNodeList(nodeList), signal)
			} else {
				config.Logger.Printf("Killing cockroach on nodes %s (SIGKILL)",
					formatNodeList(nodeList))
			}

			opts, err := getGlobalChaosOpts()
			if err != nil {
				return err
			}

			// Create failer from registry
			failer, err := createFailer(
				clusterName,
				failures.ProcessKillFailureName,
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
		"nodes on which to kill the cockroach process")
	cmd.Flags().BoolVar(&gracefulShutdown, "graceful", false,
		"use SIGTERM for graceful shutdown with drain (default: SIGKILL)")
	cmd.Flags().IntVar(&signal, "signal", 0,
		"custom signal number to send (mutually exclusive with --graceful)")
	cmd.Flags().DurationVar(&graceDuration, "grace-period", 5*time.Minute,
		"time to wait before sending SIGKILL after initial signal")

	_ = cmd.MarkFlagRequired("nodes")
	initFlagInsecureForCmd(cmd)
	return cmd
}
