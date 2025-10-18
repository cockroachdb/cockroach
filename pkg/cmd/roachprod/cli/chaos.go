// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Global chaos options that apply to all chaos commands
var (
	chaosWaitBeforeCleanup time.Duration
	chaosRunForever        bool
	chaosCertsDir          string
	chaosReplicationFactor int
)

// GlobalChaosOpts captures global chaos flags
type GlobalChaosOpts struct {
	WaitBeforeCleanup time.Duration
	RunForever        bool
}

// buildChaosCmd creates the root chaos command
func (cr *commandRegistry) buildChaosCmd() *cobra.Command {
	chaosCmd := &cobra.Command{
		Use:   "chaos [command]",
		Short: "Failure injection related commands",
		Long: `Failure injection related commands for testing cluster resilience.

Failure injection commands allow you to inject various types of failures into a
cluster to test its behavior under adverse conditions. Each failure type has its
own lifecycle: Setup → Inject → Wait → Recover → Cleanup.

Global flags control the duration and cleanup behavior of all chaos commands.
`,
	}

	// Add global flags
	chaosCmd.PersistentFlags().DurationVar(&chaosWaitBeforeCleanup,
		"wait-before-cleanup", 5*time.Minute,
		"time to wait before cleaning up the failure")
	chaosCmd.PersistentFlags().BoolVar(&chaosRunForever,
		"run-forever", false,
		"if set, takes precedence over --wait-before-cleanup. On graceful shutdown, cleans up the injected failure")
	chaosCmd.PersistentFlags().StringVar(&chaosCertsDir,
		"certs-dir", install.CockroachNodeCertsDir,
		"local path to certs directory for secure clusters")
	chaosCmd.PersistentFlags().IntVar(&chaosReplicationFactor,
		"replication-factor", 0,
		"expected replication factor for the cluster (0 = use default of 3)")

	// Add subcommands
	chaosCmd.AddCommand(cr.buildChaosNetworkCmd())

	return chaosCmd
}

// getGlobalChaosOpts returns the global chaos options from flags
func getGlobalChaosOpts() GlobalChaosOpts {
	return GlobalChaosOpts{
		WaitBeforeCleanup: chaosWaitBeforeCleanup,
		RunForever:        chaosRunForever,
	}
}

// getClusterOptions returns cluster options for creating a failer
func getClusterOptions() []failures.ClusterOptionFunc {
	opts := []failures.ClusterOptionFunc{
		failures.Secure(!insecure),
		failures.LocalCertsPath(chaosCertsDir),
	}
	if chaosReplicationFactor > 0 {
		opts = append(opts, failures.ReplicationFactor(chaosReplicationFactor))
	}
	return opts
}

// parseInt32SliceToNodes converts a uint32 slice to install.Nodes
func parseInt32SliceToNodes(nodes []int32) install.Nodes {
	if len(nodes) == 0 {
		return nil
	}
	result := make(install.Nodes, len(nodes))
	for i, n := range nodes {
		result[i] = install.Node(n)
	}
	return result
}

// createFailer creates a failer instance from the registry
func createFailer(
	clusterName string, failureName string, opts ...failures.ClusterOptionFunc,
) (*failures.Failer, error) {
	registry := failures.GetFailureRegistry()
	return registry.GetFailer(
		clusterName,
		failureName,
		config.Logger,
		false, // disableStateValidation
		opts...,
	)
}

// runFailureLifecycle executes the full failure lifecycle:
// Setup → Inject → Wait → Recover → Cleanup
func runFailureLifecycle(
	ctx context.Context,
	l *logger.Logger,
	failer *failures.Failer,
	args failures.FailureArgs,
	opts GlobalChaosOpts,
) error {
	// Ensure cleanup always runs, even if we panic or get interrupted
	cleanupDone := false
	defer func() {
		if !cleanupDone {
			l.Printf("Running cleanup due to early exit...")
			if err := failer.Cleanup(ctx, l); err != nil {
				l.Errorf("Cleanup failed: %v", err)
			}
		}
	}()

	// Setup phase
	l.Printf("Setting up failure dependencies...")
	if err := failer.Setup(ctx, l, args); err != nil {
		return errors.Wrap(err, "failed to setup failure")
	}

	// Inject phase
	l.Printf("Injecting failure...")
	if err := failer.Inject(ctx, l, args); err != nil {
		return errors.Wrap(err, "failed to inject failure")
	}

	// Wait phase
	if opts.RunForever {
		l.Printf("Failure injected. Waiting for interrupt (Ctrl+C)...")
		<-waitForInterrupt()
		l.Printf("Interrupt received. Beginning recovery...")
	} else {
		l.Printf("Failure injected. Waiting %s before recovery...", opts.WaitBeforeCleanup)
		select {
		case <-time.After(opts.WaitBeforeCleanup):
			l.Printf("Wait period complete. Beginning recovery...")
		case <-waitForInterrupt():
			l.Printf("Interrupt received. Beginning recovery...")
		}
	}

	// Recover phase
	l.Printf("Recovering from failure...")
	if err := failer.Recover(ctx, l); err != nil {
		return errors.Wrap(err, "failed to recover from failure")
	}

	// Cleanup phase
	l.Printf("Cleaning up failure dependencies...")
	if err := failer.Cleanup(ctx, l); err != nil {
		return errors.Wrap(err, "failed to cleanup failure")
	}

	cleanupDone = true
	l.Printf("Failure lifecycle completed successfully")
	return nil
}

// waitForInterrupt returns a channel that receives a signal when SIGINT or SIGTERM is received
func waitForInterrupt() <-chan os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	return sigCh
}

// validateClusterAndNodes validates that:
// 1. The cluster exists
// 2. The source and destination nodes are valid for the cluster
func validateClusterAndNodes(clusterName string, srcNodes, destNodes install.Nodes) error {
	// Load clusters to ensure the cache is up to date
	if err := roachprod.LoadClusters(); err != nil {
		return errors.Wrap(err, "failed to load clusters")
	}

	// Get cluster to validate it exists and get node count
	c, err := roachprod.GetClusterFromCache(
		config.Logger,
		clusterName,
		install.SimpleSecureOption(!insecure),
	)
	if err != nil {
		return errors.Wrapf(err, "cluster %q not found", clusterName)
	}

	// Validate source nodes
	if err := validateNodesInCluster(c, srcNodes, "source"); err != nil {
		return err
	}

	// Validate destination nodes
	if err := validateNodesInCluster(c, destNodes, "destination"); err != nil {
		return err
	}

	return nil
}

// validateNodesInCluster validates that all nodes are within the cluster's range
func validateNodesInCluster(c *install.SyncedCluster, nodes install.Nodes, name string) error {
	if len(nodes) == 0 {
		return errors.Newf("%s nodes cannot be empty", name)
	}

	clusterNodes := c.Nodes
	if len(clusterNodes) == 0 {
		return errors.Newf("cluster has no nodes")
	}

	maxNode := clusterNodes[len(clusterNodes)-1]
	for _, n := range nodes {
		if n < 1 || n > maxNode {
			return errors.Newf("%s node %d is out of range (cluster has nodes 1-%d)",
				name, n, maxNode)
		}
	}
	return nil
}

// formatNodeList formats a node list for display
func formatNodeList(nodes install.Nodes) string {
	if len(nodes) == 0 {
		return "none"
	}
	parts := make([]string, len(nodes))
	for i, n := range nodes {
		parts[i] = fmt.Sprintf("%d", n)
	}
	return strings.Join(parts, ",")
}
