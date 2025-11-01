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

// FailureStage represents a single stage in the failure injection lifecycle
type FailureStage string

const (
	// StageAll runs the complete lifecycle: Setup → Inject → Wait → Recover → Cleanup
	StageAll FailureStage = "all"
	// StageSetup runs only the setup phase
	StageSetup FailureStage = "setup"
	// StageInject runs only the inject phase
	StageInject FailureStage = "inject"
	// StageRecover runs only the recover phase
	StageRecover FailureStage = "recover"
	// StageCleanup runs only the cleanup phase
	StageCleanup FailureStage = "cleanup"
)

// ValidStages returns all valid lifecycle stage values for a failure
func ValidStages() []string {
	return []string{
		string(StageAll),
		string(StageSetup),
		string(StageInject),
		string(StageRecover),
		string(StageCleanup),
	}
}

// Global chaos options that apply to all chaos commands
var (
	chaosWaitBeforeCleanup time.Duration
	chaosRunForever        bool
	chaosCertsDir          string
	chaosReplicationFactor int
	chaosStage             string
)

// GlobalChaosOpts captures global chaos flags
type GlobalChaosOpts struct {
	WaitBeforeCleanup time.Duration
	RunForever        bool
	Stage             FailureStage
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
	chaosCmd.PersistentFlags().StringVar(&chaosStage,
		"stage", string(StageAll),
		`lifecycle stage to execute. Options:
  - all: runs the complete lifecycle (Setup → Inject → Wait → Recover → Cleanup)
  - setup: runs only the setup phase (prepares failure dependencies)
  - inject: runs only the inject phase (activates the failure)
  - recover: runs only the recover phase (removes the failure)
  - cleanup: runs only the cleanup phase (removes failure dependencies)
Default: all`)

	// Add subcommands
	chaosCmd.AddCommand(cr.buildChaosNetworkCmd())

	return chaosCmd
}

// getGlobalChaosOpts returns the global chaos options from flags.
// It validates the stage flag and returns an error if invalid.
func getGlobalChaosOpts() (GlobalChaosOpts, error) {
	stage := FailureStage(chaosStage)

	// Validate stage
	validStages := ValidStages()
	isValid := false
	for _, s := range validStages {
		if string(stage) == s {
			isValid = true
			break
		}
	}
	if !isValid {
		return GlobalChaosOpts{}, errors.Newf("invalid stage %q, must be one of: %s",
			chaosStage, strings.Join(validStages, ", "))
	}

	return GlobalChaosOpts{
		WaitBeforeCleanup: chaosWaitBeforeCleanup,
		RunForever:        chaosRunForever,
		Stage:             stage,
	}, nil
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

// createFailer creates a failer instance from the registry.
// State validation is enabled (disableStateValidation=false) only when stage is StageAll.
// For individual stages, state validation is disabled (disableStateValidation=true) to allow
// running stages independently without enforcing the complete lifecycle order.
func createFailer(
	clusterName string, failureName string, stage FailureStage, opts ...failures.ClusterOptionFunc,
) (*failures.Failer, error) {
	registry := failures.GetFailureRegistry()
	disableStateValidation := stage != StageAll
	return registry.GetFailer(
		clusterName,
		failureName,
		config.Logger,
		disableStateValidation,
		opts...,
	)
}

// runFailureLifecycle executes the failure lifecycle based on the specified stage.
// If stage is StageAll, runs the complete lifecycle: Setup → Inject → Wait → Recover → Cleanup.
// Otherwise, runs only the specified individual stage.
func runFailureLifecycle(
	ctx context.Context,
	l *logger.Logger,
	failer *failures.Failer,
	args failures.FailureArgs,
	opts GlobalChaosOpts,
) error {
	switch opts.Stage {
	case StageSetup:
		return runSetupStage(ctx, l, failer, args)
	case StageInject:
		return runInjectStage(ctx, l, failer, args)
	case StageRecover:
		return runRecoverStage(ctx, l, failer, args)
	case StageCleanup:
		return runCleanupStage(ctx, l, failer, args)
	case StageAll:
		return runFullLifecycle(ctx, l, failer, args, opts)
	default:
		return errors.Newf("unknown stage: %s", opts.Stage)
	}
}

// runSetupStage runs only the setup phase
func runSetupStage(
	ctx context.Context, l *logger.Logger, failer *failures.Failer, args failures.FailureArgs,
) error {
	l.Printf("Running setup stage...")
	if err := failer.Setup(ctx, l, args); err != nil {
		return errors.Wrap(err, "failed to setup failure")
	}

	l.Printf("Setup stage completed successfully")
	return nil
}

// runInjectStage runs only the inject phase
func runInjectStage(
	ctx context.Context, l *logger.Logger, failer *failures.Failer, args failures.FailureArgs,
) error {
	l.Printf("Running inject stage...")
	if err := failer.Inject(ctx, l, args); err != nil {
		return errors.Wrap(err, "failed to inject failure")
	}
	l.Printf("Inject stage completed successfully")
	return nil
}

// runRecoverStage runs only the recover phase.
// When running recover individually with state validation disabled, we use SetInjectArgs
// to provide the necessary context for recovery without actually running the inject phase.
func runRecoverStage(
	ctx context.Context, l *logger.Logger, failer *failures.Failer, args failures.FailureArgs,
) error {
	l.Printf("Running recover stage...")
	// Set the inject args directly so Recover() has the necessary context
	failer.SetInjectArgs(args)
	if err := failer.Recover(ctx, l); err != nil {
		return errors.Wrap(err, "failed to recover from failure")
	}

	if err := failer.WaitForFailureToRecover(ctx, l); err != nil {
		return errors.Wrap(err, "failed to wait for failure to recover")
	}

	l.Printf("Recover stage completed successfully")
	return nil
}

// runCleanupStage runs only the cleanup phase.
// When running cleanup individually with state validation disabled, we use SetSetupArgs
// to provide the necessary context for cleanup without actually running the setup phase.
func runCleanupStage(
	ctx context.Context, l *logger.Logger, failer *failures.Failer, args failures.FailureArgs,
) error {
	l.Printf("Running cleanup stage...")

	// Set the setup args directly so Cleanup() has the necessary context
	failer.SetSetupArgs(args)
	if err := failer.Cleanup(ctx, l); err != nil {
		return errors.Wrap(err, "failed to cleanup failure")
	}
	l.Printf("Cleanup stage completed successfully")
	return nil
}

// runFullLifecycle executes the complete failure lifecycle:
// Setup → Inject → Wait → Recover → Cleanup
func runFullLifecycle(
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
	if err := runSetupStage(ctx, l, failer, args); err != nil {
		return err
	}

	// Inject phase
	if err := runInjectStage(ctx, l, failer, args); err != nil {
		return err
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
	if err := runRecoverStage(ctx, l, failer, args); err != nil {
		return err
	}

	// Cleanup phase
	if err := runCleanupStage(ctx, l, failer, args); err != nil {
		return err
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
