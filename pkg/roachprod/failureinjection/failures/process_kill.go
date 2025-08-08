// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"golang.org/x/sys/unix"
)

type ProcessKillArgs struct {
	Nodes install.Nodes
	// GracefulShutdown will allow the cockroach process to drain before exiting.
	GracefulShutdown bool
	// Send an additional SIGKILL if the process is still running after the
	// specified duration. Defaults to 5 minutes if not specified.
	GracePeriod time.Duration
	// Signal kills the process with the specified signal. Incompatible with GracefulShutdown
	// which assumes signal is SIGTERM.
	Signal *int
}

const ProcessKillFailureName = "process-kill"

func registerProcessKillFailure(r *FailureRegistry) {
	r.add(ProcessKillFailureName, ProcessKillArgs{}, MakeProcessKillFailure)
}

func MakeProcessKillFailure(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions,
) (FailureMode, error) {
	genericFailure, err := makeGenericFailure(clusterName, l, clusterOpts, ProcessKillFailureName)
	if err != nil {
		return nil, err
	}

	return &ProcessKillFailure{
		GenericFailure: *genericFailure,
	}, nil
}

type ProcessKillFailure struct {
	GenericFailure
	waitCh <-chan error
}

func (f *ProcessKillFailure) SupportedDeploymentMode(_ roachprodutil.DeploymentMode) bool {
	return true
}

func (f *ProcessKillFailure) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return nil
}

func (f *ProcessKillFailure) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	killArgs := args.(ProcessKillArgs)
	nodes := killArgs.Nodes

	signal := unix.SIGKILL
	if killArgs.GracefulShutdown {
		signal = unix.SIGTERM
	} else if killArgs.Signal != nil {
		signal = sysutil.Signal(*killArgs.Signal)
	}

	gracePeriod := int(killArgs.GracePeriod.Seconds())
	if gracePeriod == 0 {
		gracePeriod = 300
	}

	l.Printf("Shutting down %s n%d with signal %d", f.defaultVirtualCluster.name, nodes, signal)
	label := install.VirtualClusterLabel(f.defaultVirtualCluster.name, f.defaultVirtualCluster.instance)

	desc, err := f.c.ServiceDescriptor(ctx, nodes[0], f.defaultVirtualCluster.name, install.ServiceTypeSQL, f.defaultVirtualCluster.instance)
	if err != nil {
		return err
	}

	if desc.ServiceMode != install.ServiceModeShared {
		// Stop handles both waiting for the process to exit, and re-signaling with SIGKILL after
		// the grace period has passed. However, we want the wait to happen asynchronously in
		// WaitForFailureToPropagate. We run Stop in a goroutine to achieve this, although it
		// does mean we will ignore all errors unless the user also calls WaitForFailureToPropagate.
		// We make this tradeoff in order to avoid maintaining two different Stop implementations.
		f.waitCh, _ = runAsync(ctx, l, func(ctx context.Context) error {
			return f.c.WithNodes(nodes).Stop(ctx, l, int(signal), true, gracePeriod, label)
		})
		return nil
	}

	// If it is a shared process cluster, we stop the service via SQL so there is no need
	// to run it asynchronously since we don't need to re-signal.
	return f.c.WithNodes(nodes).Stop(ctx, l, int(signal), true, gracePeriod, label)
}

func (f *ProcessKillFailure) Recover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(ProcessKillArgs).Nodes
	return f.StartNodes(ctx, l, nodes)
}

func (f *ProcessKillFailure) Cleanup(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	return nil
}

func (f *ProcessKillFailure) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	processKillArgs := args.(ProcessKillArgs)
	nodes := processKillArgs.Nodes
	l.Printf("Waiting for node kill to propagate on nodes: %v", nodes)

	// If we already asynchronously started monitoring the process for death,
	// (i.e. we killed a non shared process secondary tenant), wait for the
	// channel to return.
	if f.waitCh != nil {
		select {
		case err := <-f.waitCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Otherwise we know this is a shared process secondary tenant which
	// was killed by SQL and we just wait for the SQL connection to drop.
	return f.WaitForSQLUnavailable(ctx, l, nodes, 3*time.Minute)
}

func (f *ProcessKillFailure) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(ProcessKillArgs).Nodes
	l.Printf("Waiting for cockroach process to recover on nodes: %v", nodes)

	return f.WaitForRestartedNodesToStabilize(ctx, l, nodes, 20*time.Minute)
}
