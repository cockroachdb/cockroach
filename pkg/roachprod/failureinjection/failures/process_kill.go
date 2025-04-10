// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sys/unix"
)

type ProcessKillArgs struct {
	Nodes install.Nodes
	// GracefulShutdown will allow the cockroach process to drain before exiting.
	GracefulShutdown bool
	// Send an additional SIGKILL if the process is still running after the
	// specified duration.
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
	clusterName string, l *logger.Logger, secure bool,
) (FailureMode, error) {
	c, err := roachprod.GetClusterFromCache(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}

	return &ProcessKillFailure{
		GenericFailure: GenericFailure{
			c:        c,
			runTitle: ProcessKillFailureName,
		},
	}, nil
}

type ProcessKillFailure struct {
	GenericFailure
	systemdName string
}

func (f *ProcessKillFailure) Description() string {
	return ProcessKillFailureName
}

func (f *ProcessKillFailure) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return nil
}

func (f *ProcessKillFailure) signalProcess(
	ctx context.Context,
	l *logger.Logger,
	node install.Nodes,
	signal unix.Signal,
	gracePeriod time.Duration,
	name string,
) error {
	label := install.VirtualClusterLabel(install.SystemInterfaceName, 0 /* sqlInstance */)
	var waitCmd string
	if gracePeriod > 0 {
		gracePeriodSeconds := int(gracePeriod.Seconds())
		waitCmd = fmt.Sprintf(`
  for pid in ${pids}; do
    echo "${pid}: checking"
    waitcnt=0
    while kill -0 ${pid}; do
      if [ $waitcnt -gt %[1]d ]; then
         echo "${pid}: node did not shutdown after %[1]ds, performing a SIGKILL"
         kill -9 ${pid}
         break
      fi
      sleep 1
      waitcnt=$(expr $waitcnt + 1)
    done
  done`, gracePeriodSeconds)
	}

	killCmd := f.c.KillProcessAndWaitCmd(
		node[0],
		int(signal),
		"stop",
		label,
		waitCmd,
	)
	if err := f.c.PutString(ctx, l, node, killCmd, "/tmp/kill.sh", 0755); err != nil {
		return err
	}

	cmd := fmt.Sprintf("sudo systemd-run --unit=%s bash -c /tmp/kill.sh",
		name,
	)

	return f.Run(ctx, l, node, cmd)
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
	l.Printf("Shutting down n%d with signal %d", nodes, signal)

	f.systemdName = fmt.Sprintf("fi-kill-%d", timeutil.Now().Unix())

	return forEachNode(nodes, func(node install.Nodes) error {
		return f.signalProcess(ctx, l, node, signal, killArgs.GracePeriod, f.systemdName)
	})
}

func (f *ProcessKillFailure) Recover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(ProcessKillArgs).Nodes
	// Stop the systemd unit that was created to kill the process async
	// in case it is still running.
	if err := f.Run(ctx, l, nodes, fmt.Sprintf("sudo systemctl stop %s || true", f.systemdName)); err != nil {
		return err
	}

	l.Printf("Restarting cockroach process on nodes: %v", nodes)
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

	return forEachNode(nodes, func(n install.Nodes) error {
		return f.WaitForProcessDeath(ctx, l, n, 5*time.Minute)
	})
}

func (f *ProcessKillFailure) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(ProcessKillArgs).Nodes
	l.Printf("Waiting for cockroach process to recover on nodes: %v", nodes)

	return forEachNode(nodes, func(n install.Nodes) error {
		return f.WaitForSQLReady(ctx, l, n, time.Minute)
	})
}
