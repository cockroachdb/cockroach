// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/ssh"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const defaultMaxConcurrency = 10

// RunCommandExecutor implements the run-command hook type. It SSHes into each
// target machine and runs the declared command via bash -s. Supports optional
// retry with interval+timeout for commands like boot-readiness checks.
type RunCommandExecutor struct {
	sshClient ssh.ISSHClient
}

// NewRunCommandExecutor creates a new run-command executor.
func NewRunCommandExecutor(sshClient ssh.ISSHClient) *RunCommandExecutor {
	return &RunCommandExecutor{sshClient: sshClient}
}

// Execute runs the hook command on all target machines with bounded
// concurrency. If a retry block is configured, each machine's command is
// retried until success or timeout.
func (e *RunCommandExecutor) Execute(
	ctx context.Context, l *logger.Logger, hctx HookContext,
) error {
	script, err := buildScript(hctx.HookEnv, hctx.Declaration.Command)
	if err != nil {
		return errors.Wrap(err, "build script")
	}

	if hctx.Declaration.Retry != nil {
		return e.runWithRetry(ctx, l, hctx, script)
	}

	return runOnMachines(
		ctx, l, e.sshClient,
		hctx.MachineIPs, hctx.SSHUser, hctx.SSHPrivateKey,
		script, defaultMaxConcurrency, hctx.Declaration.Name,
	)
}

// runWithRetry runs the command on each machine, retrying failures until the
// timeout is reached or all machines succeed.
func (e *RunCommandExecutor) runWithRetry(
	ctx context.Context, l *logger.Logger, hctx HookContext, script string,
) error {
	interval, err := time.ParseDuration(hctx.Declaration.Retry.Interval)
	if err != nil {
		return errors.Wrapf(err, "parse retry interval %q", hctx.Declaration.Retry.Interval)
	}
	timeout, err := time.ParseDuration(hctx.Declaration.Retry.Timeout)
	if err != nil {
		return errors.Wrapf(err, "parse retry timeout %q", hctx.Declaration.Retry.Timeout)
	}

	deadline := timeutil.Now().Add(timeout)
	sem := make(chan struct{}, defaultMaxConcurrency)
	var mu syncutil.Mutex
	var finalErrs []error

	var wg sync.WaitGroup
	for _, ip := range hctx.MachineIPs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			err := e.retryOnMachine(
				ctx, l, addr, hctx.SSHUser, hctx.SSHPrivateKey,
				script, interval, deadline, hctx.Declaration.Name,
			)
			if err != nil {
				func() {
					mu.Lock()
					defer mu.Unlock()
					finalErrs = append(finalErrs,
						errors.Wrapf(err, "machine %s", addr))
				}()
			}
		}(ip)
	}
	wg.Wait()

	if len(finalErrs) > 0 {
		return errors.Join(finalErrs...)
	}
	return nil
}

// retryOnMachine runs the script on a single machine, retrying at the
// specified interval until either success or the deadline is reached.
func (e *RunCommandExecutor) retryOnMachine(
	ctx context.Context,
	l *logger.Logger,
	addr, user string,
	privateKey []byte,
	script string,
	interval time.Duration,
	deadline time.Time,
	hookName string,
) error {
	l.Info("hook: running on machine",
		slog.String("hook", hookName),
		slog.String("machine", addr),
	)

	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		_, stderr, err := e.sshClient.RunCommand(
			ctx, l, addr, user, privateKey, script,
		)
		if err == nil {
			l.Info("hook: machine succeeded",
				slog.String("hook", hookName),
				slog.String("machine", addr),
				slog.Int("attempts", attempt),
			)
			return nil
		}

		if timeutil.Now().After(deadline) {
			l.Info("hook: machine timed out",
				slog.String("hook", hookName),
				slog.String("machine", addr),
				slog.Int("attempts", attempt),
			)
			l.Debug("hook retry timed out",
				slog.String("hook", hookName),
				slog.String("machine", addr),
				slog.Int("attempts", attempt),
				slog.String("stderr", stderr),
			)
			return errors.Wrapf(err,
				"timed out after %d attempts", attempt)
		}

		l.Debug("hook command failed, retrying",
			slog.String("hook", hookName),
			slog.String("machine", addr),
			slog.Int("attempt", attempt),
			slog.String("stderr", stderr),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// runOnMachines runs a script on multiple machines with bounded concurrency.
// Returns a combined error if any machine fails.
func runOnMachines(
	ctx context.Context,
	l *logger.Logger,
	sshClient ssh.ISSHClient,
	ips []string,
	user string,
	privateKey []byte,
	script string,
	maxConcurrency int,
	hookName string,
) error {
	sem := make(chan struct{}, maxConcurrency)
	var mu syncutil.Mutex
	var errs []error

	var wg sync.WaitGroup
	for _, ip := range ips {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			l.Info("hook: running on machine",
				slog.String("hook", hookName),
				slog.String("machine", addr),
			)

			_, stderr, err := sshClient.RunCommand(
				ctx, l, addr, user, privateKey, script,
			)
			if err != nil {
				l.Info("hook: machine failed",
					slog.String("hook", hookName),
					slog.String("machine", addr),
				)
				l.Debug("command failed on machine",
					slog.String("hook", hookName),
					slog.String("machine", addr),
					slog.String("stderr", stderr),
				)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, errors.Wrapf(err, "machine %s", addr))
			} else {
				l.Info("hook: machine succeeded",
					slog.String("hook", hookName),
					slog.String("machine", addr),
				)
			}
		}(ip)
	}
	wg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
