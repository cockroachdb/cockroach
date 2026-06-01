// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"log/slog"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/ssh"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const defaultMaxConcurrency = 10

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
