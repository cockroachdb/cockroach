// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/errors"
)

type clusterRunner struct {
	// checks holds the list of checks to run. Each check is run to completion
	// before moving on to the next.
	checks []inspectClusterCheck

	// logger records issues reported by the checks.
	logger *inspectLoggerBundle

	progressTracker *inspectProgressTracker
}

// processClusterChecks runs any cluster-level checks using the accumulated job
// progress.
func (c *clusterRunner) Step(ctx context.Context) (bool, error) {
	c.progressTracker.mu.Lock()
	defer c.progressTracker.mu.Unlock()

	for len(c.checks) > 0 {
		check := c.checks[0]

		if !check.Started() {
			if err := check.StartCluster(ctx, &c.progressTracker.mu.spanCheckData); err != nil {
				return false, errors.Wrapf(err, "error starting cluster inspect check")
			}
		}

		for !check.DoneCluster(ctx) {
			issue, err := check.NextCluster(ctx)
			if err != nil {
				return false, errors.Wrapf(err, "error running cluster inspect check")
			}
			if issue != nil {
				err = c.logger.logIssue(ctx, issue)
				if err != nil {
					return false, errors.Wrapf(err, "error logging inspect issue")
				}
			}
			return true, nil
		}

		if err := check.CloseCluster(ctx); err != nil {
			return false, errors.Wrapf(err, "error closing cluster inspect check")
		}

		c.checks = c.checks[1:]
	}

	return false, nil
}

// Close cleans up all checks in the runner. It will attempt to close each check,
// even if errors occur during closing. If multiple checks fail to close, then
// a combined error is returned.
func (c *clusterRunner) Close(ctx context.Context) error {
	var retErr error
	for _, check := range c.checks {
		if err := check.CloseCluster(ctx); err != nil {
			retErr = errors.CombineErrors(retErr, err)
		}
	}
	return retErr
}
