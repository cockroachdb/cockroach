// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/errors"
)

// inspectCheck defines a single validation operation used by the INSPECT system.
// Each check represents a specific type of data validation, such as index consistency.
//
// A check is stateful. It must be initialized with Start(), which prepares it to
// produce results. Once started, repeated calls to Next() yield zero or more
// inspectIssue results until Done() returns true. After completion, Close() releases
// any associated resources.
//
// Checks are expected to run on a single node and may execute SQL queries or scans
// under the hood to detect inconsistencies. All results are surfaced through the
// inspectIssue type.
type inspectCheck interface {
	// Started reports whether the check has been initialized.
	Started() bool

	// Start prepares the check to begin returning results.
	Start(ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int) error

	// Next returns the next inspect error, if any.
	// Returns (nil, nil) when there are no errors for the current row.
	Next(ctx context.Context, cfg *execinfra.ServerConfig) (*inspectIssue, error)

	// Done reports whether the check has produced all results.
	Done(ctx context.Context) bool

	// Close cleans up resources for the check.
	Close(ctx context.Context) error
}

// inspectLogger records issues found by inspect checks. Implementations of this
// interface define how inspectIssue results are handled.
type inspectLogger interface {
	logIssue(ctx context.Context, issue *inspectIssue) error
}

// inspectRunner coordinates the execution of a set of inspectChecks.
//
// It manages the lifecycle of each check, including initialization,
// iteration, and cleanup. Each call to Step processes one unit of
// work: either advancing a check by one result or moving on to the next
// check if the current one is finished.
//
// When a validation issue is found, the runner calls the provided
// inspectLogger to record it.
type inspectRunner struct {
	// checks holds the list of checks to run. Each check is run to completion
	// before moving on to the next.
	checks []inspectCheck

	// logger records issues reported by the checks.
	logger inspectLogger
}

// Step advances execution by processing one result from the current inspectCheck.
//
// If the current check is not yet started, it is initialized. If it has more results,
// Step retrieves the next result and logs it if an issue is found. If the check is done,
// it is closed and removed from the queue.
//
// Returns true if a check was advanced or an issue was found. Returns false when all
// checks are complete. If an error occurs at any stage, it is returned immediately.
func (c *inspectRunner) Step(
	ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int,
) (bool, error) {
	for len(c.checks) > 0 {
		check := c.checks[0]
		if !check.Started() {
			if err := check.Start(ctx, cfg, span, workerIndex); err != nil {
				return false, err
			}
		}

		if !check.Done(ctx) {
			issue, err := check.Next(ctx, cfg)
			if err != nil {
				return false, err
			}
			if issue != nil {
				err = c.logger.logIssue(ctx, issue)
				if err != nil {
					return false, errors.Wrapf(err, "error logging inspect issue")
				}
			}
			return true, nil
		}

		if err := check.Close(ctx); err != nil {
			return false, err
		}
		c.checks = c.checks[1:]
	}
	return false, nil
}
