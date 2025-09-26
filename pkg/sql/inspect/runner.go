// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/errors"
)

// inspectCheckApplicability defines the interface for determining if a check
// applies to a span.
type inspectCheckApplicability interface {
	// AppliesTo reports whether this check applies to the given span.
	AppliesTo(codec keys.SQLCodec, span roachpb.Span) (bool, error)
}

// assertCheckApplies is a helper that calls AppliesTo and asserts the check applies.
func assertCheckApplies(
	check inspectCheckApplicability, codec keys.SQLCodec, span roachpb.Span,
) error {
	applies, err := check.AppliesTo(codec, span)
	if err != nil {
		return err
	}
	if !applies {
		return errors.AssertionFailedf(
			"check does not apply to this span: span=%s", span.String())
	}
	return nil
}

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
	inspectCheckApplicability

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

	// foundIssue indicates whether any issues were found.
	foundIssue bool
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
				c.foundIssue = true
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

// CheckCount returns the number of remaining checks to be processed.
func (c *inspectRunner) CheckCount() int {
	return len(c.checks)
}

// Close cleans up all checks in the runner. It will attempt to close each check,
// even if errors occur during closing. If multiple checks fail to close, then
// a combined error is returned.
func (c *inspectRunner) Close(ctx context.Context) error {
	var retErr error
	for _, check := range c.checks {
		if err := check.Close(ctx); err != nil {
			retErr = errors.CombineErrors(retErr, err)
		}
	}
	return retErr
}

// spanContainsTable checks if the given span contains data for the specified table.
func spanContainsTable(tableID descpb.ID, codec keys.SQLCodec, span roachpb.Span) (bool, error) {
	_, spanTableID, err := codec.DecodeTablePrefix(span.Key)
	if err != nil {
		return false, err
	}
	return descpb.ID(spanTableID) == tableID, nil
}
