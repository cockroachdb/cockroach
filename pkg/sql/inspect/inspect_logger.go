// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/errors"
)

// inspectLogger records issues found by inspect checks. Implementations of this
// interface define how inspectIssue results are handled.
type inspectLogger interface {
	// logIssue records an inspectIssue found by a check.
	logIssue(ctx context.Context, issue *inspectIssue) error

	// hasIssues returns true if any issues have been logged.
	hasIssues() bool
}

// inspectLoggers manages a collection of inspectLogger instances.
type inspectLoggers []inspectLogger

var _ inspectLogger = inspectLoggers{}

func (l inspectLoggers) logIssue(ctx context.Context, issue *inspectIssue) error {
	var retErr error

	for _, logger := range l {
		if err := logger.logIssue(ctx, issue); err != nil {
			retErr = errors.CombineErrors(retErr, err)
		}
	}
	return retErr
}

func (l inspectLoggers) hasIssues() bool {
	for _, logger := range l {
		if logger.hasIssues() {
			return true
		}
	}
	return false
}

// metricsLogger increments metrics when issues are logged.
type metricsLogger struct {
	foundIssue     atomic.Bool
	issuesFoundCtr *aggmetric.Counter
}

var _ inspectLogger = &metricsLogger{}

// logIssue implements the inspectLogger interface.
func (m *metricsLogger) logIssue(ctx context.Context, issue *inspectIssue) error {
	m.foundIssue.Store(true)
	m.issuesFoundCtr.Inc(1)
	return nil
}

// hasIssues implements the inspectLogger interface.
func (m *metricsLogger) hasIssues() bool {
	return m.foundIssue.Load()
}
