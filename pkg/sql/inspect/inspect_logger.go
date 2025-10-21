// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
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

// inspectLoggerBundle fan-outs logged inspect issues to a slice of loggers while
// tracking aggregate state about the issues encountered.
type inspectLoggerBundle struct {
	loggers             []inspectLogger
	sawInternalIssue    atomic.Bool
	sawNonInternalIssue atomic.Bool
}

var _ inspectLogger = (*inspectLoggerBundle)(nil)

// newInspectLoggerBundle constructs a bundle that delegates to the provided loggers.
func newInspectLoggerBundle(loggers ...inspectLogger) *inspectLoggerBundle {
	return &inspectLoggerBundle{loggers: loggers}
}

// logIssue implements the inspectLogger interface.
func (l *inspectLoggerBundle) logIssue(ctx context.Context, issue *inspectIssue) error {
	if issue == nil {
		return errors.AssertionFailedf("issue is nil")
	}
	if issue.ErrorType == InternalError {
		l.sawInternalIssue.Store(true)
	} else {
		l.sawNonInternalIssue.Store(true)
	}

	var retErr error
	for _, logger := range l.loggers {
		if err := logger.logIssue(ctx, issue); err != nil {
			retErr = errors.CombineErrors(retErr, err)
		}
	}
	return retErr
}

// hasIssues implements the inspectLogger interface.
func (l *inspectLoggerBundle) hasIssues() bool {
	if l == nil {
		return false
	}
	return l.sawInternalIssue.Load() || l.sawNonInternalIssue.Load()
}

// sawOnlyInternalIssues reports whether every recorded issue was an internal error.
func (l *inspectLoggerBundle) sawOnlyInternalIssues() bool {
	if l == nil {
		return false
	}
	return l.sawInternalIssue.Load() && !l.sawNonInternalIssue.Load()
}

// metricsLogger increments metrics when issues are logged.
type metricsLogger struct {
	foundIssue     atomic.Bool
	issuesFoundCtr *metric.Counter
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
