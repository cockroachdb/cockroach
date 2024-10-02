// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func init() {
	// Inject logging functions into the errors package.
	errors.SetWarningFn(Warningf)
	// Inject logging functions into the syncutil package.
	syncutil.LogExpensiveLogEnabled = untypedExpensiveLogEnabled
	syncutil.LogVEventfDepth = untypedVEventfDepth
}

// Severity aliases a type.
type Severity = logpb.Severity

// V returns true if the logging verbosity is set to the specified level or
// higher.
//
// See also ExpensiveLogEnabled().
//
// TODO(andrei): Audit uses of V() and see which ones should actually use the
// newer ExpensiveLogEnabled().
func V(level Level) bool {
	return VDepth(level, 1)
}

// ExpensiveLogEnabled is used to test whether effort should be used to produce
// log messages whose construction has a measurable cost. It returns true if
// either the current context is recording the trace, or if the caller's
// verbosity is above level.
//
// NOTE: This doesn't take into consideration whether tracing is generally
// enabled or whether a trace.Trace (i.e. sp.netTr) is attached to ctx. In
// particular, if OpenTelemetry collection is enabled, that, by itself, does
// NOT cause the expensive messages to be enabled. "SET tracing" and friends,
// on the other hand, does cause these messages to be enabled, as it shows
// that a user has expressed particular interest in a trace.
//
// Usage:
//
//	if ExpensiveLogEnabled(ctx, 2) {
//	  msg := constructExpensiveMessage()
//	  log.VEventf(ctx, 2, msg)
//	}
func ExpensiveLogEnabled(ctx context.Context, level Level) bool {
	return ExpensiveLogEnabledVDepth(ctx, 1 /* depth */, level)
}

// ExpensiveLogEnabledVDepth is like ExpensiveLogEnabled, and additionally
// accepts a depth parameter for determining the caller's verbosity.
func ExpensiveLogEnabledVDepth(ctx context.Context, depth int, level Level) bool {
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		if sp.IsVerbose() || sp.Tracer().HasExternalSink() {
			return true
		}
	}
	if VDepth(level, depth+1) {
		return true
	}
	return false
}

// untypedExpensiveLogEnabled is like ExpensiveLogEnabled, but takes an untyped
// level argument.
func untypedExpensiveLogEnabled(ctx context.Context, level int32) bool {
	return ExpensiveLogEnabled(ctx, Level(level))
}

// untypedVEventfDepth is like VEventfDepth, but takes an untyped level argument.
func untypedVEventfDepth(
	ctx context.Context, depth int, level int32, format string, args ...interface{},
) {
	VEventfDepth(ctx, depth+1, Level(level), format, args...)
}
