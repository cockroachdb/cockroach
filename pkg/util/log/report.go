// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// reportOrPanic is expected to be injected by pkg/logcrash to avoid a circular
// dependency.
//
// reportOrPanic, once injected, either reports an error to sentry, if run from a
// release binary, or panics, if triggered in tests. This is intended to be used for
// failing assertions which are recoverable but serious enough to report and to
// cause tests to fail.
//
// NB: The format string should not contain any sensitive data, and unsafe reportables
// will be redacted before reporting.
//
// NB2: We provide a default definition that always panics. This is because pkg/logcrash
// injects the real definition in its `init()` function, but this function is used
// in pkg/util/log's *own* `init()` function. pkg/logcrash imports pkg/util/log, not
// the other way around, so pkg/util/log's `init()` function is called first.
// Therefore, to avoid a nil reference in the event that our own package's `init()`
// needs to use reportOrPanic, we provide this default definition.
var reportOrPanic = func(ctx context.Context, sv *settings.Values, format string, reportables ...interface{}) {
	err := errors.Newf("%s", fmt.Sprintf(format, reportables))
	panic(err)
}

// SetReportOrPanicFn injects a definition for reportOrPanic into pkg/util/log.
//
// Used to avoid a dependency cycle.
func SetReportOrPanicFn(
	fn func(ctx context.Context, sv *settings.Values, format string, reportables ...interface{}),
) {
	reportOrPanic = fn
}
