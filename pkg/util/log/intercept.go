// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import "context"

// Intercept diverts log traffic to the given function `f`. When `f` is not nil,
// the logging package begins operating at full verbosity (i.e. `V(n) == true`
// for all `n`) but nothing will be printed to the logs. Instead, `f` is invoked
// for each log entry.
//
// To end log interception, invoke `Intercept()` with `f == nil`. Note that
// interception does not terminate atomically, that is, the originally supplied
// callback may still be invoked after a call to `Intercept` with `f == nil`.
func Intercept(ctx context.Context, f InterceptorFn) {
	// TODO(tschottdorf): restore sanity so that all methods have a *loggingT
	// receiver.
	if f != nil {
		logDepth(ctx, 0, Severity_WARNING, "log traffic is now intercepted; log files will be incomplete", nil)
	}
	logging.interceptor.Store(f) // intentionally also when f == nil
	if f == nil {
		logDepth(ctx, 0, Severity_INFO, "log interception is now stopped; normal logging resumes", nil)
	}
}

// InterceptorFn is the type of function accepted by Intercept().
type InterceptorFn func(entry Entry)
