// Package logbase is a low-dependency front-end to log.
//
// Libraries which do not want to pick up the overhead of importing log, perhaps
// because they want to compile on platforms like wasm, or are at risk of
// creating circular dependencies can use this library instead. If log is not
// imported in the program, the functionality will be minimal.
package logbase

import (
	"context"
	golog "log"
)

// ExpensiveLogEnabled is injected from pkg/util/log to avoid an import cycle.
// This also allows it to be mocked out in tests.
//
// See log.ExpensiveLogEnabled for more details.
var ExpensiveLogEnabled = func(ctx context.Context, level int32) bool { return false }

// VEventfDepth is injected from pkg/util/log to avoid an import cycle. This
// also allows it to be mocked out in tests.
//
// See log.VEventfDepth for more details.
var VEventfDepth = func(ctx context.Context, depth int, level int32, format string, args ...interface{}) {}

// Fatalf is injected from pkg/util/log to avoid an import cycle. This also
// allows it to be mocked out in tests.
//
// See log.Fatalf for more details.
var Fatalf = func(ctx context.Context, format string, args ...interface{}) {
	//nolint:fmtsafe
	golog.Fatalf(format, args...)
}

// Warningf is injected from pkg/util/log to avoid an import cycle. This also
// allows it to be mocked out in tests.
//
// See log.Warningf for more details.
var Warningf = func(ctx context.Context, format string, args ...interface{}) {
	//nolint:fmtsafe
	golog.Printf(format, args...)
}

// Infof is injected from pkg/util/log to avoid an import cycle. This also
// allows it to be mocked out in tests.
//
// See log.Infof for more details.
var Infof = func(ctx context.Context, format string, args ...interface{}) {
	//nolint:fmtsafe
	golog.Printf(format, args...)
}
