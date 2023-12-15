// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logbase is a low-dependency front-end to log.
//
// Libraries which do not want to pick up the overhead of importing log, perhaps
// because they want to compile on platforms like wasm, or are at risk of
// creating circular dependencies can use this library instead. If log is not
// imported in the program, the functionality will be minimal.
package logbase

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
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
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, args...))
	exit.WithCode(exit.FatalError())
}

// Warningf is injected from pkg/util/log to avoid an import cycle. This also
// allows it to be mocked out in tests.
//
// See log.Warningf for more details.
var Warningf = func(ctx context.Context, format string, args ...interface{}) {
	//nolint:fmtsafe
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, args...))
}

// Infof is injected from pkg/util/log to avoid an import cycle. This also
// allows it to be mocked out in tests.
//
// See log.Infof for more details.
var Infof = func(ctx context.Context, format string, args ...interface{}) {
	//nolint:fmtsafe
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, args...))
}
