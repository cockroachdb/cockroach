// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import "os"

const (
	tracebackNone = iota
	tracebackSingle
	tracebackAll
)

// Obey the GOTRACEBACK environment variable for determining which stacks to
// output during a log.Fatal.
var traceback = func() int {
	switch os.Getenv("GOTRACEBACK") {
	case "none":
		return tracebackNone
	case "single", "":
		return tracebackSingle
	default: // "all", "system", "crash"
		return tracebackAll
	}
}()

// DisableTracebacks turns off tracebacks for log.Fatals. Returns a function
// that sets the traceback settings back to where they were.
// Only intended for use by tests.
func DisableTracebacks() func() {
	oldVal := traceback
	traceback = tracebackNone
	return func() { traceback = oldVal }
}
