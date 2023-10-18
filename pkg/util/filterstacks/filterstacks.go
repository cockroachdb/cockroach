// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package filterstacks provides facilities for efficiently writing goroutine
// dumps that are limited to the subset of stacks containing stack frames that
// match a particular predicate.
package filterstacks

import (
	"fmt"
	"io"
	"runtime"
)

// Dump writes a filtered goroutine stack trace to the provided writer.
//
// It collects a goroutine profile of all goroutine stacks, filters them to only
// stacks that contain at least one Frame matching the provided predicate, and
// writes the resulting stacks to the writer.
func Dump(w io.Writer, predicate func(runtime.Frame) bool) {
	forEachMatchingGoroutineStack(predicate, func(n int, record runtime.StackRecord) {
		if n > 0 {
			fmt.Fprintln(w)
		}
		frames := runtime.CallersFrames(record.Stack())
		for {
			frame, more := frames.Next()
			// Skip the final `runtime.goexit` because the line numbers within
			// GOROOT are dependent on the Go version which interferes with test
			// detemerminism.
			if frame.Function != `runtime.goexit` {
				fmt.Fprintf(w, "%s\n  %s:%d\n", frame.Function, frame.File, frame.Line)
			}
			if !more {
				break
			}
		}
	})
}

func forEachMatchingGoroutineStack(
	predicate func(runtime.Frame) bool, matched func(int, runtime.StackRecord),
) {
	// Grab a count of the number of goroutines. There's a race that more
	// goroutines may be created before our call to GoroutineProfile
	// stops-the-world. We add a little buffer to account for it, and retry if
	// necessary. This is similar to what the Go standard library does:
	// https://cs.opensource.google/go/go/+/refs/tags/go1.21.0:src/runtime/pprof/pprof.go;l=718-740
	n := runtime.NumGoroutine()
	var ok bool
	var p []runtime.StackRecord
	for !ok {
		// Add on an extra constant 10 + 5% to the last known count of
		// goroutines to accommodate the creation of new goroutines.
		p = make([]runtime.StackRecord, n+10+n/20)

		n, ok = runtime.GoroutineProfile(p)

		// If !ok, the profile grew and we'll try again, reallocating.
	}

	k := 0
	for i := 0; i < n; i++ {
		frames := runtime.CallersFrames(p[i].Stack())
		// Iterate through the frames in one pass to check if this stack
		// contains any matching frames.
		for frame, more := frames.Next(); more; frame, more = frames.Next() {
			if predicate(frame) {
				matched(k, p[i])
				k++
				break
			}
		}
	}
}
