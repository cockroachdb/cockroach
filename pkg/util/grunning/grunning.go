// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package grunning is a library that's able to retrieve on-CPU running time for
// individual goroutines. It relies on using a patched Go and provides a
// primitive for fine-grained CPU attribution and control. See #82356 for more
// details.
package grunning

import "time"

// Time returns the time spent by the current goroutine in the running state.
func Time() time.Duration {
	return time.Duration(grunningnanos())
}

// Subtract is a helper function to subtract a duration from another. It's
// commonly used to measure how much running time is spent doing some piece of
// work.
//
//gcassert:inline
func Subtract(end, start time.Duration) time.Duration {
	return time.Duration(end.Nanoseconds() - start.Nanoseconds())
}

// Supported returns true iff per-goroutine running time is available in this
// build. We use a patched Go runtime for all platforms officially supported for
// CRDB when built using Bazel. Engineers commonly building CRDB also use happen
// to use two platforms we don't use a patched Go for:
// - FreeBSD (we don't have cross-compilers setup), and
// - M1/M2 Macs (we don't have a code-signing pipeline, yet).
// We use '(darwin && arm64) || freebsd || !bazel' as the build tag to exclude
// unsupported platforms.
func Supported() bool {
	return supported()
}
