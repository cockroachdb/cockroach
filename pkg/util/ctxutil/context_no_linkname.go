// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file includes an implementation of WhenDone for builds after Go 1.23
// that do not use our Go fork.
//
//go:build !bazel && gc && go1.23

package ctxutil

import "context"

// WhenDone arranges for the specified function to be invoked when
// parent context becomes done and returns true.
// See context_bazel.go for the full documentation on this function.
// This version does the same but is missing an assertion that requires the
// patched Go runtime to work properly.
func WhenDone(parent context.Context, done WhenDoneFunc) bool {
	if parent.Done() == nil {
		return false
	}

	propagateCancel(parent, done)
	return true
}
