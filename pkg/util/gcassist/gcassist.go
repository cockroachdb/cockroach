// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package gcassist provides control over the Go runtime's GC assist mechanism.
// It relies on CockroachDB's forked Go runtime which exposes
// runtime.SetGCAssistEnabled and runtime.GCAssistEnabled. Under vanilla Go
// (non-Bazel builds), these are no-ops.
package gcassist

// SetEnabled enables or disables the Go runtime's GC assist.
// Under vanilla Go this is a no-op.
func SetEnabled(v bool) {
	setEnabled(v)
}

// Enabled reports whether GC assist is currently enabled.
// Under vanilla Go this always returns true.
func Enabled() bool {
	return enabled()
}
