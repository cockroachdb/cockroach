// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// If a stock/off-the-shelf Go SDK is used instead of the bazel-managed SDK
// pulled from Cockroach's fork of Go, yielding, which relies on our fork's
// runtime extensions, is a noop.
//go:build !bazel

package admission

func runtimeYield() {
}
