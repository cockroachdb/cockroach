// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package errcheck defines an Analyzer that is a slightly modified version
// of the errcheck Analyzer from upstream (github.com/kisielk/errcheck).
// Specifically, we pass in our own list of excludes.

// Package errcheck is intentionally empty when not built under Bazel.

//go:build !bazel

package errcheck

// File intentionally empty.
