// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file contains stub definitions in the event that we do not have access
// to the internal goroutine machinery. This would be because we are using an
// upstream Go (not our fork) and are running a version after 1.23.
//
//go:build !bazel

package goschedstats

import "github.com/cockroachdb/cockroach/pkg/settings/cluster"

// CumulativeNormalizedRunnableGoroutines returns 0.0.
func CumulativeNormalizedRunnableGoroutines() float64 {
	return 0.0
}

// RegisterRunnableCountCallback does nothing and returns -1.
func RegisterRunnableCountCallback(cb RunnableCountCallback) (id int64) {
	return -1
}

// UnregisterRunnableCountCallback is a no-op.
func UnregisterRunnableCountCallback(id int64) {}

// RegisterSettings is a no-op.
func RegisterSettings(st *cluster.Settings) {}
