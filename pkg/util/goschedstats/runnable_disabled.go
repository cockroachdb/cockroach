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

const supported = false

func cumulativeNormalizedRunnableGoroutines() float64 {
	return 0.0
}

func registerRunnableCountCallback(cb RunnableCountCallback) (id int64) {
	panic("registerRunnableCountCallback must not be called when goschedstats.Supported is false")
}

func unregisterRunnableCountCallback(id int64) {}

func registerSettings(st *cluster.Settings) {}
