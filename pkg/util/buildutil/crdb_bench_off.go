// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !crdb_bench || crdb_bench_off

// Package buildutil provides a constant CrdbBenchBuild.
package buildutil

// CrdbBenchBuild is a flag that is set to true if the binary was compiled
// with the 'crdb_bench' build tag (which is the case for all benchmark targets).
const CrdbBenchBuild = false
