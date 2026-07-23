// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package revlog reads and writes the revlog: a continuous,
// externally-stored revision log of cluster changes. The on-disk
// format is specified in revlog-format.md.
//
// This package provides the low-level write and read primitives:
// TickWriter for emitting one data file in a tick, WriteTickManifest
// for sealing a tick with its close marker, and LogReader / TickReader
// for discovery and consumption.
package revlog
