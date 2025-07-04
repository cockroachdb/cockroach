// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package mmaprototype contains a part of a prototype for the multi-metric
// allocator. The code originated in the pkg/kv/kvserver/allocator/mma package
// of #142138.
//
// It is checked in to enable productionization of the multi-metric allocator
// while simultaneously allowing experimentation using a smaller (and thus less
// unwieldy) prototype branch (which necessarily needs to more tightly integrate
// with production code).
//
// See: https://github.com/cockroachdb/cockroach/pull/142138
package mmaprototype
