// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// See grunning.Supported for an explanation behind this build tag.
//
//go:build !bazel

package grunning

const supported = false

func grunningnanos() int64 { return 0 }
