// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// See grunning.Supported() for an explanation behind this build tag.
//
//go:build !((linux && s390x) || !bazel)
// +build !linux !s390x
// +build bazel

package grunning

import _ "unsafe" // for go:linkname

// grunningnanos returns the running time observed by the current goroutine by
// linking to a private symbol in the (patched) runtime package.
//
//go:linkname grunningnanos runtime.grunningnanos
func grunningnanos() int64

func supported() bool { return true }
