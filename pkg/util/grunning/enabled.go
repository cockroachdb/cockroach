// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// See grunning.Supported() for an explanation behind this build tag.
//
//go:build !(freebsd || (linux && s390x) || !bazel)
// +build !freebsd
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
