// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build bazel

package gcassist

import "runtime"

func setEnabled(v bool) { runtime.SetGCAssistEnabled(v) }
func enabled() bool     { return runtime.GCAssistEnabled() }
