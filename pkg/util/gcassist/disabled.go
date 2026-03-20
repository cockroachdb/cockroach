// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !bazel

package gcassist

func setEnabled(v bool) {}
func enabled() bool     { return true }
