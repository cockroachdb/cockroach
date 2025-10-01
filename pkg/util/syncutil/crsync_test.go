// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file verifies that crlib is set up to use the CRDB Go toolchain when
// built with bazel.
//
//go:build bazel

package syncutil

import (
	"testing"

	"github.com/cockroachdb/crlib/crsync"
)

func TestCRSync(t *testing.T) {
	if !crsync.UsingCockroachGo {
		t.Fatalf("expected crsync.UsingCockroachGo to be true")
	}
}
