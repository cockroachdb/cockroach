// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package buildutil

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/stretchr/testify/require"
)

func TestCrdbTestOn(t *testing.T) {
	// Sanity-check: make sure CrdbTestBuild is set. This should be true for
	// any test built with bazel.
	if bazel.BuiltWithBazel() {
		require.True(t, CrdbTestBuild)
	}
}
