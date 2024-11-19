// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bazel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInTestWrapper ensures that non-initialization test code built & run with
// Bazel should not execute under the Bazel-generated wrapper.
func TestInTestWrapper(t *testing.T) {
	if InBazelTest() {
		require.True(t, BuiltWithBazel())
		require.False(t, InTestWrapper())
	}
}
