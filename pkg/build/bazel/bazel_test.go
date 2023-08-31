// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bazel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTestWrapped ensures that non-initialization test code built & run with
// Bazel should not execute under the Bazel-generated wrapper.
func TestTestWrapped(t *testing.T) {
	require.Equal(t, BuiltWithBazel(), TestByBazel())

	if TestByBazel() {
		require.False(t, TestWrapped())
	}
}
