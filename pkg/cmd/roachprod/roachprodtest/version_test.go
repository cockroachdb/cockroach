// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRoachprodVersion verifies the roachprod version command works
func TestRoachprodVersion(t *testing.T) {
	rpt := NewRoachprodTest(t, DisableCleanup())

	result := rpt.Run("version")

	require.Equal(t, 0, result.ExitCode, "roachprod version should succeed")
}
