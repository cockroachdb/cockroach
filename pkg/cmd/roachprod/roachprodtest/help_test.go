// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRoachprodHelp verifies the roachprod --help command works
func TestRoachprodHelp(t *testing.T) {
	rpt := NewRoachprodTest(t, DisableCleanup())

	result := rpt.Run("--help")

	require.Equal(t, 0, result.ExitCode, "roachprod --help should succeed")
	require.Contains(t, result.Stdout, "roachprod", "Output should mention roachprod")
}
