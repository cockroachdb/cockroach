// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
	"github.com/stretchr/testify/require"
)

// TestGrafanaurl tests the roachprod grafanaurl command
func TestGrafanaurl(t *testing.T) {
	rpt := framework.NewRoachprodTest(t, framework.DisableCleanup())

	result := rpt.Run("--help")

	require.Equal(t, 0, result.ExitCode, "roachprod --help should succeed")
	require.Contains(t, result.Stdout, "roachprod", "Output should mention roachprod")
}
