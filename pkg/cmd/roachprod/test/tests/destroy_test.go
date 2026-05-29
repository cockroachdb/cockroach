// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
	"github.com/stretchr/testify/require"
)

// TestDestroy tests the roachprod destroy command. It creates a cluster,
// explicitly destroys it by name, and verifies the cluster no longer appears
// in roachprod list.
//
// WARNING: NEVER use --all-mine or -m flags with destroy. In CI, that would
// destroy all TeamCity clusters.
func TestDestroy(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t,
		framework.WithTimeout(10*time.Minute),
		framework.DisableCleanup(),
	)

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "1",
		"--clouds", "gce",
		"--lifetime", "1h",
	)

	rpt.AssertClusterExists()

	// Destroy the cluster by name.
	rpt.RunExpectSuccess("destroy", rpt.ClusterName())

	// Verify the cluster is gone. Use Run (not RunExpectSuccess) since
	// list succeeds even when the cluster isn't found.
	result := rpt.Run("list", "--json", "--pattern", rpt.ClusterName())
	require.True(t, result.Success(), "roachprod list failed: %v", result.Err)
	require.False(t, strings.Contains(result.Stdout, rpt.ClusterName()),
		"Cluster %s should not appear in list after destroy", rpt.ClusterName())
}
