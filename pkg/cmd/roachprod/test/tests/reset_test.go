// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
	"github.com/stretchr/testify/require"
)

// TestReset tests the roachprod reset command, which performs a hard VM
// power cycle. It verifies that after a reset, the VM comes back up and
// ephemeral state (/tmp) is cleared.
//
// NOTE: reset supports targeting a subset of nodes (e.g., cluster:1-3,8-9)
// but we only test resetting the entire cluster here.
func TestReset(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(15*time.Minute))

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "1",
		"--clouds", "gce",
		"--lifetime", "1h",
	)

	// Write a marker file to /tmp before reset.
	rpt.RunOnNodes("1", "touch /tmp/before-reset")
	rpt.RunOnNodes("1", "test -f /tmp/before-reset")

	// Hard reset the VM.
	rpt.RunExpectSuccess("reset", rpt.ClusterName())

	// Wait for SSH to become available again.
	rpt.WaitForSSH(5 * time.Minute)

	// Verify the VM is up.
	rpt.RunOnNodes("1", "uptime")

	// Verify /tmp was cleared by the reboot.
	result := rpt.Run("run", rpt.ClusterName()+":1", "--", "test -f /tmp/before-reset")
	require.False(t, result.Success(),
		"/tmp/before-reset should not exist after reset")
}
