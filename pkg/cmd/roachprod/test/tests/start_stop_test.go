// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
	"github.com/stretchr/testify/require"
)

// TestStartStop tests the full cockroach process lifecycle:
// create → stage → start → verify running → stop → verify stopped.
//
// Start and stop operate on the CockroachDB process (via systemd/SSH), not the
// VM itself. The VM stays running throughout.
//
// TODO: Not tested here:
//   - --secure mode (certificate distribution)
//   - --restart flag (should be its own test)
//   - --schedule-backups / --schedule-backup-args (SQL feature)
//   - --tag (multi-process tagging)
//   - --racks (CockroachDB locality assignment, not a GCE operation)
//   - --skip-init (skips cockroach init and admin user creation)
//   - --num-files-limit (systemd LimitNOFILE, hard to verify remotely)
func TestStartStop(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(20*time.Minute))

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "1",
		"--clouds", "gce",
		"--lifetime", "1h",
	)

	// Stage the latest cockroach binary.
	rpt.RunExpectSuccess("stage", rpt.ClusterName(), "cockroach")

	// Start cockroach in insecure mode.
	rpt.RunExpectSuccess("start", rpt.ClusterName(), "--insecure")

	// Verify cockroach is running via status.
	statusResult := rpt.RunExpectSuccess("status", rpt.ClusterName())
	require.False(t, strings.Contains(statusResult.Stdout, "not running"),
		"expected cockroach to be running after start, got: %s", statusResult.Stdout)

	// Stop cockroach with a randomly chosen signal.
	sigs := []int{9, 15} // SIGKILL, SIGTERM
	sig := sigs[rpt.Rand().Intn(len(sigs))]

	stopArgs := []string{"stop", rpt.ClusterName(), fmt.Sprintf("--sig=%d", sig)}

	if sig == 15 {
		// SIGTERM: add --wait and optionally a grace period.
		stopArgs = append(stopArgs, "--wait")
		gracePeriods := []int{0, 5, 30}
		gp := gracePeriods[rpt.Rand().Intn(len(gracePeriods))]
		if gp > 0 {
			stopArgs = append(stopArgs, fmt.Sprintf("--grace-period=%d", gp))
			t.Logf("Stopping with SIGTERM, grace-period=%ds (seed=%d)", gp, rpt.Seed())
		} else {
			t.Logf("Stopping with SIGTERM, no grace period (seed=%d)", rpt.Seed())
		}
	} else {
		t.Logf("Stopping with SIGKILL (seed=%d)", rpt.Seed())
	}

	rpt.RunExpectSuccess(stopArgs...)

	// Verify cockroach is no longer running.
	statusResult = rpt.RunExpectSuccess("status", rpt.ClusterName())
	require.True(t, strings.Contains(statusResult.Stdout, "not running"),
		"expected cockroach to be stopped, got: %s", statusResult.Stdout)
}
