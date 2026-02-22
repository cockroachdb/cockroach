// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
)

// TestExtend tests the roachprod extend command. Extend adds a duration to
// the cluster's current lifetime (it does not replace it). The --lifetime flag
// uses Go's time.ParseDuration, so the largest unit is hours (e.g. "24h"),
// not days ("1d" is invalid).
func TestExtend(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	initialLifetime := 1 * time.Hour

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "1",
		"--clouds", "gce",
		"--lifetime", initialLifetime.String(),
	)

	rpt.AssertClusterLifetime(initialLifetime)

	// Pick a random extension duration.
	extensions := []time.Duration{30 * time.Minute, 1 * time.Hour, 2 * time.Hour}
	extension := extensions[rpt.Rand().Intn(len(extensions))]
	t.Logf("Extending cluster by %s (seed=%d)", extension, rpt.Seed())

	rpt.RunExpectSuccess("extend", rpt.ClusterName(),
		fmt.Sprintf("--lifetime=%s", extension.String()),
	)

	rpt.InvalidateClusterCache()
	rpt.AssertClusterLifetime(initialLifetime + extension)
}
