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

// TestManaged tests the managed instance group lifecycle: create, grow, and
// shrink. Managed instance groups use a completely separate creation path from
// the standard and SDK paths, and grow/shrink are only supported on managed
// clusters.
//
// TODO: No coverage for --gce-managed-dns-zone and --gce-managed-dns-domain.
// These default to roachprod-managed and roachprod-managed.crdb.io
// respectively. Testing non-default values would require a separate Cloud DNS
// zone provisioned in GCP.
func TestManaged(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(15*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating managed cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	// Create
	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-managed",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterHasLabel("managed", "true")

	// Grow
	growBy := 1 + rpt.Rand().Intn(2)
	t.Logf("Growing cluster by %d nodes", growBy)

	rpt.RunExpectSuccess("grow", rpt.ClusterName(), fmt.Sprintf("%d", growBy))

	rpt.InvalidateClusterCache()
	rpt.AssertClusterNodeCount(numNodes + growBy)

	// Shrink
	shrinkBy := 1 + rpt.Rand().Intn(growBy)
	t.Logf("Shrinking cluster by %d nodes", shrinkBy)

	rpt.RunExpectSuccess("shrink", rpt.ClusterName(), fmt.Sprintf("%d", shrinkBy))

	rpt.InvalidateClusterCache()
	rpt.AssertClusterNodeCount(numNodes + growBy - shrinkBy)
}

// TestLoadBalancer tests load balancer lifecycle on a managed instance group.
// Load balancers are only supported on MIG clusters (--gce-managed). The test
// creates a LB, then cross-references the output of load-balancer list,
// load-balancer ip, and load-balancer pgurl to verify they agree on the IP.
// Cockroach does not need to be running; these commands create and query cloud
// infrastructure independently.
func TestLoadBalancer(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(15*time.Minute))

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "1",
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-managed",
	)

	// Create a load balancer for the default service (system SQL port).
	rpt.RunExpectSuccess("load-balancer", "create", rpt.ClusterName())

	// List load balancers and verify at least one exists.
	listResult := rpt.RunExpectSuccess("load-balancer", "list", rpt.ClusterName())
	require.False(t, strings.Contains(listResult.Stdout, "No load balancers found"),
		"expected at least one load balancer after create")
	listOutput := strings.TrimSpace(listResult.Stdout)
	require.NotEmpty(t, listOutput, "load-balancer list returned empty output")

	// Get the LB IP and verify it appears in the list output.
	ipResult := rpt.RunExpectSuccess("load-balancer", "ip", rpt.ClusterName())
	lbIP := strings.TrimSpace(ipResult.Stdout)
	require.NotEmpty(t, lbIP, "load-balancer ip returned empty output")
	require.True(t, strings.Contains(listOutput, lbIP),
		"load-balancer list output %q does not contain IP %q", listOutput, lbIP)

	// Get the pgurl and verify it contains the LB IP.
	pgResult := rpt.RunExpectSuccess("load-balancer", "pgurl", rpt.ClusterName(),
		"--insecure",
	)
	pgurl := strings.TrimSpace(pgResult.Stdout)
	require.True(t, strings.Contains(pgurl, lbIP),
		"load-balancer pgurl %q does not contain IP %q", pgurl, lbIP)
}
