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

// TestPopulateEtcHosts tests the roachprod populate-etc-hosts command.
// It creates a cluster with 1-3 nodes and verifies that after running
// populate-etc-hosts, /etc/hosts on node 1 contains the private IPs
// of all nodes in the cluster.
func TestPopulateEtcHosts(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
	)

	rpt.RunExpectSuccess("populate-etc-hosts", rpt.ClusterName())

	info := rpt.GetClusterInfo()
	require.Len(t, info.VMs, numNodes)

	// Verify /etc/hosts on node 1 contains the private IPs of all nodes.
	result := rpt.RunOnNodes("1", "cat /etc/hosts")
	hosts := result.Stdout
	for _, vm := range info.VMs {
		require.True(t, strings.Contains(hosts, vm.PrivateIP),
			"/etc/hosts on node 1 missing private IP %s for %s", vm.PrivateIP, vm.Name)
	}
}
