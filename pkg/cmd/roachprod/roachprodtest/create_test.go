// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

// TestCloudCreate tests creating a cloud cluster with a basic configuration
func TestCloudCreate(t *testing.T) {
	t.Parallel()
	rpt := NewRoachprodTest(t, WithTimeout(10*time.Minute))

	// Create a 3-node GCE cluster
	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "3",
		"--clouds", "gce",
		"--lifetime", "1h",
	)

	// Verify cluster exists and has correct configuration
	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(3)
	rpt.AssertClusterCloud("gce")
}

// TestCloudCreateRandomized tests creating clusters with randomized GCE options
func TestCloudCreateRandomized(t *testing.T) {
	t.Parallel()
	rpt := NewRoachprodTest(t, WithTimeout(15*time.Minute))

	// Generate random GCE create options using the test's seeded RNG
	opts := RandomGCECreateOptions(rpt.Rand())
	t.Logf("Creating cluster with random options (seed=%d): %s", rpt.Seed(), opts.String())

	// Create cluster with randomized options
	args := opts.ToCreateArgs(rpt.ClusterName())
	rpt.RunExpectSuccess(args...)

	// Verify cluster exists with correct configuration
	rpt.AssertClusterExists()
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterNodeCount(opts.NumNodes)

	// Type assert to GCE options for verification
	gceOpts := opts.ProviderOpts.(*gce.ProviderOpts)
	rpt.AssertClusterMachineType(gceOpts.MachineType)
	if len(gceOpts.Zones) > 0 {
		// If specific zones were requested, verify first node is in one of them
		info := rpt.GetClusterInfo()
		require.NotEmpty(t, info.VMs, "Cluster should have VMs")
		require.Contains(t, gceOpts.Zones, info.VMs[0].Zone,
			"VM zone %s should be in requested zones %v", info.VMs[0].Zone, gceOpts.Zones)
	}

	// Verify we can get cluster status
	status := rpt.Status()
	require.True(t, status.Success(), "Status should succeed")
}

// TestCloudCreateWithSpecificZone tests creating a cluster in a specific GCE zone
// and verifies all VMs are created in the correct zone
func TestCloudCreateWithSpecificZone(t *testing.T) {
	t.Parallel()
	rpt := NewRoachprodTest(t, WithTimeout(10*time.Minute))

	// Get the list of potential zones from roachprod GCE provider implementation
	// This list is small, is there a benefit to expanding it?
	validZones := gce.DefaultZones(string(vm.ArchAMD64), true)

	// Randomly pick a zone from the valid zones
	zone := validZones[rpt.Rand().Intn(len(validZones))]
	t.Logf("Testing with randomly selected zone: %s", zone)

	// Create cluster with specific zone
	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "3",
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-zones", zone,
	)

	// Verify cluster is in the correct zone
	rpt.AssertClusterZone(zone)

	// Also verify we got the right number of nodes
	rpt.AssertClusterNodeCount(3)

	// And verify it's on GCE
	rpt.AssertClusterCloud("gce")
}

// TestCloudCreateWithZoneCounts tests creating a cluster using the AZ:N zone format
// where N specifies how many nodes should be in each zone
func TestCloudCreateWithZoneCounts(t *testing.T) {
	t.Parallel()
	rpt := NewRoachprodTest(t, WithTimeout(10*time.Minute))

	// Get valid zones and randomly select 2-3 zones
	validZones := gce.DefaultZones(string(vm.ArchAMD64), true)
	numZones := 2 + rpt.Rand().Intn(2) // 2 or 3 zones

	// Shuffle and pick first numZones
	selectedZones := make([]string, len(validZones))
	copy(selectedZones, validZones)
	rpt.Rand().Shuffle(len(selectedZones), func(i, j int) {
		selectedZones[i], selectedZones[j] = selectedZones[j], selectedZones[i]
	})
	selectedZones = selectedZones[:numZones]

	// Assign random node counts to each zone (1-3 nodes per zone)
	zoneCounts := make(map[string]int)
	totalNodes := 0
	zoneArgs := make([]string, numZones)
	for i, zone := range selectedZones {
		count := 1 + rpt.Rand().Intn(3) // 1-3 nodes
		zoneCounts[zone] = count
		totalNodes += count
		zoneArgs[i] = fmt.Sprintf("%s:%d", zone, count)
	}

	zonesArg := fmt.Sprintf("%s", zoneArgs[0])
	for i := 1; i < len(zoneArgs); i++ {
		zonesArg += "," + zoneArgs[i]
	}

	t.Logf("Creating cluster with zone counts: %s (total nodes: %d)", zonesArg, totalNodes)

	// Create cluster with zone counts
	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", totalNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-zones", zonesArg,
	)

	// Verify cluster has the correct total number of nodes
	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(totalNodes)
	rpt.AssertClusterCloud("gce")

	// Verify node distribution across zones
	info := rpt.GetClusterInfo()
	actualZoneCounts := make(map[string]int)
	for _, vm := range info.VMs {
		actualZoneCounts[vm.Zone]++
	}

	t.Logf("Expected zone distribution: %v", zoneCounts)
	t.Logf("Actual zone distribution: %v", actualZoneCounts)

	// Verify each zone has the expected number of nodes
	for zone, expectedCount := range zoneCounts {
		actualCount := actualZoneCounts[zone]
		require.Equal(t, expectedCount, actualCount,
			"Zone %s should have %d nodes, but has %d", zone, expectedCount, actualCount)
	}

	// Verify no unexpected zones
	require.Equal(t, len(zoneCounts), len(actualZoneCounts),
		"Should have nodes in exactly %d zones, but found %d", len(zoneCounts), len(actualZoneCounts))
}
