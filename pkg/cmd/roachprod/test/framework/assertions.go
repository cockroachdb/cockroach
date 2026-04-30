// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// assertions.go provides cluster state assertions (node count, zone, machine
// type, architecture, storage, labels, etc.) backed by a cached result from
// GetClusterInfo(). Call InvalidateClusterCache after operations that modify
// cluster state.
package framework

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// AssertClusterZone verifies all VMs in the cluster are in the expected zone
func (tc *RoachprodTest) AssertClusterZone(expectedZone string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expectedZone, vm.Zone,
			"VM %s is in zone %s, expected %s", vm.Name, vm.Zone, expectedZone)
	}
}

// AssertClusterNodeCount verifies the cluster has the expected number of nodes
func (tc *RoachprodTest) AssertClusterNodeCount(expectedCount int) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.Len(tc.t, info.VMs, expectedCount,
		"Cluster has %d nodes, expected %d", len(info.VMs), expectedCount)
}

// AssertClusterMachineType verifies all VMs use the expected machine type
func (tc *RoachprodTest) AssertClusterMachineType(expectedType string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expectedType, vm.MachineType,
			"VM %s has machine type %s, expected %s", vm.Name, vm.MachineType, expectedType)
	}
}

// AssertClusterCloud verifies the cluster is on the expected cloud provider
func (tc *RoachprodTest) AssertClusterCloud(expectedCloud string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.CloudProviders, "Cluster has no cloud providers")

	// CloudProviders is an array like ["gce-cockroach-ephemeral"]
	// We check if the expected cloud is a prefix of any provider
	found := false
	for _, provider := range info.CloudProviders {
		if strings.HasPrefix(provider, expectedCloud) {
			found = true
			break
		}
	}
	require.True(tc.t, found,
		"Cluster cloud providers %v do not include %s", info.CloudProviders, expectedCloud)
}

// AssertClusterArchitecture verifies all VMs have the expected architecture
func (tc *RoachprodTest) AssertClusterArchitecture(expectedArch string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expectedArch, string(vm.CPUArch),
			"VM %s has architecture %s, expected %s", vm.Name, vm.CPUArch, expectedArch)
	}
}

// AssertClusterExists verifies the cluster appears in roachprod list
func (tc *RoachprodTest) AssertClusterExists() {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.Equal(tc.t, tc.clusterName, info.Name,
		"Cluster name mismatch: got %s, expected %s", info.Name, tc.clusterName)
	require.NotEmpty(tc.t, info.VMs, "Cluster exists but has no VMs")
}

// AssertClusterLifetime verifies the cluster has the expected lifetime
func (tc *RoachprodTest) AssertClusterLifetime(expectedLifetime time.Duration) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.Equal(tc.t, expectedLifetime, info.Lifetime,
		"Cluster has lifetime %s, expected %s", info.Lifetime, expectedLifetime)
}

// AssertClusterPreemptible verifies all VMs have the expected preemptible status
func (tc *RoachprodTest) AssertClusterPreemptible(expected bool) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expected, vm.Preemptible,
			"VM %s has preemptible=%v, expected %v", vm.Name, vm.Preemptible, expected)
	}
}

// AssertClusterLocalDiskCount verifies all VMs have the expected number of local disks
func (tc *RoachprodTest) AssertClusterLocalDiskCount(expected int) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Len(tc.t, vm.LocalDisks, expected,
			"VM %s has %d local disks, expected %d", vm.Name, len(vm.LocalDisks), expected)
	}
}

// AssertClusterNonBootVolumeCount verifies all VMs have the expected number
// of non-boot persistent volumes
func (tc *RoachprodTest) AssertClusterNonBootVolumeCount(expected int) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Len(tc.t, vm.NonBootAttachedVolumes, expected,
			"VM %s has %d non-boot volumes, expected %d",
			vm.Name, len(vm.NonBootAttachedVolumes), expected)
	}
}

// AssertClusterNonBootVolumeType verifies all non-boot volumes on all VMs
// have the expected volume type (e.g., "pd-ssd", "hyperdisk-balanced").
func (tc *RoachprodTest) AssertClusterNonBootVolumeType(expectedType string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		for _, vol := range vm.NonBootAttachedVolumes {
			require.Equal(tc.t, expectedType, vol.ProviderVolumeType,
				"VM %s volume %s has type %s, expected %s",
				vm.Name, vol.Name, vol.ProviderVolumeType, expectedType)
		}
	}
}

// AssertClusterNonBootVolumeSize verifies all non-boot volumes on all VMs
// have the expected size in GB.
func (tc *RoachprodTest) AssertClusterNonBootVolumeSize(expectedSizeGB int) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		for _, vol := range vm.NonBootAttachedVolumes {
			require.Equal(tc.t, expectedSizeGB, vol.Size,
				"VM %s volume %s has size %dGB, expected %dGB",
				vm.Name, vol.Name, vol.Size, expectedSizeGB)
		}
	}
}

// AssertClusterHasLabel verifies all VMs have the expected label key-value pair
func (tc *RoachprodTest) AssertClusterHasLabel(key, value string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		actual, ok := vm.Labels[key]
		require.True(tc.t, ok,
			"VM %s missing label %q", vm.Name, key)
		require.Equal(tc.t, value, actual,
			"VM %s label %q=%q, expected %q", vm.Name, key, actual, value)
	}
}

// AssertClusterMultiZone verifies VMs are spread across at least minZones
// different zones
func (tc *RoachprodTest) AssertClusterMultiZone(minZones int) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	zones := make(map[string]bool)
	for _, vm := range info.VMs {
		zones[vm.Zone] = true
	}
	require.GreaterOrEqual(tc.t, len(zones), minZones,
		"Cluster spans %d zones, expected at least %d", len(zones), minZones)
}

// AssertClusterFilesystem verifies the filesystem on /mnt/data1 matches the
// expected type by running `df -T` on every node.
func (tc *RoachprodTest) AssertClusterFilesystem(expectedFS string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for i := range info.VMs {
		node := fmt.Sprintf("%d", i+1)
		result := tc.RunOnNodes(node, "df -T /mnt/data1 | tail -1 | awk '{print $2}'")
		require.True(tc.t, result.Success(),
			"Failed to check filesystem on node %s:\nStdout: %s\nStderr: %s",
			node, result.Stdout, result.Stderr)

		// Take the last non-empty line — roachprod may print provider
		// warnings (e.g. IBM) to stdout before the actual command output.
		actual := lastNonEmptyLine(result.Stdout)
		require.Equal(tc.t, expectedFS, actual,
			"Node %s filesystem on /mnt/data1 is %s, expected %s", node, actual, expectedFS)
	}
}

// ========================================
// Randomized config assertions
// ========================================
//
// RandomGCECreateOptions (createconfig.go) co-generates assertion closures
// alongside each randomized setting. Each closure captures the expected
// value and calls the appropriate Assert* method above. These types and
// methods support that mechanism.
//
// Registration (in createconfig.go during config generation):
//
//	providerOpts.MachineType = "n2-standard-4"
//	cfg.expect("machine type = n2-standard-4", func(rpt *RoachprodTest) {
//	    rpt.AssertClusterMachineType("n2-standard-4")
//	})
//
// The closure does not execute here — it is stored and run later when the
// caller invokes cfg.AssertAll(rpt)
//
// Execution (in the test, after cluster creation):
//
//	opts := framework.RandomGCECreateOptions(rpt.Rand())
//	rpt.RunExpectSuccess(opts.ToCreateArgs(rpt.ClusterName())...)
//	opts.AssertAll(rpt) // now all registered closures execute

// clusterAssertion pairs a human-readable description with an assertion
// function that validates a specific cluster property. Assertions are
// registered during config generation and executed later via AssertAll.
type clusterAssertion struct {
	description string
	fn          func(rpt *RoachprodTest)
}

// expect registers an assertion to be run later by AssertAll.
func (cfg *RandomizedClusterConfig) expect(desc string, fn func(rpt *RoachprodTest)) {
	cfg.assertions = append(cfg.assertions, clusterAssertion{description: desc, fn: fn})
}

// AssertAll runs every assertion that was registered during config generation.
// Each assertion is logged before execution for debuggability.
func (cfg *RandomizedClusterConfig) AssertAll(rpt *RoachprodTest) {
	rpt.t.Helper()
	for _, a := range cfg.assertions {
		rpt.t.Logf("Asserting: %s", a.description)
		a.fn(rpt)
	}
}

// ========================================
// Helpers
// ========================================

// lastNonEmptyLine returns the last non-empty line from s. Roachprod
// sometimes prints provider warnings (e.g. IBM authenticator errors)
// to stdout before the actual command output, so we can't assume
// stdout is a single clean line.
func lastNonEmptyLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}
