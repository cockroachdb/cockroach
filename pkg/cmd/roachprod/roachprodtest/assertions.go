// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/stretchr/testify/require"
)

// GetClusterInfo returns cached cluster information or fetches it if the cache is stale.
// To force a fresh fetch, use RefreshClusterInfo() or InvalidateClusterCache() first.
func (tc *TestOptions) GetClusterInfo() *types.Cluster {
	tc.t.Helper()

	if tc.clusterInfoCache != nil {
		return tc.clusterInfoCache
	}
	return tc.RefreshClusterInfo()
}

// RefreshClusterInfo fetches fresh cluster information from roachprod and caches it.
// Use this when you need to ensure you have the latest cluster state.
func (tc *TestOptions) RefreshClusterInfo() *types.Cluster {
	tc.t.Helper()

	result := tc.Run("list", "--json", "--pattern", tc.clusterName)
	require.True(tc.t, result.Success(),
		"Failed to get cluster info:\nStdout: %s\nStderr: %s\nError: %v",
		result.Stdout, result.Stderr, result.Err)

	// roachprod list may write error/warning messages to stdout before the JSON.
	// Find where the JSON actually starts (first '{' character).
	// TODO: this is fragile, replace with something more robust later
	jsonStart := 0
	for i, ch := range result.Stdout {
		if ch == '{' {
			jsonStart = i
			break
		}
	}

	jsonData := result.Stdout[jsonStart:]

	var listOutput cloud.Cloud
	err := json.Unmarshal([]byte(jsonData), &listOutput)
	require.NoError(tc.t, err, "Failed to parse cluster info JSON: %s", jsonData)

	// Extract our cluster from the map
	clusterInfo, ok := listOutput.Clusters[tc.clusterName]
	require.True(tc.t, ok, "Cluster %s not found in list output", tc.clusterName)

	// Cache the result
	tc.clusterInfoCache = clusterInfo
	return clusterInfo
}

// InvalidateClusterCache marks the cached cluster information as stale.
// The next call to GetClusterInfo() will fetch fresh data from roachprod.
// Call this after operations that modify cluster state (e.g., extend, start, stop).
func (tc *TestOptions) InvalidateClusterCache() {
	tc.clusterInfoCache = nil
}

// AssertClusterZone verifies all VMs in the cluster are in the expected zone
func (tc *TestOptions) AssertClusterZone(expectedZone string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expectedZone, vm.Zone,
			"VM %s is in zone %s, expected %s", vm.Name, vm.Zone, expectedZone)
	}
}

// AssertClusterNodeCount verifies the cluster has the expected number of nodes
func (tc *TestOptions) AssertClusterNodeCount(expectedCount int) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.Len(tc.t, info.VMs, expectedCount,
		"Cluster has %d nodes, expected %d", len(info.VMs), expectedCount)
}

// AssertClusterMachineType verifies all VMs use the expected machine type
func (tc *TestOptions) AssertClusterMachineType(expectedType string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expectedType, vm.MachineType,
			"VM %s has machine type %s, expected %s", vm.Name, vm.MachineType, expectedType)
	}
}

// AssertClusterCloud verifies the cluster is on the expected cloud provider
func (tc *TestOptions) AssertClusterCloud(expectedCloud string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.CloudProviders, "Cluster has no cloud providers")

	// CloudProviders is an array like ["gce-cockroach-ephemeral"]
	// We check if the expected cloud is a prefix of any provider
	found := false
	for _, provider := range info.CloudProviders {
		if len(provider) >= len(expectedCloud) && provider[:len(expectedCloud)] == expectedCloud {
			found = true
			break
		}
	}
	require.True(tc.t, found,
		"Cluster cloud providers %v do not include %s", info.CloudProviders, expectedCloud)
}

// AssertClusterArchitecture verifies all VMs have the expected architecture
func (tc *TestOptions) AssertClusterArchitecture(expectedArch string) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.NotEmpty(tc.t, info.VMs, "Cluster has no VMs")

	for _, vm := range info.VMs {
		require.Equal(tc.t, expectedArch, string(vm.CPUArch),
			"VM %s has architecture %s, expected %s", vm.Name, vm.CPUArch, expectedArch)
	}
}

// AssertClusterExists verifies the cluster appears in roachprod list
func (tc *TestOptions) AssertClusterExists() {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.Equal(tc.t, tc.clusterName, info.Name,
		"Cluster name mismatch: got %s, expected %s", info.Name, tc.clusterName)
	require.NotEmpty(tc.t, info.VMs, "Cluster exists but has no VMs")
}

// AssertClusterLifetime verifies the cluster has the expected lifetime
func (tc *TestOptions) AssertClusterLifetime(expectedLifetime time.Duration) {
	tc.t.Helper()

	info := tc.GetClusterInfo()
	require.Equal(tc.t, expectedLifetime, info.Lifetime,
		"Cluster has lifetime %s, expected %s", info.Lifetime, expectedLifetime)
}
