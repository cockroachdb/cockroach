// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// operations.go provides convenience wrappers around common roachprod commands
// (create, destroy, start, stop, stage, status, run, put, get, etc.) and
// operational helpers (e.g., WaitForSSH) so tests can call high-level methods
// instead of assembling roachprod CLI argument lists.
package framework

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// CreateCluster creates a cluster with the given number of nodes
func (tc *RoachprodTest) CreateCluster(nodes int) {
	tc.t.Helper()
	tc.RunExpectSuccess("create", tc.clusterName, "-n", fmt.Sprintf("%d", nodes))
}

// DestroyCluster destroys the cluster
func (tc *RoachprodTest) DestroyCluster() {
	tc.t.Helper()
	tc.RunExpectSuccess("destroy", tc.clusterName)
}

// destroyCluster is the internal cleanup method
// Warning: NEVER destroy all clusters belonging to current users
// i.e. with -m, --all-mine flags
// If ran in CI, this will destroy all TeamCity clusters which would be bad
func (tc *RoachprodTest) destroyCluster() {
	// Use Run instead of RunExpectSuccess to avoid test failures during cleanup
	result := tc.Run("destroy", tc.clusterName)
	if !result.Success() {
		tc.t.Logf("Warning: Failed to destroy cluster during cleanup: %v", result.Err)
	}
}

// StageVersion stages a specific cockroach version
func (tc *RoachprodTest) StageVersion(version string) {
	tc.t.Helper()
	tc.RunExpectSuccess("stage", tc.clusterName, "cockroach", version)
}

// StageCockroach stages the latest cockroach version
func (tc *RoachprodTest) StageCockroach() {
	tc.t.Helper()
	tc.RunExpectSuccess("stage", tc.clusterName, "cockroach")
}

// StartCluster starts the cluster
func (tc *RoachprodTest) StartCluster(extraArgs ...string) {
	tc.t.Helper()
	args := []string{"start", tc.clusterName}
	args = append(args, extraArgs...)
	tc.RunExpectSuccess(args...)
}

// StopCluster stops the cluster
func (tc *RoachprodTest) StopCluster(extraArgs ...string) {
	tc.t.Helper()
	args := []string{"stop", tc.clusterName}
	args = append(args, extraArgs...)
	tc.RunExpectSuccess(args...)
}

// WipeCluster wipes the cluster data
func (tc *RoachprodTest) WipeCluster() {
	tc.t.Helper()
	tc.RunExpectSuccess("wipe", tc.clusterName)
}

// ListClusters lists all clusters
func (tc *RoachprodTest) ListClusters() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("list")
}

// Status gets the status of the cluster
func (tc *RoachprodTest) Status() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("status", tc.clusterName)
}

// SQL executes a SQL command on the cluster
func (tc *RoachprodTest) SQL(nodeSpec string, query string) *RunResult {
	tc.t.Helper()
	target := fmt.Sprintf("%s:%s", tc.clusterName, nodeSpec)
	return tc.RunExpectSuccess("sql", target, "--", "-e", query)
}

// GetAdminURL gets the admin URL for the cluster
func (tc *RoachprodTest) GetAdminURL() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("adminurl", tc.clusterName)
}

// GetPGURL gets the postgres URL for the cluster
func (tc *RoachprodTest) GetPGURL() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("pgurl", tc.clusterName)
}

// GetIP gets the IP addresses for the cluster
func (tc *RoachprodTest) GetIP() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("ip", tc.clusterName)
}

// RunOnNodes executes a command on cluster nodes
func (tc *RoachprodTest) RunOnNodes(nodeSpec string, command string) *RunResult {
	tc.t.Helper()
	target := fmt.Sprintf("%s:%s", tc.clusterName, nodeSpec)
	return tc.RunExpectSuccess("run", target, "--", command)
}

// WaitForSSH waits until SSH is available on all nodes by retrying
// `roachprod run <cluster> -- true` until it succeeds or the timeout
// is reached. Useful after operations that restart VMs (e.g., reset).
// Without a node selector, roachprod targets all nodes, so the command
// only succeeds when every node is reachable.
func (tc *RoachprodTest) WaitForSSH(timeout time.Duration) {
	tc.t.Helper()

	deadline := timeutil.Now().Add(timeout)
	target := tc.clusterName
	interval := 10 * time.Second

	// Initial delay to let the VM start rebooting.
	time.Sleep(5 * time.Second)

	for {
		result := tc.Run("run", target, "--", "true")
		if result.Success() {
			tc.t.Logf("SSH is available")
			return
		}
		remaining := time.Until(deadline)
		require.Greater(tc.t, remaining, time.Duration(0),
			"Timed out waiting for SSH after %s: %v", timeout, result.Err)
		tc.t.Logf("SSH not ready, retrying in %s (%s remaining)", interval, remaining.Round(time.Second))
		time.Sleep(interval)
	}
}

// PutFile uploads a file to the cluster
func (tc *RoachprodTest) PutFile(src string, dest string) {
	tc.t.Helper()
	if dest == "" {
		tc.RunExpectSuccess("put", tc.clusterName, src)
	} else {
		tc.RunExpectSuccess("put", tc.clusterName, src, dest)
	}
}

// GetFile downloads a file from the cluster
func (tc *RoachprodTest) GetFile(src string, dest string) {
	tc.t.Helper()
	tc.RunExpectSuccess("get", tc.clusterName, src, dest)
}
