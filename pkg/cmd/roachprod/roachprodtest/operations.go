// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import "fmt"

// CreateCluster creates a cluster with the given number of nodes
func (tc *TestOptions) CreateCluster(nodes int) {
	tc.t.Helper()
	tc.RunExpectSuccess("create", tc.clusterName, "-n", fmt.Sprintf("%d", nodes))
}

//// CreateLocalCluster creates a local cluster with the given number of nodes
//func (tc *TestOptions) CreateLocalCluster(nodes int) {
//	tc.t.Helper()
//	// Local clusters must be named 'local' or 'local-<suffix>'
//	localName := fmt.Sprintf("local-%s", tc.clusterName)
//	tc.clusterName = localName
//	tc.RunExpectSuccess("create", tc.clusterName, "-n", fmt.Sprintf("%d", nodes))
//}

// DestroyCluster destroys the cluster
func (tc *TestOptions) DestroyCluster() {
	tc.t.Helper()
	tc.RunExpectSuccess("destroy", tc.clusterName)
}

// destroyCluster is the internal cleanup method
// Warning: NEVER destroy all clusters belonging to current users
// i.e. with -m, --all-mine flags
// If run in CI, this will destroy all TeamCity clusters which would be bad
func (tc *TestOptions) destroyCluster() {
	// Use Run instead of RunExpectSuccess to avoid test failures during cleanup
	result := tc.Run("destroy", tc.clusterName)
	if !result.Success() {
		tc.t.Logf("Warning: Failed to destroy cluster during cleanup: %v", result.Err)
	}
}

// StageVersion stages a specific cockroach version
func (tc *TestOptions) StageVersion(version string) {
	tc.t.Helper()
	tc.RunExpectSuccess("stage", tc.clusterName, "cockroach", version)
}

// StageCockroach stages the latest cockroach version
func (tc *TestOptions) StageCockroach() {
	tc.t.Helper()
	tc.RunExpectSuccess("stage", tc.clusterName, "cockroach")
}

// StartCluster starts the cluster
func (tc *TestOptions) StartCluster(extraArgs ...string) {
	tc.t.Helper()
	args := []string{"start", tc.clusterName}
	args = append(args, extraArgs...)
	tc.RunExpectSuccess(args...)
}

// StopCluster stops the cluster
func (tc *TestOptions) StopCluster(extraArgs ...string) {
	tc.t.Helper()
	args := []string{"stop", tc.clusterName}
	args = append(args, extraArgs...)
	tc.RunExpectSuccess(args...)
}

// WipeCluster wipes the cluster data
func (tc *TestOptions) WipeCluster() {
	tc.t.Helper()
	tc.RunExpectSuccess("wipe", tc.clusterName)
}

// ListClusters lists all clusters
func (tc *TestOptions) ListClusters() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("list")
}

// Status gets the status of the cluster
func (tc *TestOptions) Status() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("status", tc.clusterName)
}

// SQL executes a SQL command on the cluster
func (tc *TestOptions) SQL(nodeSpec string, query string) *RunResult {
	tc.t.Helper()
	target := fmt.Sprintf("%s:%s", tc.clusterName, nodeSpec)
	return tc.RunExpectSuccess("sql", target, "--", "-e", query)
}

// GetAdminURL gets the admin URL for the cluster
func (tc *TestOptions) GetAdminURL() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("adminurl", tc.clusterName)
}

// GetPGURL gets the postgres URL for the cluster
func (tc *TestOptions) GetPGURL() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("pgurl", tc.clusterName)
}

// GetIP gets the IP addresses for the cluster
func (tc *TestOptions) GetIP() *RunResult {
	tc.t.Helper()
	return tc.RunExpectSuccess("ip", tc.clusterName)
}

// RunOnNodes executes a command on cluster nodes
func (tc *TestOptions) RunOnNodes(nodeSpec string, command string) *RunResult {
	tc.t.Helper()
	target := fmt.Sprintf("%s:%s", tc.clusterName, nodeSpec)
	return tc.RunExpectSuccess("run", target, "--", command)
}

// PutFile uploads a file to the cluster
func (tc *TestOptions) PutFile(src string, dest string) {
	tc.t.Helper()
	if dest == "" {
		tc.RunExpectSuccess("put", tc.clusterName, src)
	} else {
		tc.RunExpectSuccess("put", tc.clusterName, src, dest)
	}
}

// GetFile downloads a file from the cluster
func (tc *TestOptions) GetFile(src string, dest string) {
	tc.t.Helper()
	tc.RunExpectSuccess("get", tc.clusterName, src, dest)
}
