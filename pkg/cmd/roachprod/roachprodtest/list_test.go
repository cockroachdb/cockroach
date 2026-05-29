// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

// TestCloudList tests listing clusters
// Commenting because this just makes the log unusable rn...
//func TestCloudList(t *testing.T) {
//	t.Parallel()
//	rpt := NewRoachprodTest(t, WithTimeout(10*time.Minute))
//
//	// Create cluster
//	rpt.RunExpectSuccess("create", rpt.ClusterName(),
//		"-n", "3",
//		"--clouds", "gce",
//		"--lifetime", "1h",
//	)
//
//	// Test basic list
//	result := rpt.ListClusters()
//	require.True(t, result.Success(), "List should succeed")
//	require.Contains(t, result.Stdout, rpt.ClusterName(), "List should show our cluster")
//
//	// Test list with --mine flag
//	result = rpt.Run("list", "--mine")
//	require.True(t, result.Success(), "List --mine should succeed")
//
//	// Test list with --json flag
//	result = rpt.Run("list", "--json")
//	require.True(t, result.Success(), "List --json should succeed")
//	require.Contains(t, result.Stdout, rpt.ClusterName(), "JSON list should show our cluster")
//}
