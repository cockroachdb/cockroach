// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

// TestingBinaryVersion is a binary version that tests can use when they don't
// want to go through a Settings object.
var TestingBinaryVersion = binaryVersion

// TestingBinaryMinSupportedVersion is a minimum supported version that
// tests can use when they don't want to go through a Settings object.
var TestingBinaryMinSupportedVersion = binaryMinSupportedVersion

// TestingClusterVersion is a ClusterVersion that tests can use when they don't
// want to go through a Settings object.
var TestingClusterVersion = ClusterVersion{
	Version: TestingBinaryVersion,
}
