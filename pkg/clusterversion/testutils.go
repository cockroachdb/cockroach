// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
