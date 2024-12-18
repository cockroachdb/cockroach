// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import "github.com/cockroachdb/cockroach/pkg/testutils/skip"

// TestingClusterVersion is a ClusterVersion that tests can use when they don't
// want to go through a Settings object.
var TestingClusterVersion = ClusterVersion{
	Version: Latest.Version(),
}

// SkipWhenMinSupportedVersionIsAtLeast skips this test if MinSupported is >=
// the given version.
//
// Used for upgrade tests that require support for a previous version; it allows
// experimenting with bumping MinSupported and limiting how many things must be
// fixed in the same PR that bumps it.
func SkipWhenMinSupportedVersionIsAtLeast(t skip.SkippableTest, v Key) {
	t.Helper()
	if MinSupported.Version().AtLeast(v.Version()) {
		skip.IgnoreLintf(t, "test disabled when MinVersion >= %s", v)
	}
}
