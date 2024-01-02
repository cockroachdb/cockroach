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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

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
func SkipWhenMinSupportedVersionIsAtLeast(t skip.SkippableTest, major, minor int) {
	t.Helper()
	v := roachpb.Version{Major: int32(major), Minor: int32(minor)}
	if MinSupported.Version().AtLeast(v) {
		skip.IgnoreLint(t, "test disabled when MinVersion >= %d.%d", major, minor)
	}
}
