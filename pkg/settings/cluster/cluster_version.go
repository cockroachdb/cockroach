// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cluster

import "github.com/cockroachdb/cockroach/pkg/roachpb"

var (
	// BinaryMinimumSupportedVersion is the earliest version of data supported
	// by this binary. If this binary is started using a store that has data
	// marked with an earlier version than BinaryMinimumSupportedVersion, then
	// the binary will exit with an error.
	BinaryMinimumSupportedVersion = VersionBase

	// BinaryServerVersion is the version of this binary.
	//
	// NB: This is the version that a new cluster will use when created.
	BinaryServerVersion = VersionMeta2Splits
)

// List all historical versions here in reverse chronological order, with
// comments describing what backwards-incompatible features were introduced.
//
// NB: when adding a version, don't forget to bump ServerVersion above (and
// perhaps MinimumSupportedVersion, if necessary). Note that the version
// upgrade process requires the versions as seen by a cluster to be
// monotonic. Once we've added 1.1-2 (VersionMVCCNetworkStats), we can't go
// back and add 1.0-4 (VersionFixSomeCriticalBug) because clusters running
// 1.1-2 can't coordinate the switch over to the functionality added by
// 1.0-4. Such clusters would need to be wiped. As a result, we recommend not
// bumping to a new minor version until the prior 1.X.0 release has been
// performed.
var (
	// VersionMeta2Splits is is https://github.com/cockroachdb/cockroach/pull/18970.
	VersionMeta2Splits = roachpb.Version{Major: 1, Minor: 1, Unstable: 3}

	// VersionMVCCNetworkStats is https://github.com/cockroachdb/cockroach/pull/18828.
	VersionMVCCNetworkStats = roachpb.Version{Major: 1, Minor: 1, Unstable: 2}

	// VersionRaftLastIndex is https://github.com/cockroachdb/cockroach/pull/18717.
	VersionRaftLastIndex = roachpb.Version{Major: 1, Minor: 1, Unstable: 1}

	// VersionStatsBasedRebalancing is https://github.com/cockroachdb/cockroach/pull/16878.
	VersionStatsBasedRebalancing = roachpb.Version{Major: 1, Minor: 0, Unstable: 3}

	// VersionSplitHardStateBelowRaft is https://github.com/cockroachdb/cockroach/pull/17051.
	VersionSplitHardStateBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 2}

	// VersionRaftLogTruncationBelowRaft is https://github.com/cockroachdb/cockroach/pull/16993.
	VersionRaftLogTruncationBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 1}

	// VersionBase corresponds to any binary older than 1.0-1,
	// though these binaries won't know anything about the mechanism in which
	// this version is used.
	VersionBase = roachpb.Version{Major: 1}
)

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (cv ClusterVersion) IsActive(v roachpb.Version) bool {
	return !cv.UseVersion.Less(v)
}
