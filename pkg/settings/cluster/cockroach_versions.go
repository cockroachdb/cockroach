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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// VersionKey is a unique identifier for a version of CockroachDB.
type VersionKey int

// Version constants. To add a version:
// - add it at the end of this block
// - add it at the end of the `Versions` block below
// - (typically only applies to major and minor releases): bump BinaryMinimumSupportedVersion.
//   For example, when introducing the `1.4` release, BinaryMinimumSupportedVersion would increase to `1.3`.
const (
	VersionBase VersionKey = iota
	VersionRaftLogTruncationBelowRaft
	VersionSplitHardStateBelowRaft
	VersionStatsBasedRebalancing
	Version1_1
	VersionRaftLastIndex
	VersionMVCCNetworkStats
	VersionMeta2Splits

	// Add new versions here (step one of two)

)

// Versions lists all historical versions here in chronological order, with comments describing what
// backwards-incompatible features were introduced.
//
// NB: The version upgrade process requires the versions as seen by a cluster to be monotonic. Once
// we've added 1.1-2 (VersionMVCCNetworkStats), we can't go back and add 1.0-4
// (VersionFixSomeCriticalBug) because clusters running 1.1-2 can't coordinate the switch over to
// the functionality added by 1.0-4. Such clusters would need to be wiped. As a result, we recommend
// not bumping to a new minor version until the prior 1.X.0 release has been performed.
var versionsSingleton = keyedVersions([]keyedVersion{
	{
		// VersionBase corresponds to any binary older than 1.0-1, though these binaries won't know
		// anything about the mechanism in which this version is used.
		Key:     VersionBase,
		Version: roachpb.Version{Major: 1},
	},
	{
		// VersionRaftLogTruncationBelowRaft is https://github.com/cockroachdb/cockroach/pull/16993.
		Key:     VersionRaftLogTruncationBelowRaft,
		Version: roachpb.Version{Major: 1, Minor: 0, Unstable: 1},
	},
	{
		// VersionSplitHardStateBelowRaft is https://github.com/cockroachdb/cockroach/pull/17051.
		Key:     VersionSplitHardStateBelowRaft,
		Version: roachpb.Version{Major: 1, Minor: 0, Unstable: 2},
	},
	{
		// VersionStatsBasedRebalancing is https://github.com/cockroachdb/cockroach/pull/16878.
		Key:     VersionStatsBasedRebalancing,
		Version: roachpb.Version{Major: 1, Minor: 0, Unstable: 3},
	},
	{
		// Version1_1 is CockroachDB v1.1 (it remains unchainged for all v1.1.x releases
		// unless we need to introduce a new migration).
		Key:     Version1_1,
		Version: roachpb.Version{Major: 1, Minor: 1},
	},
	{
		// VersionRaftLastIndex is https://github.com/cockroachdb/cockroach/pull/18717.
		Key:     VersionRaftLastIndex,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 1},
	},
	{
		// VersionMVCCNetworkStats is https://github.com/cockroachdb/cockroach/pull/18828.
		Key:     VersionMVCCNetworkStats,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 2},
	},
	{
		// VersionMeta2Splits is https://github.com/cockroachdb/cockroach/pull/18970.
		Key:     VersionMeta2Splits,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 3},
	},

	// Add new versions here (step two of two).

}).Validated()

var (
	// BinaryMinimumSupportedVersion is the earliest version of data supported
	// by this binary. If this binary is started using a store that has data
	// marked with an earlier version than BinaryMinimumSupportedVersion, then
	// the binary will exit with an error.
	BinaryMinimumSupportedVersion = VersionByKey(VersionBase)

	// BinaryServerVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	BinaryServerVersion = versionsSingleton[len(versionsSingleton)-1].Version
)

// VersionByKey returns the roachpb.Version for a given key.
// It is a fatal error to use an invalid key.
func VersionByKey(key VersionKey) roachpb.Version {
	return versionsSingleton.MustByKey(key)
}
