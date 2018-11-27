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

// Version constants.
//
// To add a version:
//   - Add it at the end of this block.
//   - Add it at the end of the `Versions` block below.
//   - For major or minor versions, bump BinaryMinimumSupportedVersion. For
//     example, if introducing the `1.4` release, bump it from `1.2` to `1.3`.
//
// To delete a version.
//   - Remove its associated runtime checks.
//   - Mark it as "unused" when it is only referenced in this file.
//   - If the version key after a major or minor version is unused, remove it
//     and its associated keyedVersion.
//     - Apply recursively.
const (
	VersionBase VersionKey = iota
	VersionRaftLogTruncationBelowRaft
	VersionSplitHardStateBelowRaft
	VersionStatsBasedRebalancing
	Version1_1
	VersionRaftLastIndex
	VersionMVCCNetworkStats
	VersionMeta2Splits // unused
	VersionRPCNetworkStats
	VersionRPCVersionCheck
	VersionClearRange
	VersionPartitioning
	VersionLeaseSequence            // unused
	VersionUnreplicatedTombstoneKey // unused
	VersionRecomputeStats
	VersionNoRaftProposalKeys
	VersionTxnSpanRefresh
	VersionReadUncommittedRangeLookups // unused
	VersionPerReplicaZoneConstraints
	VersionLeasePreferences
	Version2_0
	VersionImportSkipRecords
	VersionProposedTSLeaseRequest
	VersionRangeAppliedStateKey
	VersionImportFormats
	VersionSecondaryLookupJoins
	VersionClientSideWritingFlag
	VersionColumnarTimeSeries
	VersionTxnCoordMetaInvalidField
	VersionAsyncConsensus
	VersionBatchResponse
	VersionCreateChangefeed
	VersionRangeMerges
	VersionBitArrayColumns
	VersionLoadBasedRebalancing
	Version2_1
	VersionCascadingZoneConfigs
	VersionLoadSplits
	VersionExportStorageWorkload

	// Add new versions here (step one of two).

)

// versionsSingleton lists all historical versions here in chronological order,
// with comments describing what backwards-incompatible features were
// introduced.
//
// A roachpb.Version has the colloquial form MAJOR.MINOR[.PATCH][-UNSTABLE],
// where the PATCH and UNSTABLE components can be omitted if zero. Keep in mind
// that a version with an unstable component, like 1.1-2, represents a version
// that was developed AFTER v1.1 was released and is not slated for release
// until the next stable version (either 1.2-0 or 2.0-0). Patch releases, like
// 1.1.2, do not have associated migrations.
//
// NB: The version upgrade process requires the versions as seen by a cluster to
// be monotonic. Once we've added 1.1-0 (Version1_1), we can't slot in 1.0-4
// (VersionFixSomeCriticalBug) because clusters already running 1.1-0 won't
// migrate through the new 1.0-4 version. Such clusters would need to be wiped.
// As a result, do not bump the major or minor version until we are absolutely
// sure that no new migrations will need to be added (i.e., when cutting the
// final release candidate).
var versionsSingleton = keyedVersions([]keyedVersion{
	{
		// VersionBase corresponds to any binary older than 1.0-1, though these
		// binaries predate this cluster versioning system.
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
		// Version1_1 is CockroachDB v1.1. It's used for all v1.1.x patch releases.
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
	{
		// VersionRPCNetworkStats is https://github.com/cockroachdb/cockroach/pull/19897.
		Key:     VersionRPCNetworkStats,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 4},
	},
	{
		// VersionRPCVersionCheck is https://github.com/cockroachdb/cockroach/pull/20587.
		Key:     VersionRPCVersionCheck,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 5},
	},
	{
		// VersionClearRange is https://github.com/cockroachdb/cockroach/pull/20601.
		Key:     VersionClearRange,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 6},
	},
	{
		// VersionPartitioning gates all backwards-incompatible changes required by
		// table partitioning, as described in the RFC:
		// https://github.com/cockroachdb/cockroach/pull/18683
		//
		// These backwards-incompatible changes include:
		//   - writing table descriptors with a partitioning scheme
		//   - writing zone configs with index or partition subzones
		//
		// There is no guarantee that upgrading a cluster that uses partitioning
		// will work properly until v2.0 is released. Such clusters should expect to
		// be wiped after every v1.1-X upgrade.
		Key:     VersionPartitioning,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 7},
	},
	{
		// VersionLeaseSequence is https://github.com/cockroachdb/cockroach/pull/20953.
		Key:     VersionLeaseSequence,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 8},
	},
	{
		// VersionUnreplicatedTombstoneKey is https://github.com/cockroachdb/cockroach/pull/21120.
		Key:     VersionUnreplicatedTombstoneKey,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 9},
	},
	{
		// VersionRecomputeStats is https://github.com/cockroachdb/cockroach/pull/21345.
		Key:     VersionRecomputeStats,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 10},
	},
	{
		// VersionNoRaftProposalKeys is https://github.com/cockroachdb/cockroach/pull/20647.
		Key:     VersionNoRaftProposalKeys,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 11},
	},
	{
		// VersionTxnSpanRefresh is https://github.com/cockroachdb/cockroach/pull/21140.
		Key:     VersionTxnSpanRefresh,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 12},
	},
	{
		// VersionReadUncommittedRangeLookups is https://github.com/cockroachdb/cockroach/pull/21276.
		Key:     VersionReadUncommittedRangeLookups,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 13},
	},
	{
		// VersionPerReplicaZoneConstraints is https://github.com/cockroachdb/cockroach/pull/22819.
		Key:     VersionPerReplicaZoneConstraints,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 14},
	},
	{
		// VersionLeasePreferences is https://github.com/cockroachdb/cockroach/pull/23202.
		Key:     VersionLeasePreferences,
		Version: roachpb.Version{Major: 1, Minor: 1, Unstable: 15},
	},
	{
		// Version2_0 is CockroachDB v2.0. It's used for all v2.0.x patch releases.
		Key:     Version2_0,
		Version: roachpb.Version{Major: 2, Minor: 0},
	},
	{
		// VersionImportSkipRecords is https://github.com/cockroachdb/cockroach/pull/23466
		Key:     VersionImportSkipRecords,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 1},
	},
	{
		// VersionProposedTSLeaseRequest is https://github.com/cockroachdb/cockroach/pull/23466
		Key:     VersionProposedTSLeaseRequest,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 2},
	},
	{
		// VersionRangeAppliedStateKey is https://github.com/cockroachdb/cockroach/pull/22317.
		Key:     VersionRangeAppliedStateKey,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 3},
	},
	{
		// VersionImportFormats is https://github.com/cockroachdb/cockroach/pull/25615.
		Key:     VersionImportFormats,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 4},
	},
	{
		// VersionSecondaryLookupJoins is https://github.com/cockroachdb/cockroach/pull/25628.
		Key:     VersionSecondaryLookupJoins,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 5},
	},
	{
		// VersionClientsideWritingFlag is https://github.com/cockroachdb/cockroach/pull/25541.
		// Before this version, the Transaction.Writing flag used to be set by the
		// server. After, it is set only by the client.
		Key:     VersionClientSideWritingFlag,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 6},
	},
	{
		// VersionColumnarTimeSeries is https://github.com/cockroachdb/cockroach/pull/26614.
		Key:     VersionColumnarTimeSeries,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 7},
	},
	{
		// VersionTxnCoordMetaInvalidField is https://github.com/cockroachdb/cockroach/pull/27420.
		Key:     VersionTxnCoordMetaInvalidField,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 8},
	},
	{
		// VersionAsyncConsensus is https://github.com/cockroachdb/cockroach/pull/26599.
		Key:     VersionAsyncConsensus,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 9},
	},
	{
		// VersionBatchResponse is https://github.com/cockroachdb/cockroach/pull/26553.
		Key:     VersionBatchResponse,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 10},
	},
	{
		// VersionCreateChangefeed is https://github.com/cockroachdb/cockroach/pull/27962.
		Key:     VersionCreateChangefeed,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 11},
	},
	{
		// VersionRangeMerges is https://github.com/cockroachdb/cockroach/pull/28865.
		Key:     VersionRangeMerges,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 12},
	},
	{
		// VersionBitArrayColumns is https://github.com/cockroachdb/cockroach/pull/28807.
		Key:     VersionBitArrayColumns,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 13},
	},
	{
		// VersionLoadBasedRebalancing is https://github.com/cockroachdb/cockroach/pull/28852.
		Key:     VersionLoadBasedRebalancing,
		Version: roachpb.Version{Major: 2, Minor: 0, Unstable: 14},
	},
	{
		// Version2_1 is CockroachDB v2.1. It's used for all v2.1.x patch releases.
		Key:     Version2_1,
		Version: roachpb.Version{Major: 2, Minor: 1},
	},
	{
		// VersionCascadingZoneConfigs is https://github.com/cockroachdb/cockroach/pull/30611.
		Key:     VersionCascadingZoneConfigs,
		Version: roachpb.Version{Major: 2, Minor: 1, Unstable: 1},
	},
	{
		// VersionLoadSplits is https://github.com/cockroachdb/cockroach/pull/31413.
		Key:     VersionLoadSplits,
		Version: roachpb.Version{Major: 2, Minor: 1, Unstable: 2},
	},
	{
		// VersionExportStorageWorkload is https://github.com/cockroachdb/cockroach/pull/31899.
		Key:     VersionExportStorageWorkload,
		Version: roachpb.Version{Major: 2, Minor: 1, Unstable: 3},
	},

	// Add new versions here (step two of two).

}).Validated()

var (
	// BinaryMinimumSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than BinaryMinimumSupportedVersion, then the binary will exit with
	// an error.
	BinaryMinimumSupportedVersion = VersionByKey(Version2_0)

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
