// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterversion

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// VersionKey is a unique identifier for a version of CockroachDB.
type VersionKey int

// Version constants. You'll want to add a new one in the following cases:
//
// (a) When introducing a backwards incompatible feature. Broadly, by this we
//     mean code that's structured as follows:
//
//      if (given-version is active) {
//          // Implies that all nodes in the cluster are running binaries that
//          // have this code. We can "enable" the new feature knowing that
//          // outbound RPCs, requests, etc. will be handled by nodes that know
//          // how to do so.
//      } else {
//          // There may be some nodes running older binaries without this code.
//          // To be safe, we'll want to behave as we did before introducing
//         // this feature.
//      }
//
//     See clusterversion.Handle.IsActive and usage of existing versions below
//     for additional commentary.
//
// (b) When cutting a major release branch. When cutting release-20.2 for
//     example, you'll want to introduce the following to `master`.
//
//       (i)  Version20_2 (keyed to v20.2.0-0})
//	     (ii) VersionStart21_1 (keyed to v20.2.0-1})
//
//    You'll then want to backport (i) to the release branch itself (i.e.
//    release-20.2). You'll also want to bump binaryMinSupportedVersion. In the
//    example above, you'll set it to Version20_2. This indicates that the
//    minimum binary version required in a cluster with with nodes running
//    v21.1 binaries (including pre-release alphas) is v20.2
//
// When introducing a version constant, you'll want to:
//   (1) Add it at the end of this block
//   (2) Add it at the end of the `versionsSingleton` block below.
//
// 									---
//
// You'll want to delete versions from this list after cutting a major release.
// Once the development for 21.1 begins, after step (ii) from above, all
// versions introduced in the previous release can be removed (everything prior
// to Version20_2 in our example).
//
// When removing a version, you'll want to remove its associated runtime checks.
// All "is active" checks for the key will always evaluate to true. You'll also
// want to delete the constant and remove its entry in the `versionsSingleton`
// block below.
//
//go:generate stringer -type=VersionKey
const (
	_ VersionKey = iota - 1 // want first named one to start at zero

	// Version19_1 is CockroachDB v19.1. It's used for all v19.1.x patch
	// releases.
	Version19_1

	// v20.1 versions.
	//
	// VersionContainsEstimatesCounter is
	// https://github.com/cockroachdb/cockroach/pull/37583.
	//
	// MVCCStats.ContainsEstimates has been migrated from boolean to a
	// counter so that the consistency checker and splits can reset it by
	// returning -ContainsEstimates, avoiding racing with other operations
	// that want to also change it.
	//
	// The migration maintains the invariant that raft commands with
	// ContainsEstimates zero or one want the bool behavior (i.e. 1+1=1).
	// Before the cluster version is active, at proposal time we'll refuse
	// any negative ContainsEstimates plus we clamp all others to {0,1}.
	// When the version is active, and ContainsEstimates is positive, we
	// multiply it by 2 (i.e. we avoid 1). Downstream of raft, we use old
	// behavior for ContainsEstimates=1 and the additive behavior for
	// anything else.
	VersionContainsEstimatesCounter
	// VersionNamespaceTableWithSchemas is
	// https://github.com/cockroachdb/cockroach/pull/41977
	//
	// It represents the migration to a new system.namespace table that has an
	// added parentSchemaID column. In addition to the new column, the table is
	// no longer in the system config range -- implying it is no longer gossiped.
	VersionNamespaceTableWithSchemas
	// VersionAuthLocalAndTrustRejectMethods introduces the HBA rule
	// prefix 'local' and auth methods 'trust' and 'reject', for use
	// in server.host_based_authentication.configuration.
	//
	// A separate cluster version ensures the new syntax is not
	// introduced while previous-version nodes are still running, as
	// this would block any new SQL client.
	VersionAuthLocalAndTrustRejectMethods

	// TODO(irfansharif): The versions above can/should all be removed. They
	// were orinally introduced in v20.1. There are inflight PRs to do so
	// (#57155, #57156, #57158).

	// v20.2 versions.
	//
	// VersionStart20_2 demarcates work towards CockroachDB v20.2.
	VersionStart20_2
	// VersionGeospatialType enables the use of Geospatial features.
	VersionGeospatialType
	// VersionEnums enables the use of ENUM types.
	VersionEnums
	// VersionRangefeedLeases is the enablement of leases uses rangefeeds. All
	// nodes with this versions will have rangefeeds enabled on all system
	// ranges. Once this version is finalized, gossip is not needed in the
	// schema lease subsystem. Nodes which start with this version finalized
	// will not pass gossip to the SQL layer.
	VersionRangefeedLeases
	// VersionAlterColumnTypeGeneral enables the use of alter column type for
	// conversions that require the column data to be rewritten.
	VersionAlterColumnTypeGeneral
	// VersionAlterSystemJobsTable is a version which modified system.jobs
	// table.
	VersionAlterSystemJobsAddCreatedByColumns
	// VersionAddScheduledJobsTable is a version which adds
	// system.scheduled_jobs table.
	VersionAddScheduledJobsTable
	// VersionUserDefinedSchemas enables the creation of user defined schemas.
	VersionUserDefinedSchemas
	// VersionNoOriginFKIndexes allows for foreign keys to no longer need
	// indexes on the origin side of the relationship.
	VersionNoOriginFKIndexes
	// VersionClientRangeInfosOnBatchResponse moves the response RangeInfos from
	// individual response headers to the batch header.
	VersionClientRangeInfosOnBatchResponse
	// VersionNodeMembershipStatus gates the usage of the MembershipStatus enum
	// in the Liveness proto. See comment on proto definition for more details.
	VersionNodeMembershipStatus
	// VersionRangeStatsRespHasDesc adds the RangeStatsResponse.RangeInfo field.
	VersionRangeStatsRespHasDesc
	// VersionMinPasswordLength adds the server.user_login.min_password_length
	// setting.
	VersionMinPasswordLength
	// VersionAbortSpanBytes adds a field to MVCCStats
	// (MVCCStats.AbortSpanBytes) that tracks the size of a range's abort span.
	VersionAbortSpanBytes
	// VersionAlterSystemJobsTableAddLeaseColumn is a version which modified
	// system.jobs table.
	VersionAlterSystemJobsAddSqllivenessColumnsAddNewSystemSqllivenessTable
	// VersionMaterializedViews enables the use of materialized views.
	VersionMaterializedViews
	// VersionBox2DType enables the use of the box2d type.
	VersionBox2DType
	// VersionLeasedDatabasedDescriptors enables leased database descriptors.
	// Now that we unconditionally use leased descriptors in 21.1 and the main
	// usages of this version gate have been removed, this version remains to
	// gate a few miscellaneous database descriptor changes.
	VersionLeasedDatabaseDescriptors
	// VersionUpdateScheduledJobsSchema drops schedule_changes and adds
	// schedule_status.
	VersionUpdateScheduledJobsSchema
	// VersionCreateLoginPrivilege is when CREATELOGIN/NOCREATELOGIN are
	// introduced.
	//
	// It represents adding authn principal management via CREATELOGIN role
	// option.
	VersionCreateLoginPrivilege
	// VersionHBAForNonTLS is when the 'hostssl' and 'hostnossl' HBA configs are
	// introduced.
	VersionHBAForNonTLS
	// Version20_2 is CockroachDB v20.2. It's used for all v20.2.x patch
	// releases.
	Version20_2

	// v21.1 versions.
	//
	// VersionStart21_1 demarcates work towards CockroachDB v21.1.
	VersionStart21_1
	// VersionEmptyArraysInInvertedIndexes is when empty arrays are added to
	// array inverted indexes.
	VersionEmptyArraysInInvertedIndexes

	// Step (1): Add new versions here.
)

// versionsSingleton lists all historical versions here in chronological order,
// with comments describing what backwards-incompatible features were
// introduced.
//
// A roachpb.Version has the colloquial form MAJOR.MINOR[.PATCH][-INTERNAL],
// where the PATCH and INTERNAL components can be omitted if zero. Keep in mind
// that a version with an internal component, like 1.1-2, represents a version
// that was developed AFTER v1.1 was released and is not slated for release
// until the next stable version (either 1.2-0 or 2.0-0). Patch releases, like
// 1.1.2, do not have associated migrations.
//
// NB: The version upgrade process requires the versions as seen by a cluster to
// be monotonic. Once we've added 1.1-0,  we can't slot in 1.0-4 because
// clusters already running 1.1-0 won't migrate through the new 1.0-4 version.
// Such clusters would need to be wiped. As a result, do not bump the major or
// minor version until we are absolutely sure that no new migrations will need
// to be added (i.e., when cutting the final release candidate).
var versionsSingleton = keyedVersions([]keyedVersion{
	{
		Key:     Version19_1,
		Version: roachpb.Version{Major: 19, Minor: 1},
	},
	{
		Key:     VersionContainsEstimatesCounter,
		Version: roachpb.Version{Major: 19, Minor: 2, Internal: 2},
	},
	{
		Key:     VersionNamespaceTableWithSchemas,
		Version: roachpb.Version{Major: 19, Minor: 2, Internal: 5},
	},
	{
		Key:     VersionAuthLocalAndTrustRejectMethods,
		Version: roachpb.Version{Major: 19, Minor: 2, Internal: 8},
	},

	// v20.2 versions.
	{
		Key:     VersionStart20_2,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 1},
	},
	{
		Key:     VersionGeospatialType,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 2},
	},
	{
		Key:     VersionEnums,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 3},
	},
	{
		Key:     VersionRangefeedLeases,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 4},
	},
	{
		Key:     VersionAlterColumnTypeGeneral,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 5},
	},
	{
		Key:     VersionAlterSystemJobsAddCreatedByColumns,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 6},
	},
	{
		Key:     VersionAddScheduledJobsTable,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 7},
	},
	{
		Key:     VersionUserDefinedSchemas,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 8},
	},
	{
		Key:     VersionNoOriginFKIndexes,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 9},
	},
	{
		Key:     VersionClientRangeInfosOnBatchResponse,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 10},
	},
	{
		Key:     VersionNodeMembershipStatus,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 11},
	},
	{
		Key:     VersionRangeStatsRespHasDesc,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 12},
	},
	{
		Key:     VersionMinPasswordLength,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 13},
	},
	{
		Key:     VersionAbortSpanBytes,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 14},
	},
	{
		Key:     VersionAlterSystemJobsAddSqllivenessColumnsAddNewSystemSqllivenessTable,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 15},
	},
	{
		Key:     VersionMaterializedViews,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 16},
	},
	{
		Key:     VersionBox2DType,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 17},
	},
	{
		Key:     VersionLeasedDatabaseDescriptors,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 18},
	},
	{
		Key:     VersionUpdateScheduledJobsSchema,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 19},
	},
	{
		Key:     VersionCreateLoginPrivilege,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 20},
	},
	{
		Key:     VersionHBAForNonTLS,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 21},
	},
	{
		Key:     Version20_2,
		Version: roachpb.Version{Major: 20, Minor: 2},
	},

	// v21.1 versions.
	{
		Key:     VersionStart21_1,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 1},
	},
	{
		Key:     VersionEmptyArraysInInvertedIndexes,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 2},
	},

	// Step (2): Add new versions here.
})

// TODO(irfansharif): clusterversion.binary{,MinimumSupported}Version
// feels out of place. A "cluster version" and a "binary version" are two
// separate concepts.
var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error.
	binaryMinSupportedVersion = VersionByKey(Version20_2)

	// binaryVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	binaryVersion = versionsSingleton[len(versionsSingleton)-1].Version
)

// VersionByKey returns the roachpb.Version for a given key.
// It is a fatal error to use an invalid key.
func VersionByKey(key VersionKey) roachpb.Version {
	return versionsSingleton.MustByKey(key)
}
