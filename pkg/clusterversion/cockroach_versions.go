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

// Key is a unique identifier for a version of CockroachDB.
type Key int

// Version constants. These drive compatibility between versions as well as
// migrations. Before you add a version or consider removing one, please
// familiarize yourself with the rules below.
//
// Adding Versions
//
// You'll want to add a new one in the following cases:
//
// (a) When introducing a backwards incompatible feature. Broadly, by this we
//     mean code that's structured as follows:
//
//      if (specific-version is active) {
//          // Implies that all nodes in the cluster are running binaries that
//          // have this code. We can "enable" the new feature knowing that
//          // outbound RPCs, requests, etc. will be handled by nodes that know
//          // how to do so.
//      } else {
//          // There may be some nodes running older binaries without this code.
//          // To be safe, we'll want to behave as we did before introducing
//          // this feature.
//      }
//
//     Authors of migrations need to be careful in ensuring that end-users
//     aren't able to enable feature gates before they're active. This is fine:
//
//      func handleSomeNewStatement() error {
//          if !(specific-version is active) {
//              return errors.New("cluster version needs to be bumped")
//          }
//          // ...
//      }
//
//     At the same time, with requests/RPCs originating at other crdb nodes, the
//     initiator of the request gets to decide what's supported. A node should
//     not refuse functionality on the grounds that its view of the version gate
//     is as yet inactive. Consider the sender:
//
//      func invokeSomeRPC(req) {
//	        if (specific-version is active) {
//              // Like mentioned above, this implies that all nodes in the
//              // cluster are running binaries that can handle this new
//              // feature. We may have learned about this fact before the
//              // node on the other end. This is due to the fact that migration
//              // manager informs each node about the specific-version being
//              // activated active concurrently. See BumpClusterVersion for
//              // where that happens. Still, it's safe for us to enable the new
//              // feature flags as we trust the recipient to know how to deal
//              // with it.
//		        req.NewFeatureFlag = true
//	        }
//	        send(req)
//      }
//
//    And consider the recipient:
//
//     func someRPC(req) {
//         if !req.NewFeatureFlag {
//             // Legacy behavior...
//         }
//         // There's no need to even check if the specific-version is active.
//         // If the flag is enabled, the specific-version must have been
//         // activated, even if we haven't yet heard about it (we will pretty
//         // soon).
//     }
//
//     See clusterversion.Handle.IsActive and usage of some existing versions
//     below for more clues on the matter.
//
// (b) When cutting a major release branch. When cutting release-20.2 for
//     example, you'll want to introduce the following to `master`.
//
//       (i)  V20_2 (keyed to v20.2.0-0})
//       (ii) Start21_1 (keyed to v20.2.0-1})
//
//    You'll then want to backport (i) to the release branch itself (i.e.
//    release-20.2). You'll also want to bump binaryMinSupportedVersion. In the
//    example above, you'll set it to V20_2. This indicates that the
//    minimum binary version required in a cluster with with nodes running
//    v21.1 binaries (including pre-release alphas) is v20.2, i.e. that an
//    upgrade into such a binary must start out from at least v20.2 nodes.
//
//    Aside: At the time of writing, the binary min supported version is the
//    last major release, though we may consider relaxing this in the future
//    (i.e. for example could skip up to one major release) as we move to a more
//    frequent release schedule.
//
// When introducing a version constant, you'll want to:
//   (1) Add it at the end of this block. For versions introduced during and
//       after the 21.1 release, Internal versions must be even-numbered. The
//       odd versions are used for internal book-keeping.
//   (2) Add it at the end of the `versionsSingleton` block below.
//
// Migrations
//
// Migrations are idempotent functions that can be attached to versions and will
// be rolled out before the respective cluster version gets rolled out. They are
// primarily a means to remove legacy state from the cluster. For example, a
// migration might scan the cluster for an outdated type of table descriptor and
// rewrite it into a new format. Migrations are tricky to get right and they have
// their own documentation in ./pkg/migration, which you should peruse should you
// feel that a migration is necessary for your use case.
//
// Phasing out Versions and Migrations
//
// Versions and Migrations can be removed once they are no longer going to be
// exercised. This is primarily driven by the BinaryMinSupportedVersion, which
// declares the oldest *cluster* (not binary) version of CockroachDB that may
// interface with the running node. It typically trails the current version by
// one release. For example, if the current branch is a `21.1.x` release, you
// will have a BinaryMinSupportedVersion of `21.0`, meaning that the versions
// 20.2.0-1, 20.2.0-2, etc are always going to be active on any peer and thus
// can be "baked in"; similarly all migrations attached to any of these versions
// can be assumed to have run (or not having been necessary due to the cluster
// having been initialized at a higher version in the first place). Note that
// this implies that all peers will have a *binary* version of at least the
// MinSupportedVersion as well, as this is a prerequisite for running at that
// cluster version. Finally, note that even when all cluster versions known
// to the current binary are active (i.e. most of the time), you still need
// to be able to inter-op with older *binary* and/or *cluster* versions. This
// is because *tenants* are allowed to run at any binary version compatible
// with (i.e. greater than or equal to) the MinSupportedVersion. To give a
// concrete example, a fully up-to-date v21.1 KV host cluster can have tenants
// running against it that use the v21.0 binary and any cluster version known
// to that binary (v20.2-0 ... v20.2-50 or thereabouts).
//
// You'll want to delete versions from this list after cutting a major release.
// Once the development for 21.1 begins, after step (ii) from above, all
// versions introduced in the previous release can be removed (everything prior
// to V20_2 in our example).
//
// When removing a version, you'll want to remove its associated runtime checks.
// All "is active" checks for the key will always evaluate to true. You'll also
// want to delete the constant and remove its entry in the `versionsSingleton`
// block below.
//
//go:generate stringer -type=Key
const (
	_ Key = iota - 1 // want first named one to start at zero

	// v20.2 versions.
	//
	// Start20_2 demarcates work towards CockroachDB v20.2.
	// If you're here to remove versions, please read the comment at the
	// beginning of the const block. We cannot remove these versions until
	// MinSupportedVersion=21.1, i.e. on the master branch *after* cutting
	// the 21.1 release. This is because we now support tenants at the
	// predecessor binary interacting with a fully upgraded KV cluster.
	Start20_2
	// GeospatialType enables the use of Geospatial features.
	GeospatialType
	// AlterColumnTypeGeneral enables the use of alter column type for
	// conversions that require the column data to be rewritten.
	AlterColumnTypeGeneral
	// UserDefinedSchemas enables the creation of user defined schemas.
	UserDefinedSchemas
	// NoOriginFKIndexes allows for foreign keys to no longer need indexes on
	// the origin side of the relationship.
	NoOriginFKIndexes
	// NodeMembershipStatus gates the usage of the MembershipStatus enum in the
	// Liveness proto. See comment on proto definition for more details.
	NodeMembershipStatus
	// MinPasswordLength adds the server.user_login.min_password_length setting.
	MinPasswordLength
	// AbortSpanBytes adds a field to MVCCStats
	// (MVCCStats.AbortSpanBytes) that tracks the size of a range's abort span.
	AbortSpanBytes
	// MaterializedViews enables the use of materialized views.
	MaterializedViews
	// Box2DType enables the use of the box2d type.
	Box2DType
	// CreateLoginPrivilege is when CREATELOGIN/NOCREATELOGIN are introduced.
	//
	// It represents adding authn principal management via CREATELOGIN role
	// option.
	CreateLoginPrivilege
	// HBAForNonTLS is when the 'hostssl' and 'hostnossl' HBA configs are
	// introduced.
	HBAForNonTLS
	// V20_2 is CockroachDB v20.2. It's used for all v20.2.x patch releases.
	V20_2

	// v21.1 versions.
	//
	// Start21_1 demarcates work towards CockroachDB v21.1.
	Start21_1
	// EmptyArraysInInvertedIndexes is when empty arrays are added to array
	// inverted indexes.
	EmptyArraysInInvertedIndexes
	// UniqueWithoutIndexConstraints is when adding UNIQUE WITHOUT INDEX
	// constraints is supported.
	UniqueWithoutIndexConstraints
	// VirtualComputedColumns is when virtual computed columns are supported.
	VirtualComputedColumns
	// CPutInline is conditional put support for inline values.
	CPutInline
	// ReplicaVersions enables the versioning of Replica state.
	ReplicaVersions
	// replacedTruncatedAndRangeAppliedStateMigration stands in for
	// TruncatedAndRangeAppliedStateMigration which was	re-introduced after the
	// migration job was introduced. This is necessary because the jobs
	// infrastructure used to run this migration in v21.1 and its later alphas
	// was introduced after this version was first introduced. Later code in the
	// release relies on the job to run the migration but the job relies on
	// its startup migrations having been run. Versions associated with long
	// running migrations must follow LongRunningMigrations.
	replacedTruncatedAndRangeAppliedStateMigration
	// replacedPostTruncatedAndRangeAppliedStateMigration is like the above
	// version. See its comment.
	replacedPostTruncatedAndRangeAppliedStateMigration
	// NewSchemaChanger enables the new schema changer.
	NewSchemaChanger
	// LongRunningMigrations introduces the LongRunningMigrations table and jobs.
	// All versions which have a registered long-running migration must have a
	// version higher than this version.
	LongRunningMigrations
	// TruncatedAndRangeAppliedStateMigration is part of the migration to stop
	// using the legacy truncated state within KV. After the migration, we'll be
	// using the unreplicated truncated state and the RangeAppliedState on all
	// ranges. Callers that wish to assert on there no longer being any legacy
	// will be able to do so after PostTruncatedAndRangeAppliedStateMigration is
	// active. This lets remove any holdover code handling the possibility of
	// replicated truncated state in 21.2.
	//
	// TODO(irfansharif): Do the above in 21.2.
	TruncatedAndRangeAppliedStateMigration
	// PostTruncatedAndRangeAppliedStateMigration is used to purge all replicas
	// using the replicated legacy TruncatedState. It's also used in asserting
	// that no replicated truncated state representation is found.
	PostTruncatedAndRangeAppliedStateMigration
	// SeparatedIntents allows the writing of separated intents/locks.
	SeparatedIntents
	// TracingVerbosityIndependentSemantics marks a change in which trace spans
	// are propagated across RPC boundaries independently of their verbosity setting.
	// This requires a version gate this violates implicit assumptions in v20.2.
	TracingVerbosityIndependentSemantics
	// SequencesRegclass starts storing sequences used in DEFAULT expressions
	// via their IDs instead of their names, which leads to allowing such
	// sequences to be renamed.
	SequencesRegclass
	// ImplicitColumnPartitioning introduces implicit column partitioning to
	// tables.
	ImplicitColumnPartitioning
	// MultiRegionFeatures introduces new multi-region features to the
	// database, such as adding REGIONS to a DATABASE or setting the LOCALITY
	// on a TABLE.
	MultiRegionFeatures
	// ClosedTimestampsRaftTransport enables the Raft transport for closed
	// timestamps and disables the previous per-node transport.
	ClosedTimestampsRaftTransport
	// ChangefeedsSupportPrimaryIndexChanges is used to indicate that all
	// nodes support detecting and restarting on primary index changes.
	ChangefeedsSupportPrimaryIndexChanges
	// ForeignKeyRepresentationMigration is used to ensure that all no table
	// descriptors use the pre-19.2 foreign key migration.
	ForeignKeyRepresentationMigration
	// PriorReadSummaries introduces support for the use of read summary objects
	// to ship information about reads on a range through lease changes and
	// range merges.
	PriorReadSummaries
	// NonVotingReplicas enables the creation of non-voting replicas.
	NonVotingReplicas
	// ProtectedTsMetaPrivilegesMigration is for the migration which fixes the
	// privileges of the protected_ts_meta system table.
	ProtectedTsMetaPrivilegesMigration
	// V21_1 is CockroachDB v21.1. It's used for all v21.1.x patch releases.
	V21_1

	// v21.1PLUS release. This is a special v21.1.x release with extra changes,
	// used internally for the 2021 serverless offering.
	Start21_1PLUS

	// v21.2 versions.
	//
	// Start21_2 demarcates work towards CockroachDB v21.2.
	Start21_2
	// JoinTokensTable adds the system table for storing ephemeral generated
	// join tokens.
	JoinTokensTable
	// AcquisitionTypeInLeaseHistory augments the per-replica lease history to
	// include the type of lease acquisition event that resulted in that replica's
	// current lease.
	AcquisitionTypeInLeaseHistory
	// SerializeViewUDTs serializes user defined types used in views to allow
	// for renaming of the referenced types.
	SerializeViewUDTs
	// ExpressionIndexes is when expression indexes are supported.
	ExpressionIndexes
	// DeleteDeprecatedNamespaceTableDescriptorMigration deletes the descriptor at ID=2.
	DeleteDeprecatedNamespaceTableDescriptorMigration
	// FixDescriptors is for the migration to fix all descriptors.
	FixDescriptors
	// SQLStatsTable adds the system tables for storing persisted SQL statistics
	// for statements and transactions.
	SQLStatsTable

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
var versionsSingleton = keyedVersions{

	// v20.2 versions.
	{
		Key:     Start20_2,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 1},
	},
	{
		Key:     GeospatialType,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 2},
	},
	{
		Key:     AlterColumnTypeGeneral,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 5},
	},
	{
		Key:     UserDefinedSchemas,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 8},
	},
	{
		Key:     NoOriginFKIndexes,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 9},
	},
	{
		Key:     NodeMembershipStatus,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 11},
	},
	{
		Key:     MinPasswordLength,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 13},
	},
	{
		Key:     AbortSpanBytes,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 14},
	},
	{
		Key:     MaterializedViews,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 16},
	},
	{
		Key:     Box2DType,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 17},
	},
	{
		Key:     CreateLoginPrivilege,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 20},
	},
	{
		Key:     HBAForNonTLS,
		Version: roachpb.Version{Major: 20, Minor: 1, Internal: 21},
	},
	{
		Key:     V20_2,
		Version: roachpb.Version{Major: 20, Minor: 2},
	},

	// v21.1 versions. Internal versions defined here-on-forth must be even.
	{
		Key:     Start21_1,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 2},
	},
	{
		Key:     EmptyArraysInInvertedIndexes,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 4},
	},
	{
		Key:     UniqueWithoutIndexConstraints,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 6},
	},
	{
		Key:     VirtualComputedColumns,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 8},
	},
	{
		Key:     CPutInline,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 10},
	},
	{
		Key:     ReplicaVersions,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 12},
	},
	{
		Key:     replacedTruncatedAndRangeAppliedStateMigration,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 14},
	},
	{
		Key:     replacedPostTruncatedAndRangeAppliedStateMigration,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 16},
	},
	{
		Key:     NewSchemaChanger,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 18},
	},
	{
		Key:     LongRunningMigrations,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 20},
	},
	{
		Key:     TruncatedAndRangeAppliedStateMigration,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 22},
	},
	{
		Key:     PostTruncatedAndRangeAppliedStateMigration,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 24},
	},
	{
		Key:     SeparatedIntents,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 26},
	},
	{
		Key:     TracingVerbosityIndependentSemantics,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 28},
	},
	{
		Key:     SequencesRegclass,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 30},
	},
	{
		Key:     ImplicitColumnPartitioning,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 32},
	},
	{
		Key:     MultiRegionFeatures,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 34},
	},
	{
		Key:     ClosedTimestampsRaftTransport,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 36},
	},
	{
		Key:     ChangefeedsSupportPrimaryIndexChanges,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 38},
	},
	{
		Key:     ForeignKeyRepresentationMigration,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 42},
	},
	{
		Key:     PriorReadSummaries,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 44},
	},
	{
		Key:     NonVotingReplicas,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 46},
	},
	{
		Key:     ProtectedTsMetaPrivilegesMigration,
		Version: roachpb.Version{Major: 20, Minor: 2, Internal: 48},
	},
	{
		// V21_1 is CockroachDB v21.1. It's used for all v21.1.x patch releases.
		Key:     V21_1,
		Version: roachpb.Version{Major: 21, Minor: 1},
	},

	// v21.1PLUS version. This is a special v21.1.x release with extra changes,
	// used internally for the 2021 Serverless offering.
	//
	// Any v21.1PLUS change that needs a migration will have a v21.2 version on
	// master but a v21.1PLUS version on the v21.1PLUS branch.
	{
		Key: Start21_1PLUS,
		// The Internal version starts out at 14 for historic reasons: at the time
		// this was added, v21.2 versions were already defined up to 12.
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 14},
	},

	// v21.2 versions.
	{
		Key:     Start21_2,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 102},
	},
	{
		Key:     JoinTokensTable,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 104},
	},
	{
		Key:     AcquisitionTypeInLeaseHistory,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 106},
	},
	{
		Key:     SerializeViewUDTs,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 108},
	},
	{
		Key:     ExpressionIndexes,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 110},
	},
	{
		Key:     DeleteDeprecatedNamespaceTableDescriptorMigration,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 112},
	},
	{
		Key:     FixDescriptors,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 114},
	},
	{
		Key:     SQLStatsTable,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 116},
	},
	// Step (2): Add new versions here.
}

// TODO(irfansharif): clusterversion.binary{,MinimumSupported}Version
// feels out of place. A "cluster version" and a "binary version" are two
// separate concepts.
var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error.
	binaryMinSupportedVersion = ByKey(V20_2)

	// binaryVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	binaryVersion = versionsSingleton[len(versionsSingleton)-1].Version
)

// ByKey returns the roachpb.Version for a given key.
// It is a fatal error to use an invalid key.
func ByKey(key Key) roachpb.Version {
	return versionsSingleton.MustByKey(key)
}

// ListBetween returns the list of cluster versions in the range
// (from, to].
func ListBetween(from, to ClusterVersion) []ClusterVersion {
	return listBetweenInternal(from, to, versionsSingleton)
}

func listBetweenInternal(from, to ClusterVersion, vs keyedVersions) []ClusterVersion {
	var cvs []ClusterVersion
	for _, keyedV := range vs {
		// Read: "from < keyedV <= to".
		if from.Less(keyedV.Version) && keyedV.Version.LessEq(to.Version) {
			cvs = append(cvs, ClusterVersion{Version: keyedV.Version})
		}
	}
	return cvs
}
