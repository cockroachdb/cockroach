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
//    minimum binary version required in a cluster with nodes running
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
//       odd versions are used for internal book-keeping. The Internal version
//       should be the previous Internal version for the same minor release plus
//       two.
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

	// v21.1 versions.
	//
	// V21_1 is CockroachDB v21.1. It's used for all v21.1.x patch releases.
	//
	// TODO(irfansharif): This can be removed as part of #71708 (bump
	// min-supported version to 21.2).
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
	// DatabaseRoleSettings adds the system table for storing per-user and
	// per-role default session settings.
	DatabaseRoleSettings
	// TenantUsageTable adds the system table for tracking tenant usage.
	TenantUsageTable
	// SQLInstancesTable adds the system table for storing SQL instance information
	// per tenant.
	SQLInstancesTable
	// Can return new retryable rangefeed errors without crashing the client
	NewRetryableRangefeedErrors
	// AlterSystemWebSessionsCreateIndexes creates indexes on the columns revokedAt and
	// lastUsedAt for the system.web_sessions table.
	AlterSystemWebSessionsCreateIndexes
	// SeparatedIntentsMigration adds the migration to move over all remaining
	// intents to the separated lock table space.
	SeparatedIntentsMigration
	// PostSeparatedIntentsMigration runs a cleanup migration after the main
	// SeparatedIntentsMigration.
	PostSeparatedIntentsMigration
	// RetryJobsWithExponentialBackoff retries failed jobs with exponential delays.
	RetryJobsWithExponentialBackoff
	// RecordsBasedRegistry replaces the existing monolithic protobuf-based
	// encryption-at-rest file registry with the new incremental records-based registry.
	RecordsBasedRegistry
	// AutoSpanConfigReconciliationJob adds the AutoSpanConfigReconciliationJob
	// type.
	AutoSpanConfigReconciliationJob
	// DefaultPrivileges default privileges are supported in this version.
	DefaultPrivileges
	// ZonesTableForSecondaryTenants adds system.zones for all secondary tenants.
	ZonesTableForSecondaryTenants
	// UseKeyEncodeForHashShardedIndexes changes the expression used in hash
	// sharded indexes from string casts to crdb_internal.datums_to_bytes.
	UseKeyEncodeForHashShardedIndexes
	// DatabasePlacementPolicy setting PLACEMENT for databases is supported in this
	// version.
	DatabasePlacementPolicy
	// GeneratedAsIdentity is the syntax support for `GENERATED {ALWAYS | BY
	// DEFAULT} AS IDENTITY` under `CREATE TABLE` syntax.
	GeneratedAsIdentity
	// OnUpdateExpressions setting ON UPDATE column expressions is supported in
	// this version.
	OnUpdateExpressions
	// SpanConfigurationsTable adds the span configurations system table, to
	// store all KV span configs.
	SpanConfigurationsTable
	// BoundedStaleness adds capabilities to perform bounded staleness reads.
	BoundedStaleness
	// DateAndIntervalStyle enables DateStyle and IntervalStyle to be changed.
	DateAndIntervalStyle
	// PebbleFormatVersioned ratchets Pebble's format major version to
	// the version FormatVersioned.
	PebbleFormatVersioned
	// MarkerDataKeysRegistry switches to using an atomic marker file
	// for denoting which data keys registry is active.
	MarkerDataKeysRegistry
	// PebbleSetWithDelete switches to a backwards incompatible Pebble version
	// that provides SingleDelete semantics that are cleaner and robust to
	// programming error. See https://github.com/cockroachdb/pebble/issues/1255
	// and #69891.
	PebbleSetWithDelete
	// TenantUsageSingleConsumptionColumn changes the tenant_usage system table to
	// use a single consumption column (encoding a proto).
	TenantUsageSingleConsumptionColumn
	// SQLStatsTables adds the system table for storing persisted SQL statistics
	// for statements.
	SQLStatsTables
	// SQLStatsCompactionScheduledJob creates a ScheduledJob for SQL Stats
	// compaction on cluster startup and ensures that there is only one entry for
	// the schedule.
	SQLStatsCompactionScheduledJob
	// V21_2 is CockroachDB v21.2. It's used for all v21.2.x patch releases.
	V21_2

	// v22.1 versions.
	//
	// Start22_1 demarcates work towards CockroachDB v22.1.
	Start22_1

	// TargetBytesAvoidExcess prevents exceeding BatchRequest.Header.TargetBytes
	// except when there is a single value in the response. 21.2 DistSender logic
	// requires the limit to always be overshot in order to properly enforce
	// limits when splitting requests.
	TargetBytesAvoidExcess
	// AvoidDrainingNames avoids using the draining_names field when renaming or
	// dropping descriptors.
	AvoidDrainingNames
	// DrainingNamesMigration adds the migration which guarantees that no
	// descriptors have draining names.
	DrainingNamesMigration
	// TraceIDDoesntImplyStructuredRecording changes the contract about the kind
	// of span that RPCs get on the server depending on the tracing context.
	TraceIDDoesntImplyStructuredRecording
	// AlterSystemTableStatisticsAddAvgSizeCol adds the column avgSize to the
	// table system.table_statistics that contains a new statistic.
	AlterSystemTableStatisticsAddAvgSizeCol
	// AlterSystemStmtDiagReqs adds the migration for
	// system.statement_diagnostics_requests table to support collecting stmt
	// bundles when the query latency exceeds the user provided threshold.
	AlterSystemStmtDiagReqs
	// MVCCAddSSTable supports MVCC-compliant AddSSTable requests via the new
	// WriteAtRequestTimestamp and DisallowConflicts parameters.
	MVCCAddSSTable
	// Public schema is backed by a descriptor.
	PublicSchemasWithDescriptors
	// UnsplitRangesInAsyncGCJobs moves ranges unsplitting from transaction of
	// "drop table"/"truncate table" to async gc jobs
	UnsplitRangesInAsyncGCJobs
	// ValidateGrantOption checks whether the current user granting privileges to
	// another user holds the grant option for those privileges
	ValidateGrantOption
	// PebbleFormatBlockPropertyCollector switches to a backwards incompatible
	// Pebble version that provides block property collectors that can be used
	// for fine-grained time bound iteration. See
	// https://github.com/cockroachdb/pebble/issues/1190 for details.
	PebbleFormatBlockPropertyCollector
	// SeedTenantSpanConfigs populates system.span_configurations with seed
	// data for secondary tenants. This state is what ensures that we always
	// split on tenant boundaries when using the span configs infrastructure.
	// This version also comes with a migration to populate the same seed data
	// for existing tenants.
	SeedTenantSpanConfigs

	// *************************************************
	// Step (1): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************
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
	// v21.1 versions.
	{
		// V21_1 is CockroachDB v21.1. It's used for all v21.1.x patch releases.
		Key:     V21_1,
		Version: roachpb.Version{Major: 21, Minor: 1},
	},

	// Internal versions must be even.

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
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1102},
	},
	{
		Key:     JoinTokensTable,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1104},
	},
	{
		Key:     AcquisitionTypeInLeaseHistory,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1106},
	},
	{
		Key:     SerializeViewUDTs,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1108},
	},
	{
		Key:     ExpressionIndexes,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1110},
	},
	{
		Key:     DeleteDeprecatedNamespaceTableDescriptorMigration,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1112},
	},
	{
		Key:     FixDescriptors,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1114},
	},
	{
		Key:     DatabaseRoleSettings,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1118},
	},
	{
		Key:     TenantUsageTable,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1120},
	},
	{
		Key:     SQLInstancesTable,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1122},
	},
	{
		Key:     NewRetryableRangefeedErrors,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1124},
	},
	{
		Key:     AlterSystemWebSessionsCreateIndexes,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1126},
	},
	{
		Key:     SeparatedIntentsMigration,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1128},
	},
	{
		Key:     PostSeparatedIntentsMigration,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1130},
	},
	{
		Key:     RetryJobsWithExponentialBackoff,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1132},
	},
	{
		Key:     RecordsBasedRegistry,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1134},
	}, {
		Key:     AutoSpanConfigReconciliationJob,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1136},
	},
	{
		Key:     DefaultPrivileges,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1142},
	},
	{
		Key:     ZonesTableForSecondaryTenants,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1144},
	},
	{
		Key:     UseKeyEncodeForHashShardedIndexes,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1146},
	},
	{
		Key:     DatabasePlacementPolicy,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1148},
	},
	{
		Key:     GeneratedAsIdentity,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1150},
	},
	{
		Key:     OnUpdateExpressions,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1152},
	},
	{
		Key:     SpanConfigurationsTable,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1154},
	},
	{
		Key:     BoundedStaleness,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1156},
	},
	{
		Key:     DateAndIntervalStyle,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1160},
	},
	{
		Key:     PebbleFormatVersioned,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1162},
	},
	{
		Key:     MarkerDataKeysRegistry,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1164},
	},
	{
		Key:     PebbleSetWithDelete,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1166},
	},
	{
		Key:     TenantUsageSingleConsumptionColumn,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1168},
	},
	{
		Key:     SQLStatsTables,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1170},
	},
	{
		Key:     SQLStatsCompactionScheduledJob,
		Version: roachpb.Version{Major: 21, Minor: 1, Internal: 1172},
	},
	{
		// V21_2 is CockroachDB v21.2. It's used for all v21.2.x patch releases.
		Key:     V21_2,
		Version: roachpb.Version{Major: 21, Minor: 2},
	},

	// v22.1 versions. Internal versions must be even.
	{
		Key:     Start22_1,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 2},
	},
	{
		Key:     TargetBytesAvoidExcess,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 4},
	},
	{
		Key:     AvoidDrainingNames,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 6},
	},
	{
		Key:     DrainingNamesMigration,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 8},
	},
	{
		Key:     TraceIDDoesntImplyStructuredRecording,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 10},
	},
	{
		Key:     AlterSystemTableStatisticsAddAvgSizeCol,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 12},
	},
	{
		Key:     AlterSystemStmtDiagReqs,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 14},
	},
	{
		Key:     MVCCAddSSTable,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 16},
	},
	{
		Key:     PublicSchemasWithDescriptors,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 18},
	},
	{
		Key:     UnsplitRangesInAsyncGCJobs,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 20},
	},
	{
		Key:     ValidateGrantOption,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 22},
	},
	{
		Key:     PebbleFormatBlockPropertyCollector,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 24},
	},
	{
		Key:     SeedTenantSpanConfigs,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 26},
	},

	// *************************************************
	// Step (2): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************
}

// TODO(irfansharif): clusterversion.binary{,MinimumSupported}Version
// feels out of place. A "cluster version" and a "binary version" are two
// separate concepts.
var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error. This typically trails the current release by one (see top-level
	// comment).
	binaryMinSupportedVersion = ByKey(V21_1)

	// binaryVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	binaryVersion = versionsSingleton[len(versionsSingleton)-1].Version
)

func init() {
	const isReleaseBranch = false
	if isReleaseBranch {
		if binaryVersion != ByKey(V21_2) {
			panic("unexpected cluster version greater than release's binary version")
		}
	}
}

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
