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
//          if (specific-version is active) {
//              // Like mentioned above, this implies that all nodes in the
//              // cluster are running binaries that can handle this new
//              // feature. We may have learned about this fact before the
//              // node on the other end. This is due to the fact that migration
//              // manager informs each node about the specific-version being
//              // activated active concurrently. See BumpClusterVersion for
//              // where that happens. Still, it's safe for us to enable the new
//              // feature flags as we trust the recipient to know how to deal
//              // with it.
//            req.NewFeatureFlag = true
//          }
//          send(req)
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
// their own documentation in ./pkg/upgrade, which you should peruse should you
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

	// V21_2 is CockroachDB v21.2. It's used for all v21.2.x patch releases.
	V21_2

	// v22.1 versions.
	//
	// Start22_1 demarcates work towards CockroachDB v22.1.
	Start22_1

	// PebbleFormatBlockPropertyCollector switches to a backwards incompatible
	// Pebble version that provides block property collectors that can be used
	// for fine-grained time bound iteration. See
	// https://github.com/cockroachdb/pebble/issues/1190 for details.
	PebbleFormatBlockPropertyCollector
	// PublicSchemasWithDescriptors backs public schemas with descriptors.
	PublicSchemasWithDescriptors
	// EnsureSpanConfigReconciliation ensures that the host tenant has run its
	// reconciliation process at least once.
	EnsureSpanConfigReconciliation
	// EnsureSpanConfigSubscription ensures that all KV nodes are subscribed to
	// the global span configuration state, observing the entries installed as
	// in EnsureSpanConfigReconciliation.
	EnsureSpanConfigSubscription
	// EnableSpanConfigStore enables the use of the span configs infrastructure
	// in KV.
	EnableSpanConfigStore
	// EnableLeaseHolderRemoval enables removing a leaseholder and transferring the lease
	// during joint configuration, including to VOTER_INCOMING replicas.
	EnableLeaseHolderRemoval
	// ChangefeedIdleness is the version where changefeed aggregators forward
	// idleness-related information alnog with resolved spans to the frontier
	ChangefeedIdleness
	// RowLevelTTL is the version where we allow row level TTL tables.
	RowLevelTTL
	// PebbleFormatSplitUserKeysMarked performs a Pebble-level migration and
	// upgrades the Pebble format major version to FormatSplitUserKeysMarked.
	PebbleFormatSplitUserKeysMarked
	// EnableNewStoreRebalancer enables the new store rebalancer introduced in
	// 22.1.
	EnableNewStoreRebalancer
	// ClusterLocksVirtualTable enables querying the crdb_internal.cluster_locks
	// virtual table, which sends a QueryLocksRequest RPC to all cluster ranges.
	ClusterLocksVirtualTable
	// AutoStatsTableSettings is the version where we allow auto stats related
	// table settings.
	AutoStatsTableSettings
	// SuperRegions enables the usage on super regions.
	SuperRegions
	// EnableNewChangefeedOptions enables the usage of new changefeed options
	// such as end_time, initial_scan_only, and setting the value of initial_scan
	// to 'yes|no|only'
	EnableNewChangefeedOptions
	// SpanCountTable adds system.span_count to track the number of committed
	// tenant spans.
	SpanCountTable
	// PreSeedSpanCountTable precedes PreSeedSpanCountTable, it enables span
	// accounting for incremental schema changes.
	PreSeedSpanCountTable
	// SeedSpanCountTable seeds system.span_count with the number of committed
	// tenant spans.
	SeedSpanCountTable

	// V22_1 is CockroachDB v22.1. It's used for all v22.1.x patch releases.
	V22_1

	// v22.2 versions.
	//
	// Start22_2 demarcates work towards CockroachDB v22.2.
	Start22_2

	// LocalTimestamps enables the use of local timestamps in MVCC values.
	LocalTimestamps
	// PebbleFormatSplitUserKeysMarkedCompacted updates the Pebble format
	// version that recombines all user keys that may be split across multiple
	// files into a single table.
	PebbleFormatSplitUserKeysMarkedCompacted
	// EnsurePebbleFormatVersionRangeKeys is the first step of a two-part
	// migration that bumps Pebble's format major version to a version that
	// supports range keys.
	EnsurePebbleFormatVersionRangeKeys
	// EnablePebbleFormatVersionRangeKeys is the second of a two-part migration
	// and is used as the feature gate for use of range keys. Any node at this
	// version is guaranteed to reside in a cluster where all nodes support range
	// keys at the Pebble layer.
	EnablePebbleFormatVersionRangeKeys
	// TrigramInvertedIndexes enables the creation of trigram inverted indexes
	// on strings.
	TrigramInvertedIndexes
	// RemoveGrantPrivilege is the last step to migrate from the GRANT privilege to WITH GRANT OPTION.
	RemoveGrantPrivilege
	// MVCCRangeTombstones enables the use of MVCC range tombstones.
	MVCCRangeTombstones
	// UpgradeSequenceToBeReferencedByID ensures that sequences are referenced
	// by IDs rather than by their names. For example, a column's DEFAULT (or
	// ON UPDATE) expression can be defined to be 'nextval('s')'; we want to be
	// able to refer to sequence 's' by its ID, since 's' might be later renamed.
	UpgradeSequenceToBeReferencedByID
	// SampledStmtDiagReqs enables installing statement diagnostic requests that
	// probabilistically collects stmt bundles, controlled by the user provided
	// sampling rate.
	SampledStmtDiagReqs
	// AddSSTableTombstones allows writing MVCC point tombstones via AddSSTable.
	// Previously, SSTs containing these could error.
	AddSSTableTombstones
	// SystemPrivilegesTable adds system.privileges table.
	SystemPrivilegesTable
	// EnablePredicateProjectionChangefeed indicates that changefeeds support
	// predicates and projections.
	EnablePredicateProjectionChangefeed
	// AlterSystemSQLInstancesAddLocality adds a locality column to the
	// system.sql_instances table.
	AlterSystemSQLInstancesAddLocality
	// SystemExternalConnectionsTable adds system.external_connections table.
	SystemExternalConnectionsTable
	// AlterSystemStatementStatisticsAddIndexRecommendations adds an
	// index_recommendations column to the system.statement_statistics table.
	AlterSystemStatementStatisticsAddIndexRecommendations
	// RoleIDSequence is the version where the system.role_id_sequence exists.
	RoleIDSequence
	// AddSystemUserIDColumn is the version where the system.users table has
	// a user_id column for writes only.
	AddSystemUserIDColumn
	// UsersHaveIDs is the version where all users in the system.users table
	// have ids.
	UsersHaveIDs
	// SetUserIDNotNull sets the user_id column in system.users to not null.
	SetUserIDNotNull
	// SQLSchemaTelemetryScheduledJobs adds an automatic schedule for SQL schema
	// telemetry logging jobs.
	SQLSchemaTelemetryScheduledJobs
	// SchemaChangeSupportsCreateFunction adds support of CREATE FUNCTION
	// statement.
	SchemaChangeSupportsCreateFunction
	// DeleteRequestReturnKey is the version where the DeleteRequest began
	// populating the FoundKey value in the response.
	DeleteRequestReturnKey

	// *************************************************
	// Step (1): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************
)

// TODOPreV21_2 is an alias for V21_2 for use in any version gate/check that
// previously referenced a < 21.2 version until that check/gate can be removed.
const TODOPreV21_2 = V21_2

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
		Key:     PebbleFormatBlockPropertyCollector,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 24},
	},
	{
		Key:     PublicSchemasWithDescriptors,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 34},
	},
	{
		Key:     EnsureSpanConfigReconciliation,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 36},
	},
	{
		Key:     EnsureSpanConfigSubscription,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 38},
	},
	{
		Key:     EnableSpanConfigStore,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 40},
	},
	{
		Key:     EnableLeaseHolderRemoval,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 70},
	},
	{
		Key:     ChangefeedIdleness,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 82},
	},
	{
		Key:     RowLevelTTL,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 88},
	},
	{
		Key:     PebbleFormatSplitUserKeysMarked,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 90},
	},
	{
		Key:     EnableNewStoreRebalancer,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 96},
	},
	{
		Key:     ClusterLocksVirtualTable,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 98},
	},
	{
		Key:     AutoStatsTableSettings,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 100},
	},
	{
		Key:     SuperRegions,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 104},
	},
	{
		Key:     EnableNewChangefeedOptions,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 106},
	},
	{
		Key:     SpanCountTable,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 108},
	},
	{
		Key:     PreSeedSpanCountTable,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 110},
	},
	{
		Key:     SeedSpanCountTable,
		Version: roachpb.Version{Major: 21, Minor: 2, Internal: 112},
	},
	{
		Key:     V22_1,
		Version: roachpb.Version{Major: 22, Minor: 1},
	},

	// v22.2 versions. Internal versions must be even.
	{
		Key:     Start22_2,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 2},
	},
	{
		Key:     LocalTimestamps,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 4},
	},
	{
		Key:     PebbleFormatSplitUserKeysMarkedCompacted,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 6},
	},
	{
		Key:     EnsurePebbleFormatVersionRangeKeys,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 8},
	},
	{
		Key:     EnablePebbleFormatVersionRangeKeys,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 10},
	},
	{
		Key:     TrigramInvertedIndexes,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 12},
	},
	{
		Key:     RemoveGrantPrivilege,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 14},
	},
	{
		Key:     MVCCRangeTombstones,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 16},
	},
	{
		Key:     UpgradeSequenceToBeReferencedByID,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 18},
	},
	{
		Key:     SampledStmtDiagReqs,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 20},
	},
	{
		Key:     AddSSTableTombstones,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 22},
	},
	{
		Key:     SystemPrivilegesTable,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 24},
	},
	{
		Key:     EnablePredicateProjectionChangefeed,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 26},
	},
	{
		Key:     AlterSystemSQLInstancesAddLocality,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 28},
	},
	{
		Key:     SystemExternalConnectionsTable,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 30},
	},
	{
		Key:     AlterSystemStatementStatisticsAddIndexRecommendations,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 32},
	},
	{
		Key:     RoleIDSequence,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 34},
	},
	{
		Key:     AddSystemUserIDColumn,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 36},
	},
	{
		Key:     UsersHaveIDs,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 38},
	},
	{
		Key:     SetUserIDNotNull,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 40},
	},
	{
		Key:     SQLSchemaTelemetryScheduledJobs,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 42},
	},
	{
		Key:     SchemaChangeSupportsCreateFunction,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 44},
	},
	{
		Key:     DeleteRequestReturnKey,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 46},
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
	binaryMinSupportedVersion = ByKey(V21_2)

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
