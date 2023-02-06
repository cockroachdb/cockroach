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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// Key is a unique identifier for a version of CockroachDB.
type Key int

// Version constants. These drive compatibility between versions as well as
// migrations. Before you add a version or consider removing one, please
// familiarize yourself with the rules below.
//
// # Adding Versions
//
// You'll want to add a new one in the following cases:
//
// (a) When introducing a backwards incompatible feature. Broadly, by this we
//
//	 mean code that's structured as follows:
//
//	  if (specific-version is active) {
//	      // Implies that all nodes in the cluster are running binaries that
//	      // have this code. We can "enable" the new feature knowing that
//	      // outbound RPCs, requests, etc. will be handled by nodes that know
//	      // how to do so.
//	  } else {
//	      // There may be some nodes running older binaries without this code.
//	      // To be safe, we'll want to behave as we did before introducing
//	      // this feature.
//	  }
//
//	 Authors of migrations need to be careful in ensuring that end-users
//	 aren't able to enable feature gates before they're active. This is fine:
//
//	  func handleSomeNewStatement() error {
//	      if !(specific-version is active) {
//	          return errors.New("cluster version needs to be bumped")
//	      }
//	      // ...
//	  }
//
//	 At the same time, with requests/RPCs originating at other crdb nodes, the
//	 initiator of the request gets to decide what's supported. A node should
//	 not refuse functionality on the grounds that its view of the version gate
//	 is as yet inactive. Consider the sender:
//
//	  func invokeSomeRPC(req) {
//	      if (specific-version is active) {
//	          // Like mentioned above, this implies that all nodes in the
//	          // cluster are running binaries that can handle this new
//	          // feature. We may have learned about this fact before the
//	          // node on the other end. This is due to the fact that migration
//	          // manager informs each node about the specific-version being
//	          // activated active concurrently. See BumpClusterVersion for
//	          // where that happens. Still, it's safe for us to enable the new
//	          // feature flags as we trust the recipient to know how to deal
//	          // with it.
//	        req.NewFeatureFlag = true
//	      }
//	      send(req)
//	  }
//
//	And consider the recipient:
//
//	 func someRPC(req) {
//	     if !req.NewFeatureFlag {
//	         // Legacy behavior...
//	     }
//	     // There's no need to even check if the specific-version is active.
//	     // If the flag is enabled, the specific-version must have been
//	     // activated, even if we haven't yet heard about it (we will pretty
//	     // soon).
//	 }
//
//	 See clusterversion.Handle.IsActive and usage of some existing versions
//	 below for more clues on the matter.
//
// (b) When cutting a major release branch. When cutting release-20.2 for
//
//	 example, you'll want to introduce the following to `master`.
//
//	   (i)  V20_2 (keyed to v20.2.0-0})
//	   (ii) V21_1Start (keyed to v20.2.0-1})
//
//	You'll then want to backport (i) to the release branch itself (i.e.
//	release-20.2). You'll also want to bump binaryMinSupportedVersion. In the
//	example above, you'll set it to V20_2. This indicates that the
//	minimum binary version required in a cluster with nodes running
//	v21.1 binaries (including pre-release alphas) is v20.2, i.e. that an
//	upgrade into such a binary must start out from at least v20.2 nodes.
//
//	Aside: At the time of writing, the binary min supported version is the
//	last major release, though we may consider relaxing this in the future
//	(i.e. for example could skip up to one major release) as we move to a more
//	frequent release schedule.
//
// When introducing a version constant, you'll want to:
//
//	(1) Prefix its name with the version in which it will be released.
//	(2) Add it at the end of this block. For versions introduced during and
//	    after the 21.1 release, Internal versions must be even-numbered. The
//	    odd versions are used for internal book-keeping. The Internal version
//	    should be the previous Internal version for the same minor release plus
//	    two.
//	(3) Add it at the end of the `versionsSingleton` block below.
//
// # Migrations
//
// Migrations are idempotent functions that can be attached to versions and will
// be rolled out before the respective cluster version gets rolled out. They are
// primarily a means to remove legacy state from the cluster. For example, a
// migration might scan the cluster for an outdated type of table descriptor and
// rewrite it into a new format. Migrations are tricky to get right and they have
// their own documentation in ./pkg/upgrade, which you should peruse should you
// feel that a migration is necessary for your use case.
//
// # Phasing out Versions and Migrations
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
const (
	invalidVersionKey Key = iota - 1 // want first named one to start at zero

	// VPrimordial versions are used by upgrades below BinaryMinSupportedVersion,
	// for whom the exact version they were associated with no longer matters.

	VPrimordial1
	VPrimordial2
	VPrimordial3
	VPrimordial4
	VPrimordial5
	VPrimordial6
	// NOTE(andrei): Do not introduce new upgrades corresponding to VPrimordial
	// versions. Old-version nodes might try to run the jobs created for such
	// upgrades, but they won't know about the respective upgrade, causing the job
	// to succeed without actually performing the update.
	VPrimordialMax

	// V22_1 is CockroachDB v22.1. It's used for all v22.1.x patch releases.
	V22_1

	// v22.2 versions.
	//
	// V22_2Start demarcates work towards CockroachDB v22.2.
	V22_2Start

	// V22_2LocalTimestamps enables the use of local timestamps in MVCC values.
	V22_2LocalTimestamps
	// V22_2PebbleFormatSplitUserKeysMarkedCompacted updates the Pebble format
	// version that recombines all user keys that may be split across multiple
	// files into a single table.
	V22_2PebbleFormatSplitUserKeysMarkedCompacted
	// V22_2EnsurePebbleFormatVersionRangeKeys is the first step of a two-part
	// migration that bumps Pebble's format major version to a version that
	// supports range keys.
	V22_2EnsurePebbleFormatVersionRangeKeys
	// V22_2EnablePebbleFormatVersionRangeKeys is the second of a two-part migration
	// and is used as the feature gate for use of range keys. Any node at this
	// version is guaranteed to reside in a cluster where all nodes support range
	// keys at the Pebble layer.
	V22_2EnablePebbleFormatVersionRangeKeys
	// V22_2TrigramInvertedIndexes enables the creation of trigram inverted indexes
	// on strings.
	V22_2TrigramInvertedIndexes
	// V22_2RemoveGrantPrivilege is the last step to migrate from the GRANT privilege to WITH GRANT OPTION.
	V22_2RemoveGrantPrivilege
	// V22_2MVCCRangeTombstones enables the use of MVCC range tombstones.
	V22_2MVCCRangeTombstones
	// V22_2UpgradeSequenceToBeReferencedByID ensures that sequences are referenced
	// by IDs rather than by their names. For example, a column's DEFAULT (or
	// ON UPDATE) expression can be defined to be 'nextval('s')'; we want to be
	// able to refer to sequence 's' by its ID, since 's' might be later renamed.
	V22_2UpgradeSequenceToBeReferencedByID
	// V22_2SampledStmtDiagReqs enables installing statement diagnostic requests that
	// probabilistically collects stmt bundles, controlled by the user provided
	// sampling rate.
	V22_2SampledStmtDiagReqs
	// V22_2AddSSTableTombstones allows writing MVCC point tombstones via AddSSTable.
	// Previously, SSTs containing these could error.
	V22_2AddSSTableTombstones
	// V22_2SystemPrivilegesTable adds system.privileges table.
	V22_2SystemPrivilegesTable
	// V22_2EnablePredicateProjectionChangefeed indicates that changefeeds support
	// predicates and projections.
	V22_2EnablePredicateProjectionChangefeed
	// V22_2AlterSystemSQLInstancesAddLocality adds a locality column to the
	// system.sql_instances table.
	V22_2AlterSystemSQLInstancesAddLocality
	// V22_2SystemExternalConnectionsTable adds system.external_connections table.
	V22_2SystemExternalConnectionsTable
	// V22_2AlterSystemStatementStatisticsAddIndexRecommendations adds an
	// index_recommendations column to the system.statement_statistics table.
	V22_2AlterSystemStatementStatisticsAddIndexRecommendations
	// V22_2RoleIDSequence is the version where the system.role_id_sequence exists.
	V22_2RoleIDSequence
	// V22_2AddSystemUserIDColumn is the version where the system.users table has
	// a user_id column for writes only.
	V22_2AddSystemUserIDColumn
	// V22_2SystemUsersIDColumnIsBackfilled is the version where all users in the system.users table
	// have ids.
	V22_2SystemUsersIDColumnIsBackfilled
	// V22_2SetSystemUsersUserIDColumnNotNull sets the user_id column in system.users to not null.
	V22_2SetSystemUsersUserIDColumnNotNull
	// V22_2SQLSchemaTelemetryScheduledJobs adds an automatic schedule for SQL schema
	// telemetry logging jobs.
	V22_2SQLSchemaTelemetryScheduledJobs
	// V22_2SchemaChangeSupportsCreateFunction adds support of CREATE FUNCTION
	// statement.
	V22_2SchemaChangeSupportsCreateFunction
	// V22_2DeleteRequestReturnKey is the version where the DeleteRequest began
	// populating the FoundKey value in the response.
	V22_2DeleteRequestReturnKey
	// V22_2PebbleFormatPrePebblev1Marked performs a Pebble-level migration and
	// upgrades the Pebble format major version to FormatPrePebblev1Marked. This
	// migration occurs at the per-store level and is twofold:
	//  - Each store is first bumped to a Pebble format major version that raises
	//  the minimum supported sstable format to (Pebble,v1) (block properties). New
	//  tables generated by Pebble (via compactions / flushes), and tables written
	//  for ingestion will be at table format version (Pebble,v1).
	//  - Each store is then instructed to mark all existing tables that are
	//  pre-Pebblev1 for a low-priority compaction. In a future release of
	//  Cockroach (likely 23.1), a blocking migration will be run to
	//  rewrite-compact on any remaining marked tables.
	V22_2PebbleFormatPrePebblev1Marked
	// V22_2RoleOptionsTableHasIDColumn is the version where the role options table
	// has ids.
	V22_2RoleOptionsTableHasIDColumn
	// V22_2RoleOptionsIDColumnIsBackfilled is the version where ids in the role options
	// table are backfilled.
	V22_2RoleOptionsIDColumnIsBackfilled
	// V22_2SetRoleOptionsUserIDColumnNotNull is the version where the role
	// options table id column cannot be null. This is the final step
	// of the system.role_options table migration.
	V22_2SetRoleOptionsUserIDColumnNotNull
	// V22_2UseDelRangeInGCJob enables the use of the DelRange operation in the
	// GC job. Before it is enabled, the GC job uses ClearRange operations
	// after the job waits out the GC TTL. After it has been enabled, the
	// job instead issues DelRange operations at the beginning of the job
	// and then waits for the data to be removed automatically before removing
	// the descriptor and zone configurations.
	V22_2UseDelRangeInGCJob
	// V22_2WaitedForDelRangeInGCJob corresponds to the migration which waits for
	// the GC jobs to adopt the use of DelRange with tombstones.
	V22_2WaitedForDelRangeInGCJob
	// V22_2RangefeedUseOneStreamPerNode changes rangefeed implementation to use 1 RPC stream per node.
	V22_2RangefeedUseOneStreamPerNode
	// V22_2NoNonMVCCAddSSTable adds a migration which waits for all
	// schema changes to complete. After this point, no non-MVCC
	// AddSSTable calls will be used outside of tenant streaming.
	V22_2NoNonMVCCAddSSTable
	// V22_2GCHintInReplicaState adds GC hint to replica state. When this version is
	// enabled, replicas will populate GC hint and update them when necessary.
	V22_2GCHintInReplicaState
	// V22_2UpdateInvalidColumnIDsInSequenceBackReferences looks for invalid column
	// ids in sequences' back references and attempts a best-effort-based matching
	// to update those column IDs.
	V22_2UpdateInvalidColumnIDsInSequenceBackReferences
	// V22_2TTLDistSQL uses DistSQL to distribute TTL SELECT/DELETE statements to
	// leaseholder nodes.
	V22_2TTLDistSQL
	// V22_2PrioritizeSnapshots adds prioritization to sender snapshots. When this
	// version is enabled, the receiver will look at the priority of snapshots
	// using the fields added in 22.2.
	V22_2PrioritizeSnapshots
	// V22_2EnableLeaseUpgrade version gates a change in the lease transfer protocol
	// whereby we only ever transfer expiration-based leases (and have
	// recipients later upgrade them to the more efficient epoch based ones).
	// This was done to limit the effects of ill-advised lease transfers since
	// the incoming leaseholder would need to recognize itself as such within a
	// few seconds. This needs version gating so that in mixed-version clusters,
	// as part of lease transfers, we don't start sending out expiration based
	// leases to nodes that (i) don't expect them for certain keyspans, and (ii)
	// don't know to upgrade them to efficient epoch-based ones.
	V22_2EnableLeaseUpgrade
	// V22_2SupportAssumeRoleAuth is the version where assume role authorization is
	// supported in cloud storage and KMS.
	V22_2SupportAssumeRoleAuth
	// V22_2FixUserfileRelatedDescriptorCorruption adds a migration which uses
	// heuristics to identify invalid table descriptors for userfile-related
	// descriptors.
	V22_2FixUserfileRelatedDescriptorCorruption

	// V22_2 is CockroachDB v22.2. It's used for all v22.2.x patch releases.
	V22_2

	// V23_1_Start demarcates the start of cluster versions stepped through during
	// the process of upgrading from 22.2 to 23.1.
	V23_1Start

	// V23_1TenantNamesStateAndServiceMode adds columns to system.tenants.
	V23_1TenantNamesStateAndServiceMode

	// V23_1DescIDSequenceForSystemTenant migrates the descriptor ID generator
	// counter from a meta key to the system.descriptor_id_seq sequence for the
	// system tenant.
	V23_1DescIDSequenceForSystemTenant

	// V23_1AddPartialStatisticsColumns adds two columns: one to store the predicate
	// for a partial statistics collection, and another to refer to the full statistic
	// it was collected from.
	V23_1AddPartialStatisticsColumns

	// V23_1_CreateSystemJobInfoTable creates the system.job_info table.
	V23_1CreateSystemJobInfoTable

	// V23_1RoleMembersTableHasIDColumns is the version where the role_members
	// system table has columns for ids.
	V23_1RoleMembersTableHasIDColumns

	// V23_1RoleMembersIDColumnsBackfilled is the version where the columns for
	// ids in the role_members system table have been backfilled.
	V23_1RoleMembersIDColumnsBackfilled

	// V23_1ScheduledChangefeeds is the version where scheduled changefeeds are
	// supported through `CREATE SCHEDULE FOR CHANGEFEED` statement.
	V23_1ScheduledChangefeeds

	// V23_1AddTypeColumnToJobsTable adds the nullable job_type
	// column to the system.jobs table.
	V23_1AddTypeColumnToJobsTable

	// V23_1BackfillTypeColumnInJobsTable backfills the job_type
	// column in the system.jobs table.
	V23_1BackfillTypeColumnInJobsTable

	// V23_1_AlterSystemStatementStatisticsAddIndexesUsage creates indexes usage virtual column
	// based on (statistics->>'indexes') with inverted index on table system.statement_statistics.
	V23_1_AlterSystemStatementStatisticsAddIndexesUsage

	// V23_1EnsurePebbleFormatSSTableValueBlocks upgrades the Pebble format major
	// version to FormatSSTableValueBlocks, which supports writing sstables in a
	// new format containing value blocks (sstable.TableFormatPebblev3). As part
	// of this upgrade, a preceding Pebble format major version
	// (FormatPrePebblev1MarkedCompacted) upgrade also occurs.
	//
	// Only a Pebble version that has upgraded to FormatSSTableValueBlocks can
	// read sstables with format sstable.TableFormatPebblev3 -- i.e., it is
	// insufficient for the Pebble code to be recent enough. Hence, this is the
	// first step of a two-part migration, and we cannot do this in a single
	// step. With a single step migration, consider a cluster with nodes N1 and
	// N2: once both are on V23_1EnsurePebbleFormatSSTableValueBlocks, N1 is notified
	// of the change and upgrades the format major version in Pebble and starts
	// constructing sstables (say for range snapshot ingestion) in this new
	// format. This sstable could be sent to N2 before N2 has been notified
	// about V23_1EnsurePebbleFormatSSTableValueBlocks, which will cause the sstable
	// ingestion to fail.
	V23_1EnsurePebbleFormatSSTableValueBlocks

	// V23_1EnablePebbleFormatSSTableValueBlocks is the second step of the
	// two-part migration. When this starts we are sure that all Pebble
	// instances in the cluster have already upgraded to format major version
	// FormatSSTableValueBlocks.
	V23_1EnablePebbleFormatSSTableValueBlocks

	// V23_1AlterSystemSQLInstancesAddSqlAddr adds a sql_addr column to the
	// system.sql_instances table.
	V23_1AlterSystemSQLInstancesAddSQLAddr

	// V23_1_ChangefeedExpressionProductionReady marks changefeed expressions (transformation)
	// as production ready.  This gate functions as a signal to attempt to upgrade
	// chagnefeeds created prior to this version.
	V23_1_ChangefeedExpressionProductionReady

	// V23_1KeyVisualizerTablesAndJobs adds the system tables that support the key visualizer.
	V23_1KeyVisualizerTablesAndJobs

	// V23_1_KVDirectColumnarScans introduces the support of the "direct"
	// columnar scans in the KV layer.
	V23_1_KVDirectColumnarScans

	V23_1_DeleteDroppedFunctionDescriptors

	// *************************************************
	// Step (1): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************
)

func (k Key) String() string {
	return ByKey(k).String()
}

// TODOPreV22_1 is an alias for V22_1 for use in any version gate/check that
// previously referenced a < 22.1 version until that check/gate can be removed.
const TODOPreV22_1 = V22_1

// Offset every version +1M major versions into the future if this is a dev branch.
const DevOffset = 1000000

// rawVersionsSingleton lists all historical versions here in chronological
// order, with comments describing what backwards-incompatible features were
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
//
// rawVersionsSingleton is converted to versionsSingleton below, by adding a
// large number to every major if building from master, so as to ensure that
// master builds cannot be upgraded to release-branch builds.
var rawVersionsSingleton = keyedVersions{
	{
		Key:     VPrimordial1,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 2},
	},
	{
		Key:     VPrimordial2,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 4},
	},
	{
		Key:     VPrimordial3,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 6},
	},
	{
		Key:     VPrimordial4,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 8},
	},
	{
		Key:     VPrimordial5,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 10},
	},
	{
		Key:     VPrimordial6,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 12},
	},
	{
		Key:     VPrimordialMax,
		Version: roachpb.Version{Major: 0, Minor: 0, Internal: 424242},
	},
	{
		Key:     V22_1,
		Version: roachpb.Version{Major: 22, Minor: 1},
	},

	// v22.2 versions. Internal versions must be even.
	{
		Key:     V22_2Start,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 2},
	},
	{
		Key:     V22_2LocalTimestamps,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 4},
	},
	{
		Key:     V22_2PebbleFormatSplitUserKeysMarkedCompacted,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 6},
	},
	{
		Key:     V22_2EnsurePebbleFormatVersionRangeKeys,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 8},
	},
	{
		Key:     V22_2EnablePebbleFormatVersionRangeKeys,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 10},
	},
	{
		Key:     V22_2TrigramInvertedIndexes,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 12},
	},
	{
		Key:     V22_2RemoveGrantPrivilege,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 14},
	},
	{
		Key:     V22_2MVCCRangeTombstones,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 16},
	},
	{
		Key:     V22_2UpgradeSequenceToBeReferencedByID,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 18},
	},
	{
		Key:     V22_2SampledStmtDiagReqs,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 20},
	},
	{
		Key:     V22_2AddSSTableTombstones,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 22},
	},
	{
		Key:     V22_2SystemPrivilegesTable,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 24},
	},
	{
		Key:     V22_2EnablePredicateProjectionChangefeed,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 26},
	},
	{
		Key:     V22_2AlterSystemSQLInstancesAddLocality,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 28},
	},
	{
		Key:     V22_2SystemExternalConnectionsTable,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 30},
	},
	{
		Key:     V22_2AlterSystemStatementStatisticsAddIndexRecommendations,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 32},
	},
	{
		Key:     V22_2RoleIDSequence,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 34},
	},
	{
		Key:     V22_2AddSystemUserIDColumn,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 36},
	},
	{
		Key:     V22_2SystemUsersIDColumnIsBackfilled,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 38},
	},
	{
		Key:     V22_2SetSystemUsersUserIDColumnNotNull,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 40},
	},
	{
		Key:     V22_2SQLSchemaTelemetryScheduledJobs,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 42},
	},
	{
		Key:     V22_2SchemaChangeSupportsCreateFunction,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 44},
	},
	{
		Key:     V22_2DeleteRequestReturnKey,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 46},
	},
	{
		Key:     V22_2PebbleFormatPrePebblev1Marked,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 48},
	},
	{
		Key:     V22_2RoleOptionsTableHasIDColumn,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 50},
	},
	{
		Key:     V22_2RoleOptionsIDColumnIsBackfilled,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 52},
	},
	{
		Key:     V22_2SetRoleOptionsUserIDColumnNotNull,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 54},
	},
	{
		Key:     V22_2UseDelRangeInGCJob,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 56},
	},
	{
		Key:     V22_2WaitedForDelRangeInGCJob,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 58},
	},
	{
		Key:     V22_2RangefeedUseOneStreamPerNode,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 60},
	},
	{
		Key:     V22_2NoNonMVCCAddSSTable,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 62},
	},
	{
		Key:     V22_2GCHintInReplicaState,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 64},
	},
	{
		Key:     V22_2UpdateInvalidColumnIDsInSequenceBackReferences,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 66},
	},
	{
		Key:     V22_2TTLDistSQL,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 68},
	},
	{
		Key:     V22_2PrioritizeSnapshots,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 70},
	},
	{
		Key:     V22_2EnableLeaseUpgrade,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 72},
	},
	{
		Key:     V22_2SupportAssumeRoleAuth,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 74},
	},
	{
		Key:     V22_2FixUserfileRelatedDescriptorCorruption,
		Version: roachpb.Version{Major: 22, Minor: 1, Internal: 76},
	},
	{
		Key:     V22_2,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 0},
	},
	{
		Key:     V23_1Start,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 2},
	},
	{
		Key:     V23_1TenantNamesStateAndServiceMode,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 4},
	},
	{
		Key:     V23_1DescIDSequenceForSystemTenant,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 6},
	},
	{
		Key:     V23_1AddPartialStatisticsColumns,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 8},
	},
	{
		Key:     V23_1CreateSystemJobInfoTable,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 10},
	},
	{
		Key:     V23_1RoleMembersTableHasIDColumns,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 12},
	},
	{
		Key:     V23_1RoleMembersIDColumnsBackfilled,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 14},
	},
	{
		Key:     V23_1ScheduledChangefeeds,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 16},
	},
	{
		Key:     V23_1AddTypeColumnToJobsTable,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 18},
	},
	{
		Key:     V23_1BackfillTypeColumnInJobsTable,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 20},
	},
	{
		Key:     V23_1_AlterSystemStatementStatisticsAddIndexesUsage,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 22},
	},
	{
		Key:     V23_1EnsurePebbleFormatSSTableValueBlocks,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 24},
	},
	{
		Key:     V23_1EnablePebbleFormatSSTableValueBlocks,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 26},
	},
	{
		Key:     V23_1AlterSystemSQLInstancesAddSQLAddr,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 28},
	},
	{
		Key:     V23_1_ChangefeedExpressionProductionReady,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 30},
	},
	{
		Key:     V23_1KeyVisualizerTablesAndJobs,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 32},
	},
	{
		Key:     V23_1_KVDirectColumnarScans,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 34},
	},
	{
		Key:     V23_1_DeleteDroppedFunctionDescriptors,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 36},
	},

	// *************************************************
	// Step (2): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************
}

// developmentBranch must true on the main development branch but should be set
// to false on a release branch once the set of versions becomes append-only and
// associated upgrade implementations are frozen. It can be forced to true via
// an env var even on a release branch, to allow running a release binary in a
// dev cluster.
var developmentBranch = true || envutil.EnvOrDefaultBool("COCKROACH_FORCE_DEV_VERSION", false)

const (
	// finalVersion should be set on a release branch to the minted final cluster
	// version key, e.g. to V22_2 on the release-22.2 branch once it is minted.
	// Setting it has the effect of ensuring no versions are subsequently added.
	finalVersion = invalidVersionKey
)

var allowUpgradeToDev = envutil.EnvOrDefaultBool("COCKROACH_UPGRADE_TO_DEV_VERSION", false)

var versionsSingleton = func() keyedVersions {
	if developmentBranch {
		// If this is a dev branch, we offset every version +1M major versions into
		// the future. This means a cluster that runs the migrations in a dev build,
		// while they are still in flux, will persist this offset version, and thus
		// cannot then "upgrade" to the released build, as its non-offset versions
		// would then be a downgrade, which is blocked.
		//
		// By default, when offsetting versions in a dev binary, we offset *all of
		// them*, which includes the minimum version from upgrades are supported.
		// This means a dev binary cannot join, resume or upgrade a release version
		// cluster, which is by design as it avoids unintentionally but irreversibly
		// upgrading a cluster to dev versions. Opting in to such an upgrade is
		// possible however via setting COCKROACH_UPGRADE_TO_DEV_VERSION. Doing so
		// skips offsetting the earliest version this binary supports, meaning it
		// will support an upgrade from as low as that released version that then
		// advances into the dev-numbered versions.
		//
		// Note that such upgrades may in fact be a *downgrade* of the logical
		// version! For example, on a cluster that is on released version 3, a dev
		// binary containing versions 1, 2, 3, and 4 started with this flag would
		// renumber only 2-4 to be +1M. It would then step from 3 "up" to 1000002 --
		// which conceptually is actually back down to 2 -- then back to to 1000003,
		// then on to 1000004, etc.
		skipFirst := allowUpgradeToDev
		first := true
		for i := range rawVersionsSingleton {
			// VPrimordial versions are not offset; they don't matter for the logic
			// offsetting is used for.
			if rawVersionsSingleton[i].Major == rawVersionsSingleton.MustByKey(VPrimordialMax).Major {
				continue
			}

			if skipFirst && first {
				first = false
				continue
			}
			rawVersionsSingleton[i].Major += DevOffset
		}
	}
	return rawVersionsSingleton
}()

// V23_1 is a placeholder that will eventually be replaced by the actual 23.1
// version Key, but in the meantime it points to the latest Key. The placeholder
// is defined so that it can be referenced in code that simply wants to check if
// a cluster is running 23.1 and has completed all associated migrations; most
// version gates can use this instead of defining their own version key if all
// simply need to check is that the cluster has upgraded to 23.1.
var V23_1 = versionsSingleton[len(versionsSingleton)-1].Key

const (
	BinaryMinSupportedVersionKey = V22_2
)

// TODO(irfansharif): clusterversion.binary{,MinimumSupported}Version
// feels out of place. A "cluster version" and a "binary version" are two
// separate concepts.
var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error. This typically trails the current release by one (see top-level
	// comment).
	binaryMinSupportedVersion = ByKey(BinaryMinSupportedVersionKey)

	BinaryVersionKey = V23_1
	// binaryVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	binaryVersion = ByKey(BinaryVersionKey)
)

func init() {
	if finalVersion > invalidVersionKey {
		if binaryVersion != ByKey(finalVersion) {
			panic("binary version does not match final version")
		}
	} else if binaryVersion.Internal == 0 {
		panic("a non-upgrade cluster version must be the final version")
	}
}

// ByKey returns the roachpb.Version for a given key.
// It is a fatal error to use an invalid key.
func ByKey(key Key) roachpb.Version {
	return versionsSingleton.MustByKey(key)
}

// ListBetween returns the list of cluster versions in the range
// (from, to].
func ListBetween(from, to roachpb.Version) []roachpb.Version {
	return listBetweenInternal(from, to, versionsSingleton)
}

func listBetweenInternal(from, to roachpb.Version, vs keyedVersions) []roachpb.Version {
	var cvs []roachpb.Version
	for _, keyedV := range vs {
		// Read: "from < keyedV <= to".
		if from.Less(keyedV.Version) && keyedV.Version.LessEq(to) {
			cvs = append(cvs, keyedV.Version)
		}
	}
	return cvs
}
