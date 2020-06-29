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

// Version constants.
//
// To add a version:
//   - Add it at the end of this block.
//   - Add it at the end of the `Versions` block below.
//   - For major or minor versions, bump binaryMinSupportedVersion. For
//     example, if introducing the `20.1` release, bump it to
//     VersionStart19_2 (i.e. `19.1-1`).
//
// To delete a version.
//   - Remove its associated runtime checks.
//   - If the version is not the latest one, delete the constant, comment out
//     its stanza, and say "Removed." above the versionsSingleton.
//
//go:generate stringer -type=VersionKey
const (
	_ VersionKey = iota - 1 // want first named one to start at zero
	Version19_1
	VersionStart19_2
	VersionLearnerReplicas
	VersionTopLevelForeignKeys
	VersionAtomicChangeReplicasTrigger
	VersionAtomicChangeReplicas
	VersionTableDescModificationTimeFromMVCC
	VersionPartitionedBackup
	Version19_2
	VersionStart20_1
	VersionContainsEstimatesCounter
	VersionChangeReplicasDemotion
	VersionSecondaryIndexColumnFamilies
	VersionNamespaceTableWithSchemas
	VersionProtectedTimestamps
	VersionPrimaryKeyChanges
	VersionAuthLocalAndTrustRejectMethods
	VersionPrimaryKeyColumnsOutOfFamilyZero
	VersionRootPassword
	VersionNoExplicitForeignKeyIndexIDs
	VersionHashShardedIndexes
	VersionCreateRolePrivilege
	VersionStatementDiagnosticsSystemTables
	VersionSchemaChangeJob
	VersionSavepoints
	VersionTimeTZType
	VersionTimePrecision
	Version20_1
	VersionStart20_2
	VersionGeospatialType
	VersionEnums
	VersionRangefeedLeases
	VersionAlterColumnTypeGeneral
	VersionAlterSystemJobsAddCreatedByColumns
	VersionAddScheduledJobsTable
	VersionUserDefinedSchemas
	VersionNoOriginFKIndexes

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
// be monotonic. Once we've added 1.1-0,  we can't slot in 1.0-4 because
// clusters already running 1.1-0 won't migrate through the new 1.0-4 version.
// Such clusters would need to be wiped. As a result, do not bump the major or
// minor version until we are absolutely sure that no new migrations will need
// to be added (i.e., when cutting the final release candidate).
var versionsSingleton = keyedVersions([]keyedVersion{
	//{
	// Removed
	// VersionUnreplicatedRaftTruncatedState is https://github.com/cockroachdb/cockroach/pull/34660.
	// When active, it moves the truncated state into unreplicated keyspace
	// on log truncations.
	//
	// The migration works as follows:
	//
	// 1. at any log position, the replicas of a Range either use the new
	// (unreplicated) key or the old one, and exactly one of them exists.
	//
	// 2. When a log truncation evaluates under the new cluster version,
	// it initiates the migration by deleting the old key. Under the old cluster
	// version, it behaves like today, updating the replicated truncated state.
	//
	// 3. The deletion signals new code downstream of Raft and triggers a write
	// to the new, unreplicated, key (atomic with the deletion of the old key).
	//
	// 4. Future log truncations don't write any replicated data any more, but
	// (like before) send along the TruncatedState which is written downstream
	// of Raft atomically with the deletion of the log entries. This actually
	// uses the same code as 3.
	// What's new is that the truncated state needs to be verified before
	// replacing a previous one. If replicas disagree about their truncated
	// state, it's possible for replica X at FirstIndex=100 to apply a
	// truncated state update that sets FirstIndex to, say, 50 (proposed by a
	// replica with a "longer" historical log). In that case, the truncated
	// state update must be ignored (this is straightforward downstream-of-Raft
	// code).
	//
	// 5. When a split trigger evaluates, it seeds the RHS with the legacy
	// key iff the LHS uses the legacy key, and the unreplicated key otherwise.
	// This makes sure that the invariant that all replicas agree on the
	// state of the migration is upheld.
	//
	// 6. When a snapshot is applied, the receiver is told whether the snapshot
	// contains a legacy key. If not, it writes the truncated state (which is
	// part of the snapshot metadata) in its unreplicated version. Otherwise
	// it doesn't have to do anything (the range will migrate later).
	//
	// The following diagram visualizes the above. Note that it abuses sequence
	// diagrams to get a nice layout; the vertical lines belonging to NewState
	// and OldState don't imply any particular ordering of operations.
	//
	// ┌────────┐                            ┌────────┐
	// │OldState│                            │NewState│
	// └───┬────┘                            └───┬────┘
	//     │                        Bootstrap under old version
	//     │ <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
	//     │                                     │
	//     │                                     │     Bootstrap under new version
	//     │                                     │ <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
	//     │                                     │
	//     │─ ─ ┐
	//     │    | Log truncation under old version
	//     │< ─ ┘
	//     │                                     │
	//     │─ ─ ┐                                │
	//     │    | Snapshot                       │
	//     │< ─ ┘                                │
	//     │                                     │
	//     │                                     │─ ─ ┐
	//     │                                     │    | Snapshot
	//     │                                     │< ─ ┘
	//     │                                     │
	//     │   Log truncation under new version  │
	//     │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─>│
	//     │                                     │
	//     │                                     │─ ─ ┐
	//     │                                     │    | Log truncation under new version
	//     │                                     │< ─ ┘
	//     │                                     │
	//     │                                     │─ ─ ┐
	//     │                                     │    | Log truncation under old version
	//     │                                     │< ─ ┘ (necessarily running new binary)
	//
	// Source: http://www.plantuml.com/plantuml/uml/ and the following input:
	//
	// @startuml
	// scale 600 width
	//
	// OldState <--] : Bootstrap under old version
	// NewState <--] : Bootstrap under new version
	// OldState --> OldState : Log truncation under old version
	// OldState --> OldState : Snapshot
	// NewState --> NewState : Snapshot
	// OldState --> NewState : Log truncation under new version
	// NewState --> NewState : Log truncation under new version
	// NewState --> NewState : Log truncation under old version\n(necessarily running new binary)
	// @enduml

	//Key:     VersionUnreplicatedRaftTruncatedState,
	//Version: roachpb.Version{Major: 2, Minor: 1, Unstable: 6},
	//},
	{
		// Version19_1 is CockroachDB v19.1. It's used for all v19.1.x patch releases.
		Key:     Version19_1,
		Version: roachpb.Version{Major: 19, Minor: 1},
	},
	{
		// VersionStart19_2 demarcates work towards CockroachDB v19.2.
		Key:     VersionStart19_2,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 1},
	},
	// Removed.
	// {
	// 	// VersionQueryTxnTimestamp is https://github.com/cockroachdb/cockroach/pull/36307.
	// 	Key:     VersionQueryTxnTimestamp,
	// 	Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 2},
	// },
	// Removed.
	// {
	// 	// VersionStickyBit is https://github.com/cockroachdb/cockroach/pull/37506.
	// 	Key:     VersionStickyBit,
	// 	Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 3},
	// },
	// Removed.
	// {
	// 	// VersionParallelCommits is https://github.com/cockroachdb/cockroach/pull/37777.
	// 	Key:     VersionParallelCommits,
	// 	Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 4},
	// },
	// Removed.
	// {
	// 	// VersionGenerationComparable is https://github.com/cockroachdb/cockroach/pull/38334.
	// 	Key:     VersionGenerationComparable,
	// 	Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 5},
	// },
	{
		// VersionLearnerReplicas is https://github.com/cockroachdb/cockroach/pull/38149.
		Key:     VersionLearnerReplicas,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 6},
	},
	{
		// VersionTopLevelForeignKeys is https://github.com/cockroachdb/cockroach/pull/39173.
		//
		// It represents an upgrade to the table descriptor format in which foreign
		// key references are pulled out of the index descriptors where they
		// originally were kept, and rewritten into a top level field on the index's
		// parent table descriptors. During a mixed-version state, the database will
		// write old-style table descriptors at all system boundaries, but upgrade
		// all old-style table descriptors into the new format upon read. Once the
		// upgrade is finalized, the database will write the upgraded format, but
		// continue to upgrade old-style descriptors on-demand.
		//
		// This version is also used for the new foreign key schema changes which
		// are run in the schema changer, requiring new types of mutations on the
		// table descriptor. The same version is used for both of these changes
		// because the changes are intertwined, and it slightly simplifies some of
		// the logic to assume that either neither or both sets of changes can be
		// active.
		Key:     VersionTopLevelForeignKeys,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 7},
	},
	{
		// VersionAtomicChangeReplicasTrigger is https://github.com/cockroachdb/cockroach/pull/39485.
		//
		// It enables use of updated fields in ChangeReplicasTrigger that will
		// support atomic replication changes.
		Key:     VersionAtomicChangeReplicasTrigger,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 8},
	},
	{
		// VersionAtomicChangeReplicas is https://github.com/cockroachdb/cockroach/pull/39936.
		//
		// It provides an implementation of (*Replica).ChangeReplicas that uses
		// atomic replication changes. The corresponding cluster setting
		// 'kv.atomic_replication_changes.enabled' provides a killswitch (i.e.
		// no atomic replication changes will be scheduled when it is set to
		// 'false').
		Key:     VersionAtomicChangeReplicas,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 9},
	},
	{
		// VersionTableDescModificationTimeFromMVCC is https://github.com/cockroachdb/cockroach/pull/40581
		//
		// It represents an upgrade to the table descriptor format in which
		// CreateAsOfTime and ModifiedTime are set to zero when new versions of
		// table descriptors are written. This removes the need to fix the commit
		// timestamp for transactions which update table descriptors. The value
		// is then populated by the reading client with the MVCC timestamp of the
		// row which contained the serialized table descriptor.
		Key:     VersionTableDescModificationTimeFromMVCC,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 10},
	},
	{
		// VersionPartitionedBackup is https://github.com/cockroachdb/cockroach/pull/39250.
		Key:     VersionPartitionedBackup,
		Version: roachpb.Version{Major: 19, Minor: 1, Unstable: 11},
	},
	{
		// Version19_2 is CockroachDB v19.2. It's used for all v19.2.x patch releases.
		Key:     Version19_2,
		Version: roachpb.Version{Major: 19, Minor: 2},
	},
	{
		// VersionStart20_1 demarcates work towards CockroachDB v20.1.
		Key:     VersionStart20_1,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 1},
	},
	{
		// VersionContainsEstimatesCounter is https://github.com/cockroachdb/cockroach/pull/37583.
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
		Key:     VersionContainsEstimatesCounter,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 2},
	},
	{
		// VersionChangeReplicasDemotion enables the use of voter demotions
		// during replication changes that remove (one or more) voters.
		// When this version is active, voters that are being removed transition
		// first into VOTER_DEMOTING (a joint configuration) and from there to
		// LEARNER, before they are actually removed. This added intermediate
		// step avoids losing quorum when the leaseholder crashes at an
		// inopportune moment.
		//
		// For example, without this version active, with nodes n1-n4 and a
		// range initially replicated on n1, n3, and n4, a rebalance operation
		// that wants to swap n1 for n2 first transitions into the joint
		// configuration `(n1 n3 n4) && (n2 n3 n4)`, that is, n2 is
		// VOTER_OUTGOING. After this is committed and applied (say by
		// everyone), the configuration entry for the final configuration
		// `(n2 n3 n4)` is distributed:
		//
		//- the leader is n3
		//- conf entry reaches n1, n2, n3 (so it is committed under the joint config)
		//- n1 applies conf change and immediately disappears (via replicaGC,
		//  since it's not a part of the latest config)
		//- n3 crashes
		//
		// At this point, the remaining replicas n4 and n2 form a quorum of the
		// latest committed configuration, but both still have the joint
		// configuration active, which cannot reach quorum any more.
		// The intermediate learner step added by this version makes sure that
		// n1 is still available at this point to help n2 win an election, and
		// due to the invariant that replicas never have more than one unappliable
		// configuration change in their logs, the group won't lose availability
		// when the leader instead crashes while removing the learner.
		Key:     VersionChangeReplicasDemotion,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 3},
	},
	{
		// VersionSecondaryIndexColumnFamilies is https://github.com/cockroachdb/cockroach/pull/42073.
		//
		// It allows secondary indexes to respect table level column family definitions.
		Key:     VersionSecondaryIndexColumnFamilies,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 4},
	},
	{
		// VersionNamespaceTableWithSchemas is https://github.com/cockroachdb/cockroach/pull/41977
		//
		// It represents the migration to a new system.namespace table that has an
		// added parentSchemaID column. In addition to the new column, the table is
		// no longer in the system config range -- implying it is no longer gossiped.
		Key:     VersionNamespaceTableWithSchemas,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 5},
	},
	{
		// VersionProtectedTimestamps introduces the system tables for the protected
		// timestamps subsystem.
		//
		// In this version and later the system.protected_ts_meta and
		// system.protected_ts_records tables are part of the system bootstap
		// schema.
		Key:     VersionProtectedTimestamps,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 6},
	},
	{
		// VersionPrimaryKeyChanges is https://github.com/cockroachdb/cockroach/pull/42462
		//
		// It allows online primary key changes of tables.
		Key:     VersionPrimaryKeyChanges,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 7},
	},
	{
		// VersionAuthLocalAndTrustRejectMethods introduces the HBA rule
		// prefix 'local' and auth methods 'trust' and 'reject', for use
		// in server.host_based_authentication.configuration.
		//
		// A separate cluster version ensures the new syntax is not
		// introduced while previous-version nodes are still running, as
		// this would block any new SQL client.
		Key:     VersionAuthLocalAndTrustRejectMethods,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 8},
	},
	{
		// VersionPrimaryKeyColumnsOutOfFamilyZero allows for primary key columns
		// to exist in column families other than 0, in order to prepare for
		// primary key changes that move primary key columns to different families.
		Key:     VersionPrimaryKeyColumnsOutOfFamilyZero,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 9},
	},
	{
		// VersionRootPassword enables setting a password for the `root`
		// user from SQL. Even though introducing a password for the root
		// user in 20.1 does not prevent 19.2 nodes from using the root
		// account, we need a cluster setting: the 19.2 nodes do not even
		// *check* the password, so setting a pw in a hybrid 20.1/19.2
		// cluster would yield different client auth successes in
		// different nodes (which is poor UX).
		Key:     VersionRootPassword,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 10},
	},
	{
		// VersionNoExplicitForeignKeyIndexIDs is https://github.com/cockroachdb/cockroach/pull/43332.
		//
		// It represents the migration away from using explicit index IDs in foreign
		// key constraints, and instead allows all places that need these IDs to select
		// an appropriate index to uphold the foreign key relationship.
		Key:     VersionNoExplicitForeignKeyIndexIDs,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 11},
	},
	{
		// VersionHashShardedIndexes is https://github.com/cockroachdb/cockroach/pull/42922
		//
		// It allows the creation of "hash sharded indexes", which construct a hidden
		// shard column, computed from the set of index columns, and prefix the index's
		// ranges with said shard column.
		Key:     VersionHashShardedIndexes,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 12},
	},
	{
		// VersionCreateRolePrivilege is https://github.com/cockroachdb/cockroach/pull/44232.
		//
		// It represents adding role management via CREATEROLE privilege.
		// Added new column in system.users table to track hasCreateRole.
		Key:     VersionCreateRolePrivilege,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 13},
	},
	{
		// VersionStatementDiagnosticsSystemTables introduces the system tables for
		// storing statement information (like traces, bundles).
		// In this version and later the system.statement_diagnostics_requests,
		// system.statement_diagnostics and system.statement_bundle_chunks tables
		// are part of the system bootstap schema.
		Key:     VersionStatementDiagnosticsSystemTables,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 14},
	},
	{
		// VersionSchemaChangeJob is https://github.com/cockroachdb/cockroach/pull/45870.
		//
		// From this version on, schema changes are run as jobs instead of being
		// scheduled by the SchemaChangeManager.
		Key:     VersionSchemaChangeJob,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 15},
	},
	{
		// VersionSavepoints enables the use of SQL savepoints, whereby
		// the ignored seqnum list can become populated in transaction
		// records.
		Key:     VersionSavepoints,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 16},
	},
	{
		// VersionTimeTZType enables the use of the TimeTZ data type.
		Key:     VersionTimeTZType,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 17},
	},
	{
		// VersionTimePrecision enables the use of precision with time/intervals.
		Key:     VersionTimePrecision,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 18},
	},
	{
		// Version20_1 is CockroachDB v20.1. It's used for all v20.1.x patch releases.
		Key:     Version20_1,
		Version: roachpb.Version{Major: 20, Minor: 1},
	},
	{
		// VersionStart20_2 demarcates work towards CockroachDB v20.2.
		Key:     VersionStart20_2,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 1},
	},
	{

		// VersionGeospatialType enables the use of Geospatial features.
		Key:     VersionGeospatialType,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 2},
	},
	{
		// VersionEnums enables the use of ENUM types.
		Key:     VersionEnums,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 3},
	},
	{

		// VersionRangefeedLeases is the enablement of leases uses rangefeeds.
		// All nodes with this versions will have rangefeeds enabled on all system
		// ranges. Once this version is finalized, gossip is not needed in the
		// schema lease subsystem. Nodes which start with this version finalized
		// will not pass gossip to the SQL layer.
		Key:     VersionRangefeedLeases,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 4},
	},
	{
		// VersionAlterColumnTypeGeneral enables the use of alter column type for
		// conversions that require the column data to be rewritten.
		Key:     VersionAlterColumnTypeGeneral,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 5},
	},
	{
		// VersionAlterSystemJobsTable is a version which modified system.jobs table.
		Key:     VersionAlterSystemJobsAddCreatedByColumns,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 6},
	},
	{
		// VersionAddScheduledJobsTable is a version which adds system.scheduled_jobs table.
		Key:     VersionAddScheduledJobsTable,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 7},
	},
	{
		// VersionUserDefinedSchemas enables the creation of user defined schemas.
		Key:     VersionUserDefinedSchemas,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 8},
	},
	{
		// VersionNoOriginFKIndexes allows for foreign keys to no longer need
		// indexes on the origin side of the relationship.
		Key:     VersionNoOriginFKIndexes,
		Version: roachpb.Version{Major: 20, Minor: 1, Unstable: 9},
	},

	// Add new versions here (step two of two).

})

// TODO(irfansharif): clusterversion.binary{,MinimumSupported}Version
// feels out of place. A "cluster version" and a "binary version" are two
// separate concepts.
var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error.
	// We support everything after 19.1, including pre-release 19.2 versions.
	// This is generally beneficial, but in particular it allows the
	// version-upgrade roachtest to use a pre-release 19.2 binary before upgrading
	// to HEAD; if we were to set binaryMinSupportedVersion to Version19_2,
	// that wouldn't work since you'd have to go through the final 19.2 binary
	// before going to HEAD.
	binaryMinSupportedVersion = VersionByKey(Version20_1)

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
