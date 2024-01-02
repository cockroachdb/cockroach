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
// upgrades (formerly "migrations"). Before you add a version or consider
// removing one, please familiarize yourself with the rules below.
//
// # Adding Versions
//
// You'll want to add a new one in the following cases:
//
//	(a) When introducing a backwards incompatible feature. Broadly, by this we
//	mean code that's structured as follows:
//
//		if (specific-version is active) {
//			// Implies that all nodes in the cluster are running binaries that
//			// have this code. We can "enable" the new feature knowing that
//			// outbound RPCs, requests, etc. will be handled by nodes that know
//			// how to do so.
//		} else {
//			// There may be some nodes running older binaries without this code.
//			// To be safe, we'll want to behave as we did before introducing
//			// this feature.
//		}
//
//	Authors of upgrades need to be careful in ensuring that end-users
//	aren't able to enable feature gates before they're active. This is fine:
//
//		func handleSomeNewStatement() error {
//			if !(specific-version is active) {
//				return errors.New("cluster version needs to be bumped")
//	 		}
//			// ...
//		}
//
//	At the same time, with requests/RPCs originating at other crdb nodes, the
//	initiator of the request gets to decide what's supported. A node should
//	not refuse functionality on the grounds that its view of the version gate
//	is as yet inactive. Consider the sender:
//
//		func invokeSomeRPC(req) {
//			if (specific-version is active) {
//				// Like mentioned above, this implies that all nodes in the
//				// cluster are running binaries that can handle this new
//				// feature. We may have learned about this fact before the
//				// node on the other end. This is due to the fact that migration
//				// manager informs each node about the specific-version being
//				// activated active concurrently. See BumpClusterVersion for
//				// where that happens. Still, it's safe for us to enable the new
//				// feature flags as we trust the recipient to know how to deal
//				// with it.
//				req.NewFeatureFlag = true
//			}
//			send(req)
//		}
//
//	And consider the recipient:
//
//		func someRPC(req) {
//			if !req.NewFeatureFlag {
//				// Legacy behavior...
//			}
//			// There's no need to even check if the specific-version is active.
//			// If the flag is enabled, the specific-version must have been
//			// activated, even if we haven't yet heard about it (we will pretty
//			// soon).
//		}
//
//	See clusterversion.Handle.IsActive and usage of some existing versions
//	below for more clues on the matter.
//
//	(b) When cutting a major release branch. When cutting release-20.2 for
//	example, you'll want to introduce the following to `master`.
//
//	   (i)  V20_2 (keyed to v20.2.0-0})
//	   (ii) V21_1Start (keyed to v20.2.0-1})
//
//	You'll then want to backport (i) to the release branch itself (i.e.
//	release-20.2). You'll also want to bump MinSupported. In the
//	example above, you'll set it to V20_2. This indicates that the
//	minimum cluster version required in a cluster with nodes running
//	v21.1 binaries (including pre-release alphas) is v20.2, i.e. that an
//	upgrade to v21.1 must start out from at least v20.2 nodes.
//
//	Aside: At the time of writing, the min supported version is the
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
// # Upgrades
//
// Upgrades are idempotent functions that can be attached to versions and will
// be rolled out before the respective cluster version gets rolled out. They are
// primarily a means to remove legacy state from the cluster. For example, an
// upgrade might scan the cluster for an outdated type of table descriptor and
// rewrite it into a new format. Upgrade are tricky to get right and have their
// own documentation in ./pkg/upgrade, which you should peruse should you feel
// that an upgrade is necessary for your use case.
//
// ## Permanent upgrades
//
// Permanent upgrades are upgrades that double as initialization steps when
// bootstrapping a new cluster. As such, they cannot be removed even as the
// version they are tied to becomes unsupported.
//
// # Phasing out Versions and Upgrades
//
// Versions and non-permanent upgrades can be removed once they are no longer
// going to be exercised. This is primarily driven by the
// MinSupported version, which declares the oldest *cluster* (not binary)
// version of CockroachDB that may interface with the running node. It typically
// trails the current version by one release. For example, if the current branch
// is a `21.1.x` release, you will have MinSupported=`21.0`,
// meaning that the versions 20.2.0-1, 20.2.0-2, etc are always going to be
// active on any peer and thus can be "baked in"; similarly all upgrades
// attached to any of these versions can be assumed to have run (or not having
// been necessary due to the cluster having been initialized at a higher version
// in the first place). Note that this implies that all peers will have a
// *binary* version of at least the MinSupported version as well, as this is a
// prerequisite for running at that cluster version. Finally, note that even
// when all cluster versions known to the current binary are active (i.e. most
// of the time), you still need to be able to inter-op with older *binary*
// and/or *cluster* versions. This is because *tenants* are allowed to run at
// any binary version compatible with (i.e. greater than or equal to) the
// MinSupported version. To give a concrete example, a fully up-to-date v21.1 KV
// host cluster can have tenants running against it that use the v21.0 binary
// and any cluster version known to that binary (v20.2-0 ... v20.2-50 or
// thereabouts).
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
// Permanent upgrades and their associated version key cannot be removed (even
// if it is below MinSupported). The version key should start with `Permanent_`
// to make this more explicit. The version numbers should not be changed - we
// want all nodes in a mixed-version cluster to agree on what version a certain
// upgrade step is tied to (in the unlikely scenario that we have mixed-version
// nodes while bootstrapping a cluster).
const (
	// VPrimordial versions are associated with permanent upgrades that exist for
	// historical reasons; no new primordial versions should be added, and no new
	// upgrades should be tied to existing primordial versions.

	VPrimordial1 Key = iota
	VPrimordial2
	VPrimordial3
	VPrimordial4
	VPrimordial5
	VPrimordial6

	// No new VPrimordial versions should be added.

	VPrimordialMax

	// Permanent_V22_2SQLSchemaTelemetryScheduledJobs adds an automatic schedule for SQL schema
	// telemetry logging jobs.
	// This is a permanent migration which should exist forever.
	Permanent_V22_2SQLSchemaTelemetryScheduledJobs

	// Permanent_V23_1KeyVisualizerTablesAndJobs adds the system tables that
	// support the key visualizer.
	Permanent_V23_1KeyVisualizerTablesAndJobs

	// Permanent_V23_1_CreateJobsMetricsPollingJob creates the permanent job
	// responsible for polling the jobs table for metrics.
	Permanent_V23_1_CreateJobsMetricsPollingJob

	// Permanent_V23_1_CreateAutoConfigRunnerJob is the version where the auto
	// config runner persistent job has been created.
	Permanent_V23_1_CreateAutoConfigRunnerJob

	// Permanent_V23_1ChangeSQLStatsTTL is the version where the gc TTL was
	// updated to all SQL Stats tables.
	Permanent_V23_1ChangeSQLStatsTTL

	// Permanent_V23_1CreateSystemActivityUpdateJob is the version at which
	// Cockroach adds a job that periodically updates the statement_activity and
	// transaction_activity tables.
	Permanent_V23_1CreateSystemActivityUpdateJob

	// V23_1 is CockroachDB v23.1. It's used for all v23.1.x patch releases.
	V23_1

	// V23_2Start demarcates the start of cluster versions stepped through during
	// the process of upgrading from previous supported releases to 23.2.
	V23_2Start

	// V23_2_EnableRangeCoalescingForSystemTenant enables range coalescing for
	// the system tenant.
	V23_2_EnableRangeCoalescingForSystemTenant

	// V23_2_UseACRaftEntryEntryEncodings gates the use of raft entry encodings
	// that (optionally) embed below-raft admission data.
	V23_2_UseACRaftEntryEntryEncodings

	// V23_2_PebbleFormatDeleteSizedAndObsolete upgrades Pebble's format major
	// version to FormatDeleteSizedAndObsolete, allowing use of a new sstable
	// format version Pebblev4. This version has two improvements:
	//   a) It allows the use of DELSIZED point tombstones.
	//   b) It encodes the obsolence of keys in a key-kind bit.
	V23_2_PebbleFormatDeleteSizedAndObsolete

	// V23_2_UseSizedPebblePointTombstones enables the use of Pebble's new
	// DeleteSized operations.
	V23_2_UseSizedPebblePointTombstones

	// V23_2_PebbleFormatVirtualSSTables upgrades Pebble's format major version to
	// FormatVirtualSSTables, allowing use of virtual sstables in Pebble.
	V23_2_PebbleFormatVirtualSSTables

	// V23_2_StmtDiagForPlanGist enables statement diagnostic feature to collect
	// the bundle for particular plan gist.
	V23_2_StmtDiagForPlanGist

	// V23_2_RegionaLivenessTable guarantees the regional liveness table exists
	// and its ready for use.
	V23_2_RegionaLivenessTable

	// V23_2_RemoveLockTableWaiterTouchPush simplifies the push logic in
	// lock_table_waiter by passing the wait policy of the pusher as part of the
	// push request and leaving the push outcome to the server-side logic.
	V23_2_RemoveLockTableWaiterTouchPush

	// V23_2_ChangefeedLaggingRangesOpts is used to version gate the changefeed
	// options lagging_ranges_threshold and lagging_ranges_polling_interval.
	V23_2_ChangefeedLaggingRangesOpts

	// V23_2_GrantExecuteToPublic is no longer used. See #114203.
	V23_2_GrantExecuteToPublic

	// V23_2_EnablePebbleFormatVirtualSSTables enables the Pebble
	// FormatMajorVersion for virtual sstables. Note that the ratcheting for the
	// format major version in Pebble should have happened with
	// V23_2_PebbleFormatVirtualSSTables above.
	V23_2_EnablePebbleFormatVirtualSSTables

	// Permanent_V23_2_MVCCStatisticsTable adds the system.mvcc_statistics
	// table and update job. The table is used to serve fast reads of historical
	// mvcc data from observability surfaces.
	Permanent_V23_2_MVCCStatisticsTable

	// V23_2_AddSystemExecInsightsTable is the version at which Cockroach creates
	// {statement|transaction}_execution_insights system tables.
	V23_2_AddSystemExecInsightsTable

	// ***************************************************************************
	//            WHERE TO ADD VERSION GATES DURING 23.2 STABILITY?
	// ---------------------------------------------------------------------------
	// If version gate is for 23.2 (to be backported to release-23.2):
	//    Then add new gate above this comment (immediately above this comment).
	// If version gate is for 24.1 (upcoming 24.1 development):
	//    Then add new gate at the end (immediately above the "Add new versions
	//    here" comment).
	// ***************************************************************************

	// V23_2 is CockroachDB v23.2. It's used for all v23.2.x patch releases.
	V23_2

	// V24_1Start demarcates the start of cluster versions stepped through during
	// the process of upgrading from 23.2 to 24.1.
	V24_1Start

	// *************************************************
	// Step (1) Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************

	// V24_1_DropPayloadAndProgressFromSystemJobsTable drop the unused payload and
	// progress columns from system.jobs table.
	V24_1_DropPayloadAndProgressFromSystemJobsTable

	numKeys
)

// versionsTable lists all historical versions here in chronological order.
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
// Note that versions in this table can be modified by applying a "dev offset"
// to ensure that upgrades don't occur between in-development and released
// versions (see developmentBranch and maybeApplyDevOffset).
var versionTable = [numKeys]roachpb.Version{
	VPrimordial1:   {Major: 0, Minor: 0, Internal: 2},
	VPrimordial2:   {Major: 0, Minor: 0, Internal: 4},
	VPrimordial3:   {Major: 0, Minor: 0, Internal: 6},
	VPrimordial4:   {Major: 0, Minor: 0, Internal: 8},
	VPrimordial5:   {Major: 0, Minor: 0, Internal: 10},
	VPrimordial6:   {Major: 0, Minor: 0, Internal: 12},
	VPrimordialMax: {Major: 0, Minor: 0, Internal: 424242},

	// Permanent upgrades from previous versions.
	Permanent_V22_2SQLSchemaTelemetryScheduledJobs: {Major: 22, Minor: 1, Internal: 42},
	Permanent_V23_1KeyVisualizerTablesAndJobs:      {Major: 22, Minor: 2, Internal: 32},
	Permanent_V23_1_CreateJobsMetricsPollingJob:    {Major: 22, Minor: 2, Internal: 38},
	Permanent_V23_1_CreateAutoConfigRunnerJob:      {Major: 22, Minor: 2, Internal: 90},
	Permanent_V23_1ChangeSQLStatsTTL:               {Major: 22, Minor: 2, Internal: 98},
	Permanent_V23_1CreateSystemActivityUpdateJob:   {Major: 22, Minor: 2, Internal: 102},

	V23_1: {Major: 23, Minor: 1, Internal: 0},

	// v23.2 versions. Internal versions must be even.
	V23_2Start: {Major: 23, Minor: 1, Internal: 2},
	V23_2_EnableRangeCoalescingForSystemTenant: {Major: 23, Minor: 1, Internal: 8},
	V23_2_UseACRaftEntryEntryEncodings:         {Major: 23, Minor: 1, Internal: 10},
	V23_2_PebbleFormatDeleteSizedAndObsolete:   {Major: 23, Minor: 1, Internal: 12},
	V23_2_UseSizedPebblePointTombstones:        {Major: 23, Minor: 1, Internal: 14},
	V23_2_PebbleFormatVirtualSSTables:          {Major: 23, Minor: 1, Internal: 16},
	V23_2_StmtDiagForPlanGist:                  {Major: 23, Minor: 1, Internal: 18},
	V23_2_RegionaLivenessTable:                 {Major: 23, Minor: 1, Internal: 20},
	V23_2_RemoveLockTableWaiterTouchPush:       {Major: 23, Minor: 1, Internal: 22},
	V23_2_ChangefeedLaggingRangesOpts:          {Major: 23, Minor: 1, Internal: 24},
	V23_2_GrantExecuteToPublic:                 {Major: 23, Minor: 1, Internal: 26},
	V23_2_EnablePebbleFormatVirtualSSTables:    {Major: 23, Minor: 1, Internal: 28},
	Permanent_V23_2_MVCCStatisticsTable:        {Major: 23, Minor: 1, Internal: 30},
	V23_2_AddSystemExecInsightsTable:           {Major: 23, Minor: 1, Internal: 32},

	V23_2: {Major: 23, Minor: 2, Internal: 0},

	// v24.1 versions. Internal versions must be even.
	V24_1Start: {Major: 23, Minor: 2, Internal: 2},

	// *************************************************
	// Step (2): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************

	V24_1_DropPayloadAndProgressFromSystemJobsTable: {Major: 23, Minor: 2, Internal: 4},
}

// Latest is always the highest version key. This is the maximum logical cluster
// version supported by this branch.
const Latest Key = numKeys - 1

// MinSupported is the minimum logical cluster version supported by this branch.
const MinSupported Key = V23_1

// PreviousRelease is the logical cluster version of the previous release.
//
// Note: this is always the last element of SupportedPreviousReleases(); it is
// also provided as a constant for convenience.
const PreviousRelease Key = V23_2

// V24_1 is a placeholder that will eventually be replaced by the actual 24.1
// version Key, but in the meantime it points to the latest Key. The placeholder
// is defined so that it can be referenced in code that simply wants to check if
// a cluster is running 24.1 and has completed all associated migrations; most
// version gates can use this instead of defining their own version key if they
// only need to check that the cluster has upgraded to 24.1.
const V24_1 = Latest

// developmentBranch must be true on the main development branch but should be
// set to false on a release branch once the set of versions becomes append-only
// and associated upgrade implementations are frozen.
//
// It can be forced to a specific value in two circumstances:
//  1. forced to `false` on development branches: this is used for upgrade
//     testing purposes and should never be done in real clusters;
//  2. forced to `true` on release branches: this allows running a release
//     binary in a dev cluster.
//
// See devOffsetKeyStart for more details.
const developmentBranch = true

// finalVersion should be set on a release branch to the minted final cluster
// version key, e.g. to V23_2 on the release-23.2 branch once it is minted.
// Setting it has the effect of ensuring no versions are subsequently added (see
// TestFinalVersion).
const finalVersion Key = -1

// Version returns the roachpb.Version corresponding to a key.
func (k Key) Version() roachpb.Version {
	version := versionTable[k]
	return maybeApplyDevOffset(k, version)
}

// IsFinal returns true if the key corresponds to a final version (as opposed to
// a transitional internal version during upgrade).
func (k Key) IsFinal() bool {
	return k.Version().IsFinal()
}

// ReleaseSeries returns the release series the Key. Specifically:
//   - if the key corresponds to a final version (e.g. V23_2), the result has the
//     same major/minor;
//   - if the key corresponds to a transitional upgrade version (e.g.
//     V23_2SomeFeature with version 23.2-x), the result is the next series
//     (e.g. 24.1).
//
// The key must be in the range [MinSupported, Latest].
//
// Note that the release series won't have the DevOffset applied, even if the
// version has it.
func (k Key) ReleaseSeries() roachpb.ReleaseSeries {
	// Note: TestReleaseSeries ensures that this works for all valid Keys.
	s, _ := removeDevOffset(k.Version()).ReleaseSeries()
	return s
}

func (k Key) String() string {
	return k.Version().String()
}

// SupportedPreviousReleases returns the list of final versions for previous
// that are supported by this branch (and from which we can upgrade an existing
// cluster).
func SupportedPreviousReleases() []Key {
	res := make([]Key, 0, 2)
	for k := MinSupported; k < Latest; k++ {
		if k.IsFinal() {
			res = append(res, k)
		}
	}
	return res
}

// ListBetween returns the list of cluster versions in the range
// (from, to].
func ListBetween(from, to roachpb.Version) []roachpb.Version {
	var cvs []roachpb.Version
	for k := Key(0); k < numKeys; k++ {
		if v := k.Version(); from.Less(v) && v.LessEq(to) {
			cvs = append(cvs, v)
		}
	}
	return cvs
}
