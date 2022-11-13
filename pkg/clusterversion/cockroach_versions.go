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
	// V22_2SystemPrivilegesTable adds system.privileges table.
	V22_2SystemPrivilegesTable
	// V22_2EnablePredicateProjectionChangefeed indicates that changefeeds support
	// predicates and projections.
	V22_2EnablePredicateProjectionChangefeed
	// V22_2AlterSystemSQLInstancesAddLocality adds a locality column to the
	// system.sql_instances table.
	V22_2AlterSystemSQLInstancesAddLocality

	// V22_2 is CockroachDB v22.2. It's used for all v22.2.x patch releases.
	V22_2

	// V23_1_Start demarcates the start of cluster versions stepped through during
	// the process of upgrading from 22.2 to 23.1.
	V23_1Start

	// V23_1TenantNames adds a name column to system.tenants.
	V23_1TenantNames

	// V23_1DescIDSequenceForSystemTenant migrates the descriptor ID generator
	// counter from a meta key to the system.descriptor_id_seq sequence for the
	// system tenant.
	V23_1DescIDSequenceForSystemTenant

	// V23_1AddPartialStatisticsPredicateCol adds a column to store the predicate
	// for a partial statistics collection.
	V23_1AddPartialStatisticsPredicateCol

	// *************************************************
	// Step (1): Add new versions here.
	// Do not add new versions to a patch release.
	// *************************************************
)

func (k Key) String() string {
	return ByKey(k).String()
}

// TODOAlwaysTrue22_1 is a placeholder for any version gate/check that previously
// referenced a < 22.1 version. References to TODOAlwaysTrue22_1 denotes that
// conditional version gate/check logic is now safe to remove.
const TODOAlwaysTrue22_1 = V22_1

// TODOAlwaysTrue is a placeholder for any version gate/check that previously
// referenced a < 22.2 version. References to TODOAlwaysTrue denotes that
// conditional version gate/check logic is now safe to remove.
const TODOAlwaysTrue = V22_2

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
		Key:     V22_2,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 0},
	},
	{
		Key:     V23_1Start,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 2},
	},
	{
		Key:     V23_1TenantNames,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 4},
	},
	{
		Key:     V23_1DescIDSequenceForSystemTenant,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 6},
	},
	{
		Key:     V23_1AddPartialStatisticsPredicateCol,
		Version: roachpb.Version{Major: 22, Minor: 2, Internal: 8},
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
		skipFirst := envutil.EnvOrDefaultBool("COCKROACH_UPGRADE_TO_DEV_VERSION", false)
		const devOffset = 1000000
		for i := range rawVersionsSingleton {
			if i == 0 && skipFirst {
				continue
			}
			rawVersionsSingleton[i].Major += devOffset
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

// TODO(irfansharif): clusterversion.binary{,MinimumSupported}Version
// feels out of place. A "cluster version" and a "binary version" are two
// separate concepts.
var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error. This typically trails the current release by one (see top-level
	// comment).
	binaryMinSupportedVersion = ByKey(V22_1)

	// binaryVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	binaryVersion = versionsSingleton[len(versionsSingleton)-1].Version
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
