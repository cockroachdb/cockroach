// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

/*
Loss of quorum replica recovery supports multi-version clusters during recovery
process.

There are a number of areas that are specific for LOQ recovery in terms of
version handling.
Tools are using json as an intermediate format to collect replica info and to
store replica update plan on the CLI side. That could cause problems when
underlying protobufs change. We can't deserialize json with unknown fields or
enums safely.
To address this problem, we have a cluster version embedded in the file and
whenever we generate some data, we need to ensure that generated structs doesn't
contain any new fields that are not supported by the version of the target file.
The reason why we need to version files is that cluster could have a mixed set
of binaries and if new binary is operating in a cluster of previous version,
it should never communicate or write files in a newer format.

We support versioning for both offline and half-online approaches.

For offline approach, collection will produce replica files with data compatible
with current active cluster version. Collection could be done by the binary
present on the node, either old or new.
Planner will respect cluster version and produce a plan with a version equal to
active cluster version and only containing features that are compatible with it.
Planning could be done by either new or old binary.
Plan application will ensure that stores contain version which is equal to the
version present in the plan. Application could be performed by the binary
present on the node.

For half-online approach, collecting CLI will ensure that it supports version
of the cluster reported in collection metadata. Admin rpc replica collection
endpoints will ensure that only data compatible with active cluster version is
collected. CLI will produce a recovery plan with a version equal to active
cluster version and only containing features that are compatible with it.
Staging will verify that plan version is equal to active cluster version down to
internal version as upgrade steps could be version gates to underlying storage
format changes that must be reflected in recovery.
Application will verify that plan version equal to active cluster version upon
restart.
*/

// checkVersionAllowedByBinary checks if binary could handle data version. Data
// could be either loaded from files or received from cluster.
func checkVersionAllowedByBinary(version roachpb.Version) error {
	return checkVersionAllowedImpl(version,
		clusterversion.MinSupported.Version(),
		clusterversion.Latest.Version())
}

func checkVersionAllowedImpl(version, minSupported, binaryVersion roachpb.Version) error {
	if version.Less(minSupported) {
		return errors.Newf("version is too old, minimum supported %s, found %s", minSupported, version)
	}
	if binaryVersion.Less(version) {
		return errors.Newf("version is too new, maximum supported %s, found %s", binaryVersion, version)
	}
	return nil
}

// checkPlanVersionMatches verifies that plan with version could be staged or
// applied on cluster with version current.
// Note that patch versions must be zero in accordance with roachpb.Version
// documentation.
func checkPlanVersionMatches(version, current roachpb.Version, ignoreInternal bool) error {
	if current.Equal(version) {
		return nil
	}

	if ignoreInternal {
		newVersion := version
		newVersion.Internal = current.Internal
		if newVersion.Equal(current) {
			return nil
		}
	}

	// We should never have a plan or cluster with a non zero patch version.
	// If plan has it, call it out explicitly. If cluster is broken and somehow
	// got non zero patch, then a plan generated for that cluster would preserve
	// it and pass checks above.
	if version.Patch != 0 {
		return errors.Newf("recovery plan must not use patch versions %s, active cluster version %s", version, current)
	}
	return errors.Newf("recovery plan version %s, doesn't match cluster active version %s", version, current)
}
