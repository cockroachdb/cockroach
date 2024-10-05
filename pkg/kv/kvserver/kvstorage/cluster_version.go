// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// WriteClusterVersion writes the given cluster version to the min version file.
func WriteClusterVersion(
	ctx context.Context, eng storage.Engine, cv clusterversion.ClusterVersion,
) error {
	return eng.SetMinVersion(cv.Version)
}

// WriteClusterVersionToEngines writes the given version to the given engines,
// Returns nil on success; otherwise returns first error encountered writing to
// the stores. It makes no attempt to validate the supplied version.
//
// At the time of writing this is used during bootstrap, initial server start
// (to perhaps fill into additional stores), and during cluster version bumps.
func WriteClusterVersionToEngines(
	ctx context.Context, engines []storage.Engine, cv clusterversion.ClusterVersion,
) error {
	for _, eng := range engines {
		if err := WriteClusterVersion(ctx, eng, cv); err != nil {
			return errors.Wrapf(err, "error writing version to engine %s", eng)
		}
	}
	return nil
}

// SynthesizeClusterVersionFromEngines returns the cluster version that was read
// from the engines or, if none are initialized, binaryMinSupportedVersion.
// Typically all initialized engines will have the same version persisted,
// though ill-timed crashes can result in situations where this is not the
// case. Then, the largest version seen is returned.
//
// binaryVersion is the version of this binary. An error is returned if
// any engine has a higher version, as this would indicate that this node
// has previously acked the higher cluster version but is now running an
// old binary, which is unsafe.
//
// binaryMinSupportedVersion is the minimum version supported by this binary. An
// error is returned if any engine has a version lower that this.
func SynthesizeClusterVersionFromEngines(
	ctx context.Context,
	engines []storage.Engine,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
) (clusterversion.ClusterVersion, error) {
	// Find the most recent bootstrap info.
	type originVersion struct {
		roachpb.Version
		origin string
	}

	maxPossibleVersion := roachpb.Version{Major: math.MaxInt32} // Sort above any real version.
	minStoreVersion := originVersion{
		Version: maxPossibleVersion,
		origin:  "(no store)",
	}

	// We run this twice because it's only after having seen all the versions
	// that we can decide whether the node catches a version error. However, we
	// also want to name at least one engine that violates the version
	// constraints, which at the latest the second loop will achieve (because
	// then minStoreVersion don't change any more).
	for _, eng := range engines {
		engVer := eng.MinVersion()
		if engVer == (roachpb.Version{}) {
			return clusterversion.ClusterVersion{}, errors.AssertionFailedf("store %s has no version", eng)
		}

		// Avoid running a binary with a store that is too new. For example,
		// restarting into 1.1 after having upgraded to 1.2 doesn't work.
		if binaryVersion.Less(engVer) {
			return clusterversion.ClusterVersion{}, errors.Errorf(
				"cockroach version v%s is incompatible with data in store %s; use version v%s or later",
				binaryVersion, eng, engVer)
		}

		// Track smallest use version encountered.
		if engVer.Less(minStoreVersion.Version) {
			minStoreVersion.Version = engVer
			minStoreVersion.origin = fmt.Sprint(eng)
		}
	}

	// If no use version was found, fall back to our binaryMinSupportedVersion. This
	// is the case when a brand new node is joining an existing cluster (which
	// may be on any older version this binary supports).
	if minStoreVersion.Version == maxPossibleVersion {
		minStoreVersion.Version = binaryMinSupportedVersion
	}

	cv := clusterversion.ClusterVersion{
		Version: minStoreVersion.Version,
	}
	log.Eventf(ctx, "read clusterVersion %+v", cv)

	if minStoreVersion.Version.Less(binaryMinSupportedVersion) {
		// We now check for old versions before opening the store. This case should
		// no longer be possible.
		return clusterversion.ClusterVersion{}, errors.AssertionFailedf("store %s, last used with cockroach version v%s, "+
			"is too old for running version v%s (which requires data from v%s or later)",
			minStoreVersion.origin, minStoreVersion.Version, binaryVersion, binaryMinSupportedVersion)
	}
	return cv, nil
}
