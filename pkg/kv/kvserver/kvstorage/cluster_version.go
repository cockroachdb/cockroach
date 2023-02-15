// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// WriteClusterVersion writes the given cluster version to the store-local
// cluster version key. We only accept a raw engine to ensure we're persisting
// the write durably.
func WriteClusterVersion(
	ctx context.Context, eng storage.Engine, cv clusterversion.ClusterVersion,
) error {
	err := storage.MVCCPutProto(
		ctx,
		eng,
		nil,
		keys.StoreClusterVersionKey(),
		hlc.Timestamp{},
		hlc.ClockTimestamp{},
		nil,
		&cv,
	)
	if err != nil {
		return err
	}

	// The storage engine sometimes must make backwards incompatible
	// changes. However, the store cluster version key is a key stored
	// within the storage engine, so it's unavailable when the store is
	// opened.
	//
	// The storage engine maintains its own minimum version on disk that
	// it may consult it before opening the Engine. This version is
	// stored in a separate file on the filesystem. For now, write to
	// this file in combination with the store cluster version key.
	//
	// This parallel version state is a bit of a wart and an eventual
	// goal is to replace the store cluster version key with the storage
	// engine's flat file. This requires that there are no writes to the
	// engine until either bootstrapping or joining an existing cluster.
	// Writing the version to this file would happen before opening the
	// engine for completing the rest of bootstrapping/joining the
	// cluster.
	return eng.SetMinVersion(cv.Version)
}

// ReadClusterVersion reads the cluster version from the store-local version
// key. Returns an empty version if the key is not found.
func ReadClusterVersion(
	ctx context.Context, reader storage.Reader,
) (clusterversion.ClusterVersion, error) {
	var cv clusterversion.ClusterVersion
	_, err := storage.MVCCGetProto(ctx, reader, keys.StoreClusterVersionKey(), hlc.Timestamp{},
		&cv, storage.MVCCGetOptions{})
	return cv, err
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
		eng := eng.(storage.Reader) // we're read only
		var cv clusterversion.ClusterVersion
		cv, err := ReadClusterVersion(ctx, eng)
		if err != nil {
			return clusterversion.ClusterVersion{}, err
		}
		if cv.Version == (roachpb.Version{}) {
			// This is needed when a node first joins an existing cluster, in
			// which case it won't know what version to use until the first
			// Gossip update comes in.
			cv.Version = binaryMinSupportedVersion
		}

		// Avoid running a binary with a store that is too new. For example,
		// restarting into 1.1 after having upgraded to 1.2 doesn't work.
		if binaryVersion.Less(cv.Version) {
			return clusterversion.ClusterVersion{}, errors.Errorf(
				"cockroach version v%s is incompatible with data in store %s; use version v%s or later",
				binaryVersion, eng, cv.Version)
		}

		// Track smallest use version encountered.
		if cv.Version.Less(minStoreVersion.Version) {
			minStoreVersion.Version = cv.Version
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

	// We now check for old versions up front when we open the database. We leave
	// this older check for the case where a store is so old that it doesn't have
	// a min version file.
	if minStoreVersion.Version.Less(binaryMinSupportedVersion) {
		return clusterversion.ClusterVersion{}, errors.Errorf("store %s, last used with cockroach version v%s, "+
			"is too old for running version v%s (which requires data from v%s or later)",
			minStoreVersion.origin, minStoreVersion.Version, binaryVersion, binaryMinSupportedVersion)
	}
	return cv, nil
}
