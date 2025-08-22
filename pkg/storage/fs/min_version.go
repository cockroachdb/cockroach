// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

// MinVersionFilename is the name of the file containing a marshaled
// roachpb.Version that can be updated during storage-related migrations
// and checked on startup to determine if we can safely use a
// backwards-incompatible feature.
const MinVersionFilename = "STORAGE_MIN_VERSION"

// MinVersionIsAtLeastTargetVersion returns whether the min version recorded
// on disk is at least the target version.
func MinVersionIsAtLeastTargetVersion(
	atomicRenameFS vfs.FS, dir string, target roachpb.Version,
) (bool, error) {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.
	if target == (roachpb.Version{}) {
		return false, errors.New("target version should not be empty")
	}
	minVersion, ok, err := GetMinVersion(atomicRenameFS, dir)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return !minVersion.Less(target), nil
}

// GetMinVersion returns the min version recorded on disk. If the min version
// file doesn't exist, returns ok=false.
func GetMinVersion(atomicRenameFS vfs.FS, dir string) (_ roachpb.Version, ok bool, _ error) {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.

	filename := atomicRenameFS.PathJoin(dir, MinVersionFilename)
	f, err := atomicRenameFS.Open(filename)
	if oserror.IsNotExist(err) {
		return roachpb.Version{}, false, nil
	}
	if err != nil {
		return roachpb.Version{}, false, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return roachpb.Version{}, false, err
	}
	version := roachpb.Version{}
	if err := protoutil.Unmarshal(b, &version); err != nil {
		return roachpb.Version{}, false, err
	}
	return version, true, nil
}

// ValidateMinVersionFile validates the store cluster version found
// in the min-version file in against the provided cluster version.
func ValidateMinVersionFile(
	fs vfs.FS, dir string, clusterVersion clusterversion.Handle,
) (roachpb.Version, error) {
	// Read the current store cluster version.
	storeClusterVersion, minVerFileExists, err := GetMinVersion(fs, dir)
	if !minVerFileExists || err != nil {
		return storeClusterVersion, err
	}

	v := clusterVersion
	if v == nil {
		return storeClusterVersion, errors.New("cluster version must be provided if min version file exists")
	}

	// Avoid running a binary too new for this store. This is what you'd catch
	// if, say, you restarted directly from v21.2 into v22.2 (bumping the min
	// version) without going through v22.1 first.
	//
	// Note that "going through" above means that v22.1 successfully upgrades
	// all existing stores. If v22.1 crashes half-way through the startup
	// sequence (so now some stores have v21.2, but others v22.1) you are
	// expected to run v22.1 again (hopefully without the crash this time) which
	// would then rewrite all the stores.
	if storeClusterVersion.Less(v.MinSupportedVersion()) {
		if storeClusterVersion.Major < clusterversion.DevOffset && v.LatestVersion().Major >= clusterversion.DevOffset {
			return storeClusterVersion, errors.Errorf(
				"store last used with cockroach non-development version v%s "+
					"cannot be opened by development version v%s",
				storeClusterVersion, v.LatestVersion(),
			)
		}
		return storeClusterVersion, errors.Errorf(
			"store last used with cockroach version v%s "+
				"is too old for running version v%s (which requires data from v%s or later)",
			storeClusterVersion, v.LatestVersion(), v.MinSupportedVersion(),
		)
	}

	// Avoid running a binary too old for this store. This protects against
	// scenarios where an older binary attempts to open a store created by
	// a newer version that may have incompatible data structures or formats.
	if v.LatestVersion().Less(storeClusterVersion) {
		return storeClusterVersion, errors.Errorf(
			"store last used with cockroach version v%s is too high for running "+
				"version v%s",
			storeClusterVersion, v.LatestVersion(),
		)
	}

	return storeClusterVersion, nil
}
