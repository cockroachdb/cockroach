// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
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

// writeMinVersionFile writes the provided version to disk. The caller must
// guarantee that the version will never be downgraded below the given version.
func writeMinVersionFile(atomicRenameFS vfs.FS, dir string, version roachpb.Version) error {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.
	if version == (roachpb.Version{}) {
		return errors.New("min version should not be empty")
	}
	ok, err := MinVersionIsAtLeastTargetVersion(atomicRenameFS, dir, version)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	b, err := protoutil.Marshal(&version)
	if err != nil {
		return err
	}
	filename := atomicRenameFS.PathJoin(dir, MinVersionFilename)
	if err := fs.SafeWriteToFile(atomicRenameFS, dir, filename, b, fs.UnspecifiedWriteCategory); err != nil {
		return err
	}
	return nil
}

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
	minVersion, ok, err := getMinVersion(atomicRenameFS, dir)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return !minVersion.Less(target), nil
}

// getMinVersion returns the min version recorded on disk. If the min version
// file doesn't exist, returns ok=false.
func getMinVersion(atomicRenameFS vfs.FS, dir string) (_ roachpb.Version, ok bool, _ error) {
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
