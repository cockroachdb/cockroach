// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"io/ioutil"

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

// WriteMinVersionFile writes the provided version to disk. The caller must
// guarantee that the version will never be downgraded below the given version.
func WriteMinVersionFile(atomicRenameFS vfs.FS, dir string, version *roachpb.Version) error {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.
	if version == nil {
		return errors.New("min version should not be nil")
	}
	ok, err := MinVersionIsAtLeastTargetVersion(atomicRenameFS, dir, version)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	b, err := protoutil.Marshal(version)
	if err != nil {
		return err
	}
	filename := atomicRenameFS.PathJoin(dir, MinVersionFilename)
	if err := SafeWriteToFile(atomicRenameFS, dir, filename, b); err != nil {
		return err
	}
	return nil
}

// MinVersionIsAtLeastTargetVersion returns whether the min version recorded
// on disk is at least the target version.
func MinVersionIsAtLeastTargetVersion(
	atomicRenameFS vfs.FS, dir string, target *roachpb.Version,
) (bool, error) {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.
	if target == nil {
		return false, errors.New("target version should not be nil")
	}
	minVersion, err := getMinVersion(atomicRenameFS, dir)
	if err != nil {
		return false, err
	}
	if minVersion == nil {
		return false, nil
	}
	return !minVersion.Less(*target), nil
}

// getMinVersion returns the min version recorded on disk if the min version
// file exists and nil otherwise.
func getMinVersion(atomicRenameFS vfs.FS, dir string) (*roachpb.Version, error) {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.

	filename := atomicRenameFS.PathJoin(dir, MinVersionFilename)
	f, err := atomicRenameFS.Open(filename)
	if oserror.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	version := &roachpb.Version{}
	if err := protoutil.Unmarshal(b, version); err != nil {
		return nil, err
	}
	return version, nil
}
