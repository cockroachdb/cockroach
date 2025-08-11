// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/minversion"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// writeMinVersionFile writes the provided version to disk. The caller must
// guarantee that the version will never be downgraded below the given version.
func writeMinVersionFile(atomicRenameFS vfs.FS, dir string, version roachpb.Version) error {
	// TODO(jackson): Assert that atomicRenameFS supports atomic renames
	// once Pebble is bumped to the appropriate SHA.
	if version == (roachpb.Version{}) {
		return errors.New("min version should not be empty")
	}
	ok, err := minversion.MinVersionIsAtLeastTargetVersion(atomicRenameFS, dir, version)
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
	filename := atomicRenameFS.PathJoin(dir, minversion.MinVersionFilename)
	if err := fs.SafeWriteToUnencryptedFile(atomicRenameFS, dir, filename, b, fs.UnspecifiedWriteCategory); err != nil {
		return err
	}
	return nil
}
