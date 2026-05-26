// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

// checkFilePermissions takes the passed path and file info, and returns an
// error if the file fails to match the permissions required. If this function
// returns nil, the file's permissions are acceptable.
func checkFilePermissions(processGID int, fullKeyPath string, fileACL sysutil.ACLInfo) error {
	// if the file is owned by root but also owned by the process's group
	// ID, we'll make an exception.
	if fileACL.IsOwnedByUID(uint64(0)) && fileACL.IsOwnedByGID(uint64(processGID)) {
		// if the file is owned by root, we allow those in the owning group to read it
		if sysutil.ExceedsPermissions(fileACL.Mode(), maxGroupKeyPermissions) {
			return errors.Errorf("key file %s has permissions %s, exceeds %s",
				fullKeyPath, fileACL.Mode(), maxGroupKeyPermissions)

		}

		return nil
	}

	if sysutil.ExceedsPermissions(fileACL.Mode(), maxKeyPermissions) {
		return errors.Errorf("key file %s has permissions %s, exceeds %s",
			fullKeyPath, fileACL.Mode(), maxKeyPermissions)
	}

	return nil
}
