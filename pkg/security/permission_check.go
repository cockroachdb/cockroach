// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"os"

	"github.com/cockroachdb/errors"
)

// checkFilePermissions takes the passed path and file info, and returns an
// error if the file fails to match the permissions required. If this function
// returns nil, the file's permissions are acceptable.
func checkFilePermissions(processGID int, fullKeyPath string, info os.FileInfo) error {
	// Check permissions bits.
	filePerm := info.Mode().Perm()

	if readableByProcessGroup(processGID, info) {
		// if the file is owned by root, we allow those in the owning group to read it
		if exceedsPermissions(filePerm, maxGroupKeyPermissions) {
			return errors.Errorf("key file %s has permissions %s, exceeds %s",
				fullKeyPath, filePerm, maxGroupKeyPermissions)

		}

		return nil
	}

	if exceedsPermissions(filePerm, maxKeyPermissions) {
		return errors.Errorf("key file %s has permissions %s, exceeds %s",
			fullKeyPath, filePerm, maxKeyPermissions)
	}

	return nil
}
