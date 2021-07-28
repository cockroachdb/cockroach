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

func checkFilePermissions(fullKeyPath string, info os.FileInfo) (bool, error) {
	// Check permissions bits.
	filePerm := info.Mode().Perm()

	if readableByGroup(info) {
		// if the file is owned by root, we allow those in the owning group to read it
		if exceedsPermissions(filePerm, maxGroupKeyPermissions) {
			return false, errors.Errorf("key file %s has permissions %s, exceeds %s",
				fullKeyPath, filePerm, maxGroupKeyPermissions)

		}

		return true, nil
	}

	if exceedsPermissions(filePerm, maxKeyPermissions) {
		return false, errors.Errorf("key file %s has permissions %s, exceeds %s",
			fullKeyPath, filePerm, maxKeyPermissions)
	}

	return true, nil
}

