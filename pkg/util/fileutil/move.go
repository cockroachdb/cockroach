// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fileutil

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

// Move moves a file from a directory to another, while handling
// cross-filesystem moves properly.
// If the target file already exists, it is truncated.
// If the move fails, then the target file may be left in an inconsistent state.
func Move(oldPath, newPath string) error {
	err := os.Rename(oldPath, newPath)
	if !isCrossDeviceLinkError(err) {
		return err
	}

	if err = CopyFile(oldPath, newPath); err != nil {
		return err
	}

	return os.RemoveAll(oldPath)
}

func isCrossDeviceLinkError(err error) bool {
	if err == nil {
		return false
	}
	var le *os.LinkError
	if errors.As(err, &le) {
		return sysutil.IsCrossDeviceLinkErrno(le.Err)
	}
	return false
}
