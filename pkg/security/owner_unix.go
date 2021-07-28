// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows
// +build !plan9

package security

import (
	"os"
	"syscall"
)

// readableByGroup checks if the passed file is owned by the root user, and if
// so, checks if the group ID of the process (ignoring the setgid bit if set)
// matches the group ownership of the file, returning true if so.
func readableByGroup(info os.FileInfo) bool {
	sysInfo := info.Sys()
	if nil == sysInfo {
		// if we don't have any information we can't safely
		// determine what to do.
		return false
	}

	// we use uint64 because a process should never have a GID that's less
	// than zero, and syscall.Stat_t uses uit32 for GID, since it's a C
	// struct under the hood.
	processGID := uint64(os.Getgid())

	if statTInfo, ok := sysInfo.(*syscall.Stat_t); ok {
		if statTInfo.Uid == 0 &&
			uint64(statTInfo.Gid) == processGID {
			return true
		}
	}

	return false
}
