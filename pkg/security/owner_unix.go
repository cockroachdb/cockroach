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

// readableByProcessGroup checks if the passed file is owned by the root user
// and if so, checks if the passed group ID matches the group wnership of the
// file, returning true if so.
func readableByProcessGroup(processGID int, info os.FileInfo) bool {
	sysInfo := info.Sys()
	if nil == sysInfo {
		// if we don't have any information we can't safely
		// determine what to do.
		return false
	}

	if statTInfo, ok := sysInfo.(*syscall.Stat_t); ok {
		// we use uint64 because a process should never have a GID that's less
		// than zero, and syscall.Stat_t may use uint32 for GID, since it's a C
		// struct under the hood.
		if uint64(statTInfo.Uid) == uint64(0) &&
			uint64(statTInfo.Gid) == uint64(processGID) {
			return true
		}
	}

	return false
}
