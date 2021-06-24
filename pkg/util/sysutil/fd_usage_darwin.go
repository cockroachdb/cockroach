// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build darwin

package sysutil

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

// Get populates ProcFDUsage for the current process.
func (u *ProcFDUsage) Get() error {
	openFDs, err := getOpenFileDescriptors()
	if err != nil {
		return errors.Wrap(err, "listing open file descriptors")
	}

	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return errors.Wrap(err, "getrlimit")
	}

	u.Open = openFDs
	u.SoftLimit = rlim.Cur
	u.HardLimit = rlim.Max
	return nil
}

// getOpenFileDescriptors determines the number of open files for the
// process by counting the number of files in /dev/fd.
func getOpenFileDescriptors() (uint64, error) {
	const fdDir = "/dev/fd"
	f, err := os.Open(fdDir)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	// Readdirnames doesn't return . and ..
	names, err := f.Readdirnames(0 /* limit, 0 is unlimited */)
	return uint64(len(names)), err
}
