// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build windows

package sysutil

import (
	"fmt"
	"os/user"
	"syscall"
)

// ProcessIdentity returns a string describing the user and group that this
// process is running as.
func ProcessIdentity() string {
	u, err := user.Current()
	if err != nil {
		return "<unknown>"
	}
	return fmt.Sprintf("uid %s, gid %s", u.Uid, u.Gid)
}

// IsCrossDeviceLinkErrno checks whether the given error object (as
// extracted from an *os.LinkError) is a cross-device link/rename
// error.
func IsCrossDeviceLinkErrno(errno error) bool {
	// 0x11 is Win32 Error Code ERROR_NOT_SAME_DEVICE
	// See: https://msdn.microsoft.com/en-us/library/cc231199.aspx
	return errno == syscall.Errno(0x11)
}

// TerminateSelf is a no-op on windows.
func TerminateSelf() error {
	return nil
}
