// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import "os"

type windowsACLInfo struct {
	mode os.FileMode
}

func (acl *windowsACLInfo) UID() uint64 {
	return uint64(0)
}

func (acl *windowsACLInfo) GID() uint64 {
	return uint64(0)
}

func (acl *windowsACLInfo) IsOwnedByUID(uid uint64) bool {
	return acl.UID() == uid
}

func (acl *windowsACLInfo) IsOwnedByGID(gid uint64) bool {
	return acl.GID() == gid
}

func (acl *windowsACLInfo) Mode() os.FileMode {
	return acl.mode
}

// GetFileACLInfo returns an ACLInfo that has the UID and GID populated from
// the system specific file information. On Windows, this returns an ACLInfo
// that always has the UID and GID of 0, since we haven't implemented support
// for looking up the Windows owner information.
func GetFileACLInfo(info os.FileInfo) ACLInfo {
	return &windowsACLInfo{
		mode: info.Mode().Perm(),
	}
}
