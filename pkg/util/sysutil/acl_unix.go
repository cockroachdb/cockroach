// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows && !plan9

package sysutil

import (
	"os"
	"syscall"
)

type unixACLInfo struct {
	uid  uint64
	gid  uint64
	mode os.FileMode
}

func (acl *unixACLInfo) UID() uint64 {
	return acl.uid
}

func (acl *unixACLInfo) GID() uint64 {
	return acl.gid
}

func (acl *unixACLInfo) IsOwnedByUID(uid uint64) bool {
	return acl.uid == uid
}

func (acl *unixACLInfo) IsOwnedByGID(gid uint64) bool {
	return acl.gid == gid
}

func (acl *unixACLInfo) Mode() os.FileMode {
	return acl.mode
}

// GetFileACLInfo returns an ACLInfo that has the UID and GID populated from
// the system specific file information.
func GetFileACLInfo(info os.FileInfo) ACLInfo {
	sysInfo := info.Sys()
	if nil != sysInfo {
		if statTInfo, ok := sysInfo.(*syscall.Stat_t); ok {
			// we use uint64 because a process should never have a
			// GID that's less than zero, and syscall.Stat_t may
			// use a uint16 or uint32 for GID, since it's often a
			// C struct under the hood.
			return &unixACLInfo{
				uid:  uint64(statTInfo.Uid),
				gid:  uint64(statTInfo.Gid),
				mode: info.Mode().Perm(),
			}
		}
	}

	// if we don't know who owns a file, assume that root owns it.
	return &unixACLInfo{
		uid:  uint64(0),
		gid:  uint64(0),
		mode: info.Mode().Perm(),
	}
}
