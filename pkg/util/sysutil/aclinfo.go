// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import "os"

// ACLInfo represents access control information for a file in the
// filesystem. It can be used to determine if a file has the correct
// permissions associated with it.
type ACLInfo interface {
	// UID returns the ID of the user that owns the file in question.
	UID() uint64
	// GID returns the ID of the group that owns the file in question.
	GID() uint64

	// IsOwnedByUID returns true of the passed UID is equal to the UID of the file in question.
	IsOwnedByUID(uint64) bool
	// IsOwnedByGID returns true of the passed GID is equal to the GID of the file in question.
	IsOwnedByGID(uint64) bool

	// Mode returns the os.FileMode representing the files permissions. Implementers of this
	// interface should return only the result of the `Perm` function in os.FileMode, to
	// ensure that only permissions are returned.
	Mode() os.FileMode
}

// ExceedsPermissions returns true if the passed os.FileMode represents a more stringent
// set of permissions than the ACLInfo's mode.
//
// For example, calling this function with an objectMode of 0640 will return false if
// the passed allowedMode is 0600.
func ExceedsPermissions(objectMode, allowedMode os.FileMode) bool {
	mask := os.FileMode(0777) ^ allowedMode
	return mask&objectMode != 0
}
