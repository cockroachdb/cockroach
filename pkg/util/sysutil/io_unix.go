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

package sysutil

import (
	"os"

	"golang.org/x/sys/unix"
)

const DefaultCreateMode = os.FileMode(0660)

// OpenFile wraps os.OpenFile, but will always call `chmod` when creating
// a file to ensure that it's always created with the requested permissions,
// regardless of umask. The intention of this function is to ensure that
// CockroachDB only creates files that are not readable by users outside of
// the process user and group.
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	if flag&os.O_CREATE == os.O_CREATE {
		err = file.Chmod(perm)
		if err != nil {
			return nil, err
		}
	}
	return file, nil
}

// Create wraps os.Create, but will use the default permissions
func Create(name string) (*os.File, error) {
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	// change the file mode to the default one before applying the Umask (excluding the
	// "other" bits in the Umask).
	err = file.Chmod(modeAfterUmask(DefaultCreateMode))
	if err != nil {
		return nil, err
	}

	return file, nil
}

// addOtherBits adds the the "other" bits. So a value of "0770" will
// become "0777", or "rwxrwx---" will become "rwxrwxrwx".
func addOtherBits(perm os.FileMode) os.FileMode {
	return (perm | 0007)
}

// removeOtherBits removes the the "other" bits. So a value of "0777" will
// become "0770", or "rwxrwxrwx" will become "rwxrwx---".
func removeOtherBits(perm os.FileMode) os.FileMode {
	return (perm & 0770)
}

// modeAfterUmask returns a bitwise AND NOT of the umask. This results
// in the input value with the umask applied.
//
// So for example (focusing on only one permission value):
//
// 0x111
//     ^ execute (0x1)
//    ^ write (0x2)
//   ^ read (0x4)
//
// A umask of 0x2 (block writes) will be:
//
// 0x010
//
// To apply the umask, we do a bitwise AND NOT with the input (for example,
// 0x111, or "rwx").
//
//    0x111
// &^ 0x010
//    -----
//    0x101
//
// Which as a result, means that the end result is without the write
// permission.
func modeAfterUmask(in os.FileMode) os.FileMode {
	return os.FileMode(in &^ addOtherBits(GetUmask()))
}

// GetUmask returns the current Umask.
func GetUmask() os.FileMode {
	currentUmask := unix.Umask(0022)

	// because calling Umask updates it, we have to call it a second time.
	_ = unix.Umask(currentUmask)
	return os.FileMode(currentUmask)
}
