// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sysutil

import "os"

// DefaultCreateMode is the default os.FileMode that the IO functions in
// this package will create new files with.
const DefaultCreateMode = os.FileMode(0660)

// ModeAfterMask returns a bitwise AND NOT of the passed FileMode mask.
// This function, when using the umask as the mask, will show the permissions
// new files will be created with.
//
// So for example (focusing on only one permission value):
//
// 0b111
//     ^ execute (0x1)
//    ^ write (0x2)
//   ^ read (0x4)
//
// A umask of 0x2 (block writes) will be:
//
// 0b010
//
// To apply the umask, we do a bitwise AND NOT with the input (for example,
// 0b111, or "rwx").
//
//    0b111
// &^ 0b010
//    -----
//    0b101
//
// Which as a result, means that the end result is without the write
// permission.
func ModeAfterMask(perm, mask os.FileMode) os.FileMode {
	return perm &^ mask
}
