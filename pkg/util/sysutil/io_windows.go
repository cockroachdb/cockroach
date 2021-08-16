// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sysutil

import (
	"os"
)

// OpenFile wraps os.OpenFile.
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Create wraps os.Create.
func Create(name string) (*os.File, error) {
	return os.Create(name)
}

// GetUmask returns the current Umask. Under Windows, this function will always
// return 0000.
func GetUmask() os.FileMode {
	return 0000
}
