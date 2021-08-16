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

import (
	"io/fs"
	"io/ioutil"
	"os"
)

// OpenFile wraps os.OpenFile. On UNIX-like systems this function
// modifies the passed permissions to prevent new files from being
// world-accessible.
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Create wraps os.Create. On UNIX-like systems this function
// modifies the passed permissions to prevent new files from being
// world-accessible.
func Create(name string) (*os.File, error) {
	return os.Create(name)
}

// WriteFile wraps ioutil.WriteFile. On UNIX-like systems this function
// modifies the passed permissions to prevent new files from being
// world-accessible.
func WriteFile(filename string, data []byte, perm fs.FileMode) error {
	return ioutil.WriteFile(filename, data, perm)
}
