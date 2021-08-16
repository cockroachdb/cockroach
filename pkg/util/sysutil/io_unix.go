// Copyright 2021 The Cockroach Authors.
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

import "os"

// OpenFile wraps os.OpenFile, but will mask the passed os.FileMode to
// prevent the creation of files with world access permissions. This means
// that files created by this function will only be accessible by the user
// and group that the CockroachDB process runs as.
//
// Note that the umask will still be applied to the filemode before
// creation.
func OpenFile(filename string, flag int, perm os.FileMode) (*os.File, error) {
	perm = ModeAfterMask(perm, 0007)

	file, err := os.OpenFile(filename, flag, perm)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Create wraps os.Create. On UNIX-like systems, this function uses more
// limited permissions than the
func Create(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, DefaultCreateMode)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// WriteFile works identically to ioutil.WriteFile, except that it uses the OpenFile
// function in this package (which means that files created by it do not have
// world-readable permissions on UNIX-like systems).
func WriteFile(filename string, data []byte, perm os.FileMode) error {
	// OpenFile will handle permission updating for us.
	file, err := OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	if closeErr := file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}
