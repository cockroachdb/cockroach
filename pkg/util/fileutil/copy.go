// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fileutil

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// CopyDir recursively copies all files and directories in the from directory
// into the to directory. If the to directory does not exist, it is created.
// If the to directory already exists, its contents are overwritten.
func CopyDir(from, to string) error {
	return filepath.Walk(from, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		destPath := strings.Replace(srcPath, from, to, 1)
		if info.IsDir() {
			return os.MkdirAll(destPath, info.Mode())
		}
		return CopyFile(srcPath, destPath)
	})
}

// CopyFile copies src to dst.
// If the target file already exists, it is overwritten.
// If the copy fails, the target file may be left in an inconsistent state.
func CopyFile(srcPath, destPath string) (err error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dest, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer dest.Close()
	if _, err = io.Copy(dest, src); err != nil {
		return err
	}
	return dest.Sync()
}
