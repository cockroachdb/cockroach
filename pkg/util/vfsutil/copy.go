// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vfsutil

import (
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

// CopyRecursive recursively copies all files and directories from srcPath on
// srcFS to dstPath on dstFS. If the destination path does not exist, it is
// created.
// Files that already exist in the destination path will be replaced.
func CopyRecursive(srcFS, dstFS vfs.FS, srcPath, dstPath string) error {
	srcInfo, err := srcFS.Stat(srcPath)
	if err != nil {
		return err
	}

	if srcInfo.IsDir() {
		if err := dstFS.MkdirAll(dstPath, srcInfo.Mode()); err != nil {
			return err
		}

		entries, err := srcFS.List(srcPath)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			srcEntryPath := srcFS.PathJoin(srcPath, entry)
			dstEntryPath := dstFS.PathJoin(dstPath, entry)
			if err := CopyRecursive(srcFS, dstFS, srcEntryPath, dstEntryPath); err != nil {
				return err
			}
		}
		return nil
	}

	// Copy regular file
	return copyFile(srcFS, dstFS, srcPath, dstPath)
}

func copyFile(srcFS, dstFS vfs.FS, srcPath, dstPath string) error {
	// Open source file
	src, err := srcFS.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	// Create destination file
	dst, err := dstFS.Create(dstPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	defer dst.Close()

	// Copy file contents
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}

	// Sync to ensure data is written
	return dst.Sync()
}
