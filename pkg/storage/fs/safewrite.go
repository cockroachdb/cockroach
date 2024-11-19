// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

const tempFileExtension = ".crdbtmp"

// SafeWriteToFile writes the byte slice to the filename, contained in dir,
// using the given fs. It returns after both the file and the containing
// directory are synced.
func SafeWriteToFile(
	fs vfs.FS, dir string, filename string, b []byte, category vfs.DiskWriteCategory,
) error {
	// TODO(jackson): Assert that fs supports atomic renames once Pebble
	// is bumped to the appropriate SHA and non-atomic use cases are
	// updated to avoid this method.

	tempName := filename + tempFileExtension
	f, err := fs.Create(tempName, category)
	if err != nil {
		return err
	}
	bReader := bytes.NewReader(b)
	if _, err = io.Copy(f, bReader); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = fs.Rename(tempName, filename); err != nil {
		return err
	}
	fdir, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	defer fdir.Close()
	return fdir.Sync()
}
