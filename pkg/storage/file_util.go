// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

// SafeWriteToFile writes the byte slice to the filename, contained in dir,
// using the given fs.  It returns after both the file and the containing
// directory are synced.
func SafeWriteToFile(fs vfs.FS, dir string, filename string, b []byte) error {
	tempName := filename + ".crdbtmp"
	f, err := fs.Create(tempName)
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
