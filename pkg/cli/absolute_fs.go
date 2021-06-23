// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/vfs"
)

// absoluteFS is a wrapper vfs.FS for an encryptedFS that is used only
// by the Pebble tool. It converts filepath names to absolute paths
// before calling the underlying interface implementation for functions
// that make use of the PebbleFileRegistry.
//
// This is needed when using encryptedFS, since the PebbleFileRegistry used in
// that context attempts to convert function input paths to relative paths using
// the DBDir. Both the DBDir and function input paths in a CockroachDB node are
// absolute paths, but when using the Pebble tool, the function input paths are
// based on what the cli user passed to the pebble command. We do not wish for a
// user using the cli to remember to pass an absolute path to the various pebble
// tool commands that accept paths. Note that the pebble tool commands taking a
// path parameter are quite varied: ranging from "pebble db" to "pebble lsm",
// so it is simplest to intercept the function input paths here.
//
// Note that absoluteFS assumes the wrapped vfs.FS corresponds to the underlying
// OS filesystem and will not work for the general case of a vfs.FS.
// This limitation is acceptable for this tool's use cases.
type absoluteFS struct {
	fs vfs.FS
}

var _ vfs.FS = &absoluteFS{}

func (fs *absoluteFS) Create(name string) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	return fs.fs.Create(name)
}

func (fs *absoluteFS) Link(oldname, newname string) error {
	return wrapWithAbsolute(fs.fs.Link, oldname, newname)
}

func (fs *absoluteFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	return fs.fs.Open(name, opts...)
}

func (fs *absoluteFS) OpenDir(name string) (vfs.File, error) {
	return fs.fs.OpenDir(name)
}

func (fs *absoluteFS) Remove(name string) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	return fs.fs.Remove(name)
}

func (fs *absoluteFS) RemoveAll(name string) error {
	return fs.fs.RemoveAll(name)
}

func (fs *absoluteFS) Rename(oldname, newname string) error {
	return wrapWithAbsolute(fs.fs.Rename, oldname, newname)
}

func (fs *absoluteFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return nil, err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return nil, err
	}
	return fs.fs.ReuseForWrite(oldname, newname)
}

func (fs *absoluteFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.fs.MkdirAll(dir, perm)
}

func (fs *absoluteFS) Lock(name string) (io.Closer, error) {
	return fs.fs.Lock(name)
}

func (fs *absoluteFS) List(dir string) ([]string, error) {
	return fs.fs.List(dir)
}

func (fs *absoluteFS) Stat(name string) (os.FileInfo, error) {
	return fs.fs.Stat(name)
}

func (fs *absoluteFS) PathBase(path string) string {
	return fs.fs.PathBase(path)
}

func (fs *absoluteFS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

func (fs *absoluteFS) PathDir(path string) string {
	return fs.fs.PathDir(path)
}

func (fs *absoluteFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return fs.fs.GetDiskUsage(path)
}

func wrapWithAbsolute(fn func(string, string) error, oldname, newname string) error {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return err
	}
	return fn(oldname, newname)
}
