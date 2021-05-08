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
	"path/filepath"

	"github.com/cockroachdb/pebble/vfs"
)

// absoluteFS is a wrapper vfs.FS that converts filepath names to absolute paths
// before calling the underlying interface implementation for each function.
type absoluteFS struct {
	vfs.FS
}

func (fs *absoluteFS) Create(name string) (vfs.File, error) {
	return wrapWithAbsolute1(fs.FS.Create, name)
}

func (fs *absoluteFS) Link(oldname, newname string) error {
	return wrapWithAbsolute3(fs.FS.Link, oldname, newname)
}

func (fs *absoluteFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return wrapWithAbsolute2(fs.FS.Open, name, opts...)
}

func (fs *absoluteFS) OpenDir(name string) (vfs.File, error) {
	return wrapWithAbsolute1(fs.FS.OpenDir, name)
}

func (fs *absoluteFS) Remove(name string) error {
	return wrapWithAbsolute0(fs.FS.Remove, name)
}

func (fs *absoluteFS) RemoveAll(name string) error {
	return wrapWithAbsolute0(fs.FS.RemoveAll, name)
}

func (fs *absoluteFS) Rename(oldname, newname string) error {
	return wrapWithAbsolute3(fs.FS.Rename, oldname, newname)
}

func (fs *absoluteFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	return wrapWithAbsolute4(fs.FS.ReuseForWrite, oldname, newname)
}

func wrapWithAbsolute0(fn func(string) error, name string) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	return fn(name)
}

func wrapWithAbsolute1(fn func(string) (vfs.File, error), name string) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	return fn(name)
}

func wrapWithAbsolute2(
	fn func(string, ...vfs.OpenOption) (vfs.File, error), name string, opts ...vfs.OpenOption,
) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	return fn(name, opts...)
}

func wrapWithAbsolute3(fn func(string, string) error, oldname, newname string) error {
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

func wrapWithAbsolute4(
	fn func(string, string) (vfs.File, error), oldname, newname string,
) (vfs.File, error) {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return nil, err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return nil, err
	}
	return fn(oldname, newname)
}
