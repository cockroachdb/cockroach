// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fs

import "io"

// File and FS are a partial attempt at offering the Pebble vfs.FS interface. Given the constraints
// of the RocksDB Env interface we've chosen to only include what is easy to implement. Additionally,
// it does not try to subsume all the file related functionality already in the Engine interface.
// It seems preferable to do a final cleanup only when the implementation can simply use Pebble's
// implementation of vfs.FS. At that point the following interface will become a superset of vfs.FS.
type File interface {
	io.ReadWriteCloser
	io.ReaderAt
	Sync() error
}

// FS provides a filesystem interface.
type FS interface {
	// CreateFile creates the named file for writing, truncating it if it already
	// exists.
	CreateFile(name string) (File, error)

	// CreateFileWithSync is similar to CreateFile, but the file is periodically
	// synced whenever more than bytesPerSync bytes accumulate. This syncing
	// does not provide any persistency guarantees, but can prevent latency
	// spikes.
	CreateFileWithSync(name string, bytesPerSync int) (File, error)

	// LinkFile creates newname as a hard link to the oldname file.
	LinkFile(oldname, newname string) error

	// OpenFile opens the named file for reading.
	OpenFile(name string) (File, error)

	// OpenDir opens the named directory for syncing.
	OpenDir(name string) (File, error)

	// DeleteFile removes the named file. If the file with given name doesn't exist, return an error
	// that returns true from os.IsNotExist().
	DeleteFile(name string) error

	// RenameFile renames a file. It overwrites the file at newname if one exists,
	// the same as os.Rename.
	RenameFile(oldname, newname string) error

	// CreateDir creates the named dir. Does nothing if the directory already
	// exists.
	CreateDir(name string) error

	// DeleteDir removes the named dir.
	DeleteDir(name string) error

	// ListDir returns a listing of the given directory. The names returned are
	// relative to the directory.
	ListDir(name string) ([]string, error)
}
