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

import (
	"io"
	"os"
)

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
	// Create creates the named file for writing, truncating it if it already
	// exists.
	Create(name string) (File, error)

	// CreateWithSync is similar to Create, but the file is periodically
	// synced whenever more than bytesPerSync bytes accumulate. This syncing
	// does not provide any persistency guarantees, but can prevent latency
	// spikes.
	CreateWithSync(name string, bytesPerSync int) (File, error)

	// Link creates newname as a hard link to the oldname file.
	Link(oldname, newname string) error

	// Open opens the named file for reading.
	Open(name string) (File, error)

	// OpenDir opens the named directory for syncing.
	OpenDir(name string) (File, error)

	// Remove removes the named file. If the file with given name doesn't
	// exist, return an error that returns true from oserror.IsNotExist().
	Remove(name string) error

	// Rename renames a file. It overwrites the file at newname if one exists,
	// the same as os.Rename.
	Rename(oldname, newname string) error

	// MkdirAll creates the named dir and its parents. Does nothing if the
	// directory already exists.
	MkdirAll(name string) error

	// RemoveDir removes the named dir.
	RemoveDir(name string) error

	// RemoveAll deletes the path and any children it contains.
	RemoveAll(dir string) error

	// List returns a listing of the given directory. The names returned are
	// relative to the directory.
	List(name string) ([]string, error)

	// Stat returns a FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)
}

// WriteFile writes data to a file named by filename.
func WriteFile(fs FS, filename string, data []byte) error {
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}
