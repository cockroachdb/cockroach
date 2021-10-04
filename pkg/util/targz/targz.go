// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package targz can expose a very rudimentary fs.FS given a .tar.gz-encoded
// source file. This package is very much not feature-complete and is currently
// only used to expose UI assets as an fs.FS.
package targz

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type tarGzFs struct {
	files map[string][]byte
}

type tarGzFile struct {
	name     string
	size     int64
	contents *bytes.Buffer
}

// AsFS exposes the contents of the given reader (which is a .tar.gz file)
// as an fs.FS.
func AsFS(r io.Reader) (fs.FS, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "could not open .tar.gz file")
	}
	var tarContents bytes.Buffer
	if _, err := io.Copy(&tarContents, gz); err != nil {
		return nil, errors.Wrap(err, "could not decompress .tar.gz file")
	}
	if err := gz.Close(); err != nil {
		return nil, errors.Wrap(err, "could not close gzip reader")
	}
	tarReader := tar.NewReader(bytes.NewBuffer(tarContents.Bytes()))
	var ret tarGzFs
	ret.files = make(map[string][]byte)
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "error reading .tar.gz entry")
		}
		var fileContents bytes.Buffer
		if _, err := io.Copy(&fileContents, tarReader); err != nil {
			return nil, errors.Wrap(err, "error reading .tar.gz entry")
		}
		ret.files[hdr.Name] = fileContents.Bytes()
	}
	return &ret, nil
}

// Implementation of fs.FS for *tarGzFs.

// Open opens the named file.
func (gzfs *tarGzFs) Open(name string) (fs.File, error) {
	contents, ok := gzfs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &tarGzFile{name: name, contents: bytes.NewBuffer(contents), size: int64(len(contents))}, nil
}

// Implementation of fs.File for tarGzFile.

// Stat returns a fs.FileInfo for the given file.
func (f *tarGzFile) Stat() (fs.FileInfo, error) {
	return f, nil
}

// Read reads the next len(p) bytes from the buffer or until the buffer is drained.
func (f *tarGzFile) Read(buf []byte) (int, error) {
	return f.contents.Read(buf)
}

// Close is a no-op.
func (f *tarGzFile) Close() error {
	return nil
}

// Implementation of fs.FileInfo for tarGzFile.

// Name returns the basename of the file.
func (f *tarGzFile) Name() string {
	return f.name
}

// Size returns the length in bytes for this file.
func (f *tarGzFile) Size() int64 {
	return f.size
}

// Mode returns the mode for this file.
func (*tarGzFile) Mode() fs.FileMode {
	return 0444
}

// Mode returns the mtime for this file (always the Unix epoch).
func (*tarGzFile) ModTime() time.Time {
	return timeutil.Unix(0, 0)
}

// IsDir returns whether this file is a directory (always false).
func (*tarGzFile) IsDir() bool {
	return false
}

// Sys returns nil.
func (*tarGzFile) Sys() interface{} {
	return nil
}
