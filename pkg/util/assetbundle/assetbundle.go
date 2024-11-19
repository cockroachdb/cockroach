// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package assetbundle can expose a very rudimentary fs.FS given a .tar.zst-encoded
// source file. This package is very much not feature-complete and is currently
// only used _internally_ to expose UI assets as an fs.FS.
package assetbundle

import (
	"archive/tar"
	"bytes"
	"io"
	"io/fs"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/afero"
)

// AsFS exposes the contents of the given reader (which is a .tar.zst file)
// as an fs.FS.
func AsFS(r io.Reader) (fs.FS, error) {
	zst, err := zstd.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "could not open .tar.zst file")
	}
	defer zst.Close()

	var tarContents bytes.Buffer
	if _, err := io.Copy(&tarContents, zst); err != nil {
		return nil, errors.Wrap(err, "could not decompress .tar.zst file")
	}
	tarReader := tar.NewReader(bytes.NewBuffer(tarContents.Bytes()))

	// Create an io/fs.FS alternative that's stored purely in memory
	fsys := afero.NewMemMapFs()

	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "error reading .tar.zst entry")
		}

		if err := fsys.MkdirAll(filepath.Dir(hdr.Name), fs.ModeDir); err != nil {
			return nil, errors.Wrapf(err, "error creating virtual parent directory for .tar.zst file '%s'", hdr.Name)
		}
		if err := afero.WriteReader(fsys, hdr.Name, tarReader); err != nil {
			return nil, errors.Wrap(err, "error reading .tar.zst entry")
		}
	}

	// Create a read-only io/fs.FS suitable for external use
	iofs := afero.NewIOFS(fsys)
	return iofs, nil
}
