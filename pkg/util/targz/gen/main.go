// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Given the path to a directory and an output path, this executable creates a
// .tar.gz archive. This is not feature-complete (at all) compared to the `tar`
// utility, but works for the purposes we have (i.e. packaging UI assets).

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

func run() error {
	if len(os.Args) < 3 {
		return errors.Newf("usage: %s OUTFILE SRCFILE [SRCFILE...]\n", os.Args[0])
	}
	os.Args[1] = strings.TrimRight(os.Args[1], "/")

	// Make tar archive
	var tarContents bytes.Buffer
	tarWriter := tar.NewWriter(&tarContents)
	for _, srcFile := range os.Args[2:] {
		err := filepath.WalkDir(srcFile, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if path == srcFile && d.IsDir() {
				return nil
			}
			if d.IsDir() {
				return errors.Newf("cannot compress subdirectory %s", path)
			}
			src, err := os.Open(path)
			if err != nil {
				return err
			}
			info, err := src.Stat()
			if err != nil {
				return err
			}
			err = tarWriter.WriteHeader(&tar.Header{Name: d.Name(), Size: info.Size()})
			if err != nil {
				return errors.Wrap(err, "could not write header to tar file")
			}
			_, err = io.Copy(tarWriter, src)
			if err != nil {
				return err
			}
			return src.Close()
		})
		if err != nil {
			return err
		}
	}
	err := tarWriter.Close()
	if err != nil {
		return err
	}

	// compress tar archive w/ gzip
	outFile, err := os.Create(os.Args[1])
	if err != nil {
		return err
	}
	gzipWriter := gzip.NewWriter(outFile)
	_, err = gzipWriter.Write(tarContents.Bytes())
	if err != nil {
		return err
	}
	err = gzipWriter.Close()
	if err != nil {
		return err
	}
	return outFile.Close()
}
