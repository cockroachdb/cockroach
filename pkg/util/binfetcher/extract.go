// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package binfetcher

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

func untar(r io.Reader, destFile *os.File, binary string) error {
	tarReader := tar.NewReader(r)

	done := false
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
			if binary != "" {
				// Only untar the binary.
				if filepath.Base(header.Name) != binary {
					continue
				}
			}
			if done {
				return errors.New("archive contains more than one file")
			}
			if _, err := io.Copy(destFile, tarReader); err != nil {
				return err
			}
			done = true
		default:
			return errors.Errorf("unknown tar header %+v", header)
		}
	}

	if !done {
		return errors.New("empty archive")
	}
	return nil
}

func unzip(r io.Reader, destFile *os.File, binary string) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	zipReader, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return err
	}

	done := false
	for _, f := range zipReader.File {
		if binary != "" {
			// Only untar the binary.
			if filepath.Base(f.Name) != binary {
				continue
			}
		}
		if done {
			return errors.New("archive contains more than one file")
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		//nolint:deferloop TODO(#137605)
		defer rc.Close()
		if _, err := io.Copy(destFile, rc); err != nil {
			return err
		}
		done = true
	}
	if !done {
		return errors.New("empty archive")
	}
	return nil
}
