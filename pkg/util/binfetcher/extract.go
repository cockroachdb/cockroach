// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package binfetcher

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"os"

	"github.com/cockroachdb/errors"
)

func untar(r io.Reader, destFile *os.File) error {
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

func unzip(r io.Reader, destFile *os.File) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	zipReader, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return err
	}

	done := false
	for _, f := range zipReader.File {
		if done {
			return errors.New("archive contains more than one file")
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
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
