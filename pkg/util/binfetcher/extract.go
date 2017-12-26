// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package binfetcher

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
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
