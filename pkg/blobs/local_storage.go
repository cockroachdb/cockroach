// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blobs

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/errors"
)

// localStorage wraps all operations with the local file system
// that the blob service makes.
type localStorage struct {
	externalIODir string
}

// newLocalStorage creates a new localStorage object and returns
// an error when we cannot take the absolute path of `externalIODir`.
func newLocalStorage(externalIODir string) (*localStorage, error) {
	absPath, err := filepath.Abs(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating localStorage object")
	}
	return &localStorage{externalIODir: absPath}, nil
}

func (l *localStorage) prependExternalIODir(path string) (string, error) {
	localBase := filepath.Join(l.externalIODir, path)
	// Whilst filepath.Join does some pruning by eliminating occurrences
	// of "../" in the text of the path, there can still be symlinks in
	// there, or invalid path components like the special names on
	// windows. We really want to check upfront that the path is valid.
	// filepath.Abs() will do this for us.
	absPath, err := filepath.Abs(localBase)
	if err != nil {
		// We do not keep the error directly visible, because that could
		// leak information about the directories "above" the external I/O
		// dir.
		return "", errors.HandledWithMessage(err, "computing path: "+path)
	}
	localBase = absPath
	// Make sure no symlinks accessed a way out.
	if !strings.HasPrefix(localBase, l.externalIODir) {
		return "", errors.Errorf("local file access to paths outside of external-io-dir is not allowed: %s", path)
	}
	return localBase, nil
}

// WriteFile prepends IO dir to filename and writes the content to that local file.
func (l *localStorage) WriteFile(filename string, content io.Reader) error {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return err
	}
	tmpFile, err := ioutil.TempFile(os.TempDir(), filepath.Base(fullPath))
	if err != nil {
		return errors.Wrap(err, "creating tmp file")
	}
	// TODO(georgiah): cleanup intermediate state on failure
	defer tmpFile.Close()
	_, err = io.Copy(tmpFile, content)
	if err != nil {
		return errors.Wrapf(err, "writing to local external tmp file %q", tmpFile.Name())
	}
	if err := tmpFile.Sync(); err != nil {
		return errors.Wrapf(err, "syncing to local external tmp file %q", tmpFile.Name())
	}
	if err = os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return errors.Wrap(err, "creating local external storage path")
	}
	return errors.Wrapf(fileutil.Move(tmpFile.Name(), fullPath), "renaming to local export file %q", fullPath)
}

// ReadFile prepends IO dir to filename and reads the content of that local file.
func (l *localStorage) ReadFile(filename string) (res io.ReadCloser, err error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.Errorf("expected a file but %q is a directory", fi.Name())
	}
	return f, nil
}

// List prepends IO dir to pattern and glob matches all local files against that pattern.
func (l *localStorage) List(pattern string) ([]string, error) {
	if pattern == "" {
		return nil, errors.New("pattern cannot be empty")
	}
	fullPath, err := l.prependExternalIODir(pattern)
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(fullPath)
	if err != nil {
		return nil, err
	}

	var fileList []string
	for _, file := range matches {
		fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(file, l.externalIODir), "/"))
	}
	return fileList, nil
}

// Delete prepends IO dir to filename and deletes that local file.
func (l *localStorage) Delete(filename string) error {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return errors.Wrap(err, "deleting file")
	}
	return os.Remove(fullPath)
}

// Stat prepends IO dir to filename and gets the Stat() of that local file.
func (l *localStorage) Stat(filename string) (*blobspb.BlobStat, error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, errors.Wrap(err, "getting stat of file")
	}
	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.Errorf("expected a file but %q is a directory", fi.Name())
	}
	return &blobspb.BlobStat{Filesize: fi.Size()}, nil
}
