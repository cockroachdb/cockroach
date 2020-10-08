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

// LocalStorage wraps all operations with the local file system
// that the blob service makes.
type LocalStorage struct {
	externalIODir string
}

// NewLocalStorage creates a new LocalStorage object and returns
// an error when we cannot take the absolute path of `externalIODir`.
func NewLocalStorage(externalIODir string) (*LocalStorage, error) {
	// An empty externalIODir indicates external IO is completely disabled.
	// Returning a nil *LocalStorage in this case and then hanldling `nil` in the
	// prependExternalIODir helper ensures that that is respected throughout the
	// implementation (as a failure to do so would likely fail loudly with a
	// nil-pointer dereference).
	if externalIODir == "" {
		return nil, nil
	}
	absPath, err := filepath.Abs(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating LocalStorage object")
	}
	return &LocalStorage{externalIODir: absPath}, nil
}

// prependExternalIODir makes `path` relative to the configured external I/O directory.
//
// Note that we purposefully only rely on the simplified cleanup
// performed by filepath.Join() - which is limited to stripping out
// occurrences of "../" - because we intendedly want to allow
// operators to "open up" their I/O directory via symlinks. Therefore,
// a full check via filepath.Abs() would be inadequate.
func (l *LocalStorage) prependExternalIODir(path string) (string, error) {
	if l == nil {
		return "", errors.Errorf("local file access is disabled")
	}
	localBase := filepath.Join(l.externalIODir, path)
	if !strings.HasPrefix(localBase, l.externalIODir) {
		return "", errors.Errorf("local file access to paths outside of external-io-dir is not allowed: %s", path)
	}
	return localBase, nil
}

// WriteFile prepends IO dir to filename and writes the content to that local file.
func (l *LocalStorage) WriteFile(filename string, content io.Reader) (err error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return err
	}

	targetDir := filepath.Dir(fullPath)
	if err = os.MkdirAll(targetDir, 0755); err != nil {
		return errors.Wrapf(err, "creating target local directory %q", targetDir)
	}

	// We generate the temporary file in the desired target directory.
	// This has two purposes:
	// - it avoids relying on the system-wide temporary directory, which
	//   may not be large enough to receive the file.
	// - it avoids a cross-filesystem rename in the common case.
	//   (There can still be cross-filesystem renames in very
	//   exotic edge cases, hence the use fileutil.Move below.)
	// See the explanatory comment for ioutil.TempFile to understand
	// what the "*" in the suffix means.
	tmpFile, err := ioutil.TempFile(targetDir, filepath.Base(fullPath)+"*.tmp")
	if err != nil {
		return errors.Wrap(err, "creating temporary file")
	}
	tmpFileFullName := tmpFile.Name()
	defer func() {
		if err != nil {
			// When an error occurs, we need to clean up the newly created
			// temporary file.
			_ = os.Remove(tmpFileFullName)
			//
			// TODO(someone): in the special case where an attempt is made
			// to upload to a sub-directory of the ext i/o dir for the first
			// time (MkdirAll above did create the sub-directory), and the
			// copy/rename fails, we're now left with a newly created but empty
			// sub-directory.
			//
			// We cannot safely remove that target directory here, because
			// perhaps there is another concurrent operation that is also
			// targeting it. A more principled approach could be to use a
			// mutex lock on directory accesses, and/or occasionally prune
			// empty sub-directories upon node start-ups.
		}
	}()

	// Copy the data into the temp file. We use a closure here to
	// ensure the temp file is closed after the copy is done.
	if err = func() error {
		defer tmpFile.Close()
		if _, err := io.Copy(tmpFile, content); err != nil {
			return errors.Wrapf(err, "writing to temporary file %q", tmpFileFullName)
		}
		return errors.Wrapf(tmpFile.Sync(), "flushing temporary file %q", tmpFileFullName)
	}(); err != nil {
		return err
	}

	// Finally put the file to its final location.
	return errors.Wrapf(
		fileutil.Move(tmpFileFullName, fullPath),
		"moving temporary file to final location %q", fullPath)
}

// ReadFile prepends IO dir to filename and reads the content of that local file.
func (l *LocalStorage) ReadFile(filename string) (res io.ReadCloser, err error) {
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
func (l *LocalStorage) List(pattern string) ([]string, error) {
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
		fileList = append(fileList, strings.TrimPrefix(file, l.externalIODir))
	}
	return fileList, nil
}

// Delete prepends IO dir to filename and deletes that local file.
func (l *LocalStorage) Delete(filename string) error {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return errors.Wrap(err, "deleting file")
	}
	return os.Remove(fullPath)
}

// Stat prepends IO dir to filename and gets the Stat() of that local file.
func (l *LocalStorage) Stat(filename string) (*blobspb.BlobStat, error) {
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
