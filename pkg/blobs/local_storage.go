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
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
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
	// Returning a nil *LocalStorage in this case and then handling `nil` in the
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
	return localBase, l.ensureContained(localBase, path)
}

func (l *LocalStorage) ensureContained(realPath, inputPath string) error {
	if !strings.HasPrefix(realPath, l.externalIODir) {
		return errors.Errorf("local file access to paths outside of external-io-dir is not allowed: %s", inputPath)
	}
	return nil
}

type localWriter struct {
	f         *os.File
	ctx       context.Context
	tmp, dest string
}

func (l localWriter) Write(p []byte) (int, error) {
	return l.f.Write(p)
}

func (l localWriter) Close() error {
	if err := l.ctx.Err(); err != nil {
		closeErr := l.f.Close()
		rmErr := os.Remove(l.tmp)
		return errors.CombineErrors(err, errors.Wrap(errors.CombineErrors(rmErr, closeErr), "cleaning up"))
	}

	syncErr := l.f.Sync()
	closeErr := l.f.Close()
	if err := errors.CombineErrors(closeErr, syncErr); err != nil {
		return err
	}
	// Finally put the file to its final location.
	return errors.Wrapf(
		fileutil.Move(l.tmp, l.dest),
		"moving temporary file to final location %q",
		l.dest,
	)
}

// Writer prepends IO dir to filename and writes the content to that local file.
func (l *LocalStorage) Writer(ctx context.Context, filename string) (io.WriteCloser, error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	targetDir := filepath.Dir(fullPath)
	if err = os.MkdirAll(targetDir, 0755); err != nil {
		return nil, errors.Wrapf(err, "creating target local directory %q", targetDir)
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
		return nil, errors.Wrap(err, "creating temporary file")
	}
	return localWriter{tmp: tmpFile.Name(), dest: fullPath, f: tmpFile, ctx: ctx}, nil
}

// ReadFile prepends IO dir to filename and reads the content of that local file.
func (l *LocalStorage) ReadFile(
	filename string, offset int64,
) (res ioctx.ReadCloserCtx, size int64, err error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if fi.IsDir() {
		return nil, 0, errors.Errorf("expected a file but %q is a directory", fi.Name())
	}
	if offset != 0 {
		if ret, err := f.Seek(offset, 0); err != nil {
			return nil, 0, err
		} else if ret != offset {
			return nil, 0, errors.Errorf("seek to offset %d returned %d", offset, ret)
		}
	}
	return ioctx.ReadCloserAdapter(f), fi.Size(), nil
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

	// If we are not given a glob pattern, we should recursively list this prefix
	// just like a cloud storage provider, using filepath.Walk, because absent a
	// wildcard in a pattern filepath.Glob matches at most one path.
	// TODO(dt): make this the only case -- never pass a pattern and always just
	// walk the prefix like a cloud storage listing API.
	if !strings.ContainsAny(pattern, "*?[") {
		var matches []string
		walkRoot := fullPath
		listingParent := false
		if f, err := os.Stat(fullPath); err != nil || !f.IsDir() {
			listingParent = true
			walkRoot = filepath.Dir(fullPath)
			if err := l.ensureContained(walkRoot, pattern); err != nil {
				return nil, err
			}
		}

		if err := filepath.Walk(walkRoot, func(p string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if f.IsDir() {
				return nil
			}
			if listingParent && !strings.HasPrefix(p, fullPath) {
				return nil
			}
			matches = append(matches, strings.TrimPrefix(p, l.externalIODir))
			return nil
		}); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
		return matches, nil
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
