// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/pkg/filepathextended"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageutil"
)

// errNotDir is the error returned if a path does not dir.
var errNotDir = errors.New("not a directory")

type bucket struct {
	rootPath         string
	absoluteRootPath string
	symlinks         bool
}

func newBucket(rootPath string, symlinks bool) (*bucket, error) {
	rootPath = normalpath.Unnormalize(rootPath)
	if err := validateDirPathExists(rootPath, symlinks); err != nil {
		return nil, err
	}
	absoluteRootPath, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}
	// do not validate - allow anything with OS buckets including
	// absolute paths and jumping context
	rootPath = normalpath.Normalize(rootPath)
	return &bucket{
		rootPath:         rootPath,
		absoluteRootPath: absoluteRootPath,
		symlinks:         symlinks,
	}, nil
}

func (b *bucket) Get(ctx context.Context, path string) (storage.ReadObjectCloser, error) {
	externalPath, err := b.getExternalPath(path)
	if err != nil {
		return nil, err
	}
	if err := b.validateExternalPath(path, externalPath); err != nil {
		return nil, err
	}
	resolvedPath := externalPath
	if b.symlinks {
		resolvedPath, err = filepath.EvalSymlinks(externalPath)
		if err != nil {
			return nil, err
		}
	}
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, err
	}
	// we could use fileInfo.Name() however we might as well use the externalPath
	return newReadObjectCloser(
		path,
		externalPath,
		file,
	), nil
}

func (b *bucket) Stat(ctx context.Context, path string) (storage.ObjectInfo, error) {
	externalPath, err := b.getExternalPath(path)
	if err != nil {
		return nil, err
	}
	if err := b.validateExternalPath(path, externalPath); err != nil {
		return nil, err
	}
	// we could use fileInfo.Name() however we might as well use the externalPath
	return storageutil.NewObjectInfo(
		path,
		externalPath,
	), nil
}

func (b *bucket) Walk(
	ctx context.Context,
	prefix string,
	f func(storage.ObjectInfo) error,
) error {
	externalPrefix, err := b.getExternalPrefix(prefix)
	if err != nil {
		return err
	}
	walkChecker := storageutil.NewWalkChecker()
	var walkOptions []filepathextended.WalkOption
	if b.symlinks {
		walkOptions = append(walkOptions, filepathextended.WalkWithSymlinks())
	}
	if err := filepathextended.Walk(
		externalPrefix,
		func(externalPath string, fileInfo os.FileInfo, err error) error {
			if err != nil {
				// this can happen if a symlink is broken
				// in this case, we just want to continue the walk
				if b.symlinks && os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if err := walkChecker.Check(ctx); err != nil {
				return err
			}
			absoluteExternalPath, err := filepath.Abs(externalPath)
			if err != nil {
				return err
			}
			if fileInfo.Mode().IsRegular() {
				path, err := normalpath.Rel(b.absoluteRootPath, absoluteExternalPath)
				if err != nil {
					return err
				}
				// just in case
				path, err = normalpath.NormalizeAndValidate(path)
				if err != nil {
					return err
				}
				if err := f(
					storageutil.NewObjectInfo(
						path,
						externalPath,
					),
				); err != nil {
					return err
				}
			}
			return nil
		},
		walkOptions...,
	); err != nil {
		if os.IsNotExist(err) {
			// Should be a no-op according to the spec.
			return nil
		}
		return err
	}
	return nil
}

func (b *bucket) Put(ctx context.Context, path string) (storage.WriteObjectCloser, error) {
	externalPath, err := b.getExternalPath(path)
	if err != nil {
		return nil, err
	}
	externalDir := filepath.Dir(externalPath)
	var fileInfo os.FileInfo
	if b.symlinks {
		fileInfo, err = os.Stat(externalDir)
	} else {
		fileInfo, err = os.Lstat(externalDir)
	}
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(externalDir, 0755); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !fileInfo.IsDir() {
		return nil, newErrNotDir(externalDir)
	}
	file, err := os.Create(externalPath)
	if err != nil {
		return nil, err
	}
	return newWriteObjectCloser(
		file,
	), nil
}

func (b *bucket) Delete(ctx context.Context, path string) error {
	externalPath, err := b.getExternalPath(path)
	if err != nil {
		return err
	}
	// Note: this deletes the file at the path, but it may
	// leave orphan parent directories around that were
	// created by the MkdirAll in Put.
	if err := os.Remove(externalPath); err != nil {
		if os.IsNotExist(err) {
			return storage.NewErrNotExist(path)
		}
		return err
	}
	return nil
}

func (b *bucket) DeleteAll(ctx context.Context, prefix string) error {
	externalPrefix, err := b.getExternalPrefix(prefix)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(externalPrefix); err != nil {
		// this is a no-nop per the documentation
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func (*bucket) SetExternalPathSupported() bool {
	return false
}

func (b *bucket) getExternalPath(path string) (string, error) {
	path, err := storageutil.ValidatePath(path)
	if err != nil {
		return "", err
	}
	realClean, err := filepathextended.RealClean(normalpath.Join(b.rootPath, path))
	if err != nil {
		return "", err
	}
	return normalpath.Unnormalize(realClean), nil
}

func (b *bucket) validateExternalPath(path string, externalPath string) error {
	// this is potentially introducing two calls to a file
	// instead of one, ie we do both Stat and Open as opposed to just Open
	// we do this to make sure we are only reading regular files
	var fileInfo os.FileInfo
	var err error
	if b.symlinks {
		fileInfo, err = os.Stat(externalPath)
	} else {
		fileInfo, err = os.Lstat(externalPath)
	}
	if err != nil {
		if os.IsNotExist(err) {
			return storage.NewErrNotExist(path)
		}
		return err
	}
	if !fileInfo.Mode().IsRegular() {
		// making this a user error as any access means this was generally requested
		// by the user, since we only call the function for Walk on regular files
		return fmt.Errorf("%q is not a regular file", path)
	}
	return nil
}

func (b *bucket) getExternalPrefix(prefix string) (string, error) {
	prefix, err := storageutil.ValidatePrefix(prefix)
	if err != nil {
		return "", err
	}
	realClean, err := filepathextended.RealClean(normalpath.Join(b.rootPath, prefix))
	if err != nil {
		return "", err
	}
	return normalpath.Unnormalize(realClean), nil
}

type readObjectCloser struct {
	// we use ObjectInfo for Path, ExternalPath, etc to make sure this is static
	// we put ObjectInfos in maps in other places so we do not want this to change
	// this could be a problem if the underlying file is concurrently moved or resized however
	storageutil.ObjectInfo

	file *os.File
}

func newReadObjectCloser(
	path string,
	externalPath string,
	file *os.File,
) *readObjectCloser {
	return &readObjectCloser{
		ObjectInfo: storageutil.NewObjectInfo(
			path,
			externalPath,
		),
		file: file,
	}
}

func (r *readObjectCloser) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	return n, toStorageError(err)
}

func (r *readObjectCloser) Close() error {
	return toStorageError(r.file.Close())
}

type writeObjectCloser struct {
	file *os.File
}

func newWriteObjectCloser(
	file *os.File,
) *writeObjectCloser {
	return &writeObjectCloser{
		file: file,
	}
}

func (w *writeObjectCloser) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	return n, toStorageError(err)
}

func (w *writeObjectCloser) SetExternalPath(string) error {
	return storage.ErrSetExternalPathUnsupported
}

func (w *writeObjectCloser) Close() error {
	err := toStorageError(w.file.Close())
	return err
}

// newErrNotDir returns a new Error for a path not being a directory.
func newErrNotDir(path string) *normalpath.Error {
	return normalpath.NewError(path, errNotDir)
}

func toStorageError(err error) error {
	if err == os.ErrClosed {
		return storage.ErrClosed
	}
	return err
}

// validateDirPathExists returns a non-nil error if the given dirPath
// is not a valid directory path.
func validateDirPathExists(dirPath string, symlinks bool) error {
	var fileInfo os.FileInfo
	var err error
	if symlinks {
		fileInfo, err = os.Stat(dirPath)
	} else {
		fileInfo, err = os.Lstat(dirPath)
	}
	if err != nil {
		if os.IsNotExist(err) {
			return storage.NewErrNotExist(dirPath)
		}
		return err
	}
	if !fileInfo.IsDir() {
		return newErrNotDir(dirPath)
	}
	return nil
}
