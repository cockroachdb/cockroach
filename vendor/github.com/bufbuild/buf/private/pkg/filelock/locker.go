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

package filelock

import (
	"context"
	"fmt"
	"os"

	"github.com/bufbuild/buf/private/pkg/normalpath"
)

type locker struct {
	rootDirPath string
}

func newLocker(rootDirPath string) (*locker, error) {
	// allow symlinks
	fileInfo, err := os.Stat(normalpath.Unnormalize(rootDirPath))
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, fmt.Errorf("%q is not a directory", rootDirPath)
	}
	return &locker{
		// do not validate - allow anything including absolute paths and jumping context
		rootDirPath: normalpath.Normalize(rootDirPath),
	}, nil
}

func (l *locker) Lock(ctx context.Context, path string, options ...LockOption) (Unlocker, error) {
	if err := validatePath(path); err != nil {
		return nil, err
	}
	return lock(
		ctx,
		normalpath.Unnormalize(normalpath.Join(l.rootDirPath, path)),
		options...,
	)
}

func (l *locker) RLock(ctx context.Context, path string, options ...LockOption) (Unlocker, error) {
	if err := validatePath(path); err != nil {
		return nil, err
	}
	return rlock(
		ctx,
		normalpath.Unnormalize(normalpath.Join(l.rootDirPath, path)),
		options...,
	)
}

func validatePath(path string) error {
	normalPath, err := normalpath.NormalizeAndValidate(path)
	if err != nil {
		return err
	}
	if path != normalPath {
		// just extra safety
		return fmt.Errorf("expected file lock path %q to be equal to normalized path %q", path, normalPath)
	}
	return nil
}
