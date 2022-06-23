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

// Package filepathextended provides filepath utilities.
package filepathextended

// Walking largely copied from https://github.com/golang/go/blob/master/src/path/filepath/path.go
//
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// https://github.com/golang/go/blob/master/LICENSE

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/bufbuild/buf/private/pkg/osextended"
	"go.uber.org/multierr"
)

// RealClean does filepath.Clean and filepath.FromSlash,
// but also handles ..'s in relative paths relative to the
// current working directory.
//
// As an example, if we are in proto, and we pass in
// ../proto/foo, RealClean will return foo.
func RealClean(path string) (string, error) {
	path = filepath.Clean(filepath.FromSlash(path))
	if !filepath.IsAbs(path) {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		pwd, err := osextended.Getwd()
		if err != nil {
			return "", err
		}
		if relPath, err := filepath.Rel(pwd, absPath); err == nil {
			return relPath, nil
		}
	}
	return path, nil
}

// Walk walks the walkPath.
//
// This is analogous to filepath.Walk, but optionally follows symlinks.
func Walk(walkPath string, walkFunc filepath.WalkFunc, options ...WalkOption) (retErr error) {
	defer func() {
		// If we end up with a SkipDir, this isn't an error.
		if retErr == filepath.SkipDir {
			retErr = nil
		}
	}()
	walkOptions := newWalkOptions()
	for _, option := range options {
		option(walkOptions)
	}
	// os.Lstat does not follow symlinks, while os.Stat does.
	fileInfo, err := os.Lstat(walkPath)
	if err != nil {
		// If we have an error, then we still walk to call walkFunc with the error.
		return walkFunc(walkPath, nil, err)
	}
	resolvedPath, fileInfo, err := optionallyEvaluateSymlink(walkPath, fileInfo, walkOptions.symlinks)
	if err != nil {
		// If we have an error, then we still walk to call walkFunc with the error.
		return walkFunc(walkPath, nil, err)
	}
	return walk(walkPath, resolvedPath, fileInfo, walkFunc, make(map[string]struct{}), walkOptions.symlinks)
}

// WalkOption is an option for Walk.
type WalkOption func(*walkOptions)

// WalkWithSymlinks returns a WalkOption that results in Walk following symlinks.
func WalkWithSymlinks() WalkOption {
	return func(walkOptions *walkOptions) {
		walkOptions.symlinks = true
	}
}

// walkPath is the path we give to the WalkFunc
// resolvedPath is the potentially-resolved path that we actually read from.
func walk(
	walkPath string,
	resolvedPath string,
	fileInfo os.FileInfo,
	walkFunc filepath.WalkFunc,
	resolvedPathMap map[string]struct{},
	symlinks bool,
) error {
	if symlinks {
		if _, ok := resolvedPathMap[resolvedPath]; ok {
			// Do not walk down this path.
			// We could later make it optional to error in this case.
			return nil
		}
		resolvedPathMap[resolvedPath] = struct{}{}
	}

	// If this is not a directory, just call walkFunc on it and we're done.
	if !fileInfo.IsDir() {
		return walkFunc(walkPath, fileInfo, nil)
	}

	// This is a directory, read it.
	subNames, readDirErr := readDirNames(resolvedPath)
	walkErr := walkFunc(walkPath, fileInfo, readDirErr)
	// If readDirErr != nil, walk can't walk into this directory.
	// walkErr != nil means walkFunc want walk to skip this directory or stop walking.
	// Therefore, if one of readDirErr and walkErr isn't nil, walk will return.
	if readDirErr != nil || walkErr != nil {
		// The caller's behavior is controlled by the return value, which is decided
		// by walkFunc. walkFunc may ignore readDirErr and return nil.
		// If walkFunc returns SkipDir, it will be handled by the caller.
		// So walk should return whatever walkFunc returns.
		return walkErr
	}

	for _, subName := range subNames {
		// The path we want to pass to walk is the directory walk path plus the name.
		subWalkPath := filepath.Join(walkPath, subName)
		// The path we want to actually used is the directory resolved path plus the name.
		// This is potentially a symlink-evaluated path.
		subResolvedPath := filepath.Join(resolvedPath, subName)
		subFileInfo, err := os.Lstat(subResolvedPath)
		if err != nil {
			// If we have an error, still call walkFunc and match filepath.Walk.
			if walkErr := walkFunc(subWalkPath, subFileInfo, err); walkErr != nil && walkErr != filepath.SkipDir {
				return walkErr
			}
			// No error, just continue the for loop.
			// Note that filepath.Walk does an else block instead, but we want to match
			// the same code as in the symlink if statement below.
			continue
		}
		subResolvedPath, subFileInfo, err = optionallyEvaluateSymlink(subResolvedPath, subFileInfo, symlinks)
		if err != nil {
			// If we have an error, still call walkFunc and match filepath.Walk.
			if walkErr := walkFunc(subWalkPath, subFileInfo, err); walkErr != nil && walkErr != filepath.SkipDir {
				return walkErr
			}
			// No error, just continue the for loop.
			continue
		}
		if err := walk(subWalkPath, subResolvedPath, subFileInfo, walkFunc, resolvedPathMap, symlinks); err != nil {
			// If not a directory, return the error.
			// Else, if the error is filepath.SkipDir, return the error.
			// Else, this is a directory and we have filepath.SkipDir, do not return the error and continue.
			if !subFileInfo.IsDir() || err != filepath.SkipDir {
				return err
			}
		}
	}

	return nil
}

// readDirNames reads the directory named by dirname and returns
// a sorted list of directory entries.
//
// We need to use this instead of os.ReadDir because we want to do the os.Lstat ourselves
// separately to completely match filepath.Walk.
func readDirNames(dirPath string) (_ []string, retErr error) {
	file, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = multierr.Append(retErr, file.Close())
	}()
	dirNames, err := file.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(dirNames)
	return dirNames, nil
}

type walkOptions struct {
	symlinks bool
}

func newWalkOptions() *walkOptions {
	return &walkOptions{}
}

// returns optionally-resolved path, optionally-resolved os.FileInfo
func optionallyEvaluateSymlink(filePath string, fileInfo os.FileInfo, symlinks bool) (string, os.FileInfo, error) {
	if !symlinks {
		return filePath, fileInfo, nil
	}
	if fileInfo.Mode()&os.ModeSymlink != os.ModeSymlink {
		return filePath, fileInfo, nil
	}
	resolvedFilePath, err := filepath.EvalSymlinks(filePath)
	if err != nil {
		return filePath, fileInfo, err
	}
	resolvedFileInfo, err := os.Lstat(resolvedFilePath)
	if err != nil {
		return filePath, fileInfo, err
	}
	return resolvedFilePath, resolvedFileInfo, nil
}
