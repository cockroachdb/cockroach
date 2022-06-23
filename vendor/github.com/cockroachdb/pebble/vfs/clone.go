// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io/ioutil"
	"sort"

	"github.com/cockroachdb/errors/oserror"
)

type cloneOpts struct {
	skip func(string) bool
	sync bool
}

// A CloneOption configures the behavior of Clone.
type CloneOption func(*cloneOpts)

// CloneSkip configures Clone to skip files for which the provided function
// returns true when passed the file's path.
func CloneSkip(fn func(string) bool) CloneOption {
	return func(co *cloneOpts) { co.skip = fn }
}

// CloneSync configures Clone to sync files and directories.
var CloneSync CloneOption = func(o *cloneOpts) { o.sync = true }

// Clone recursively copies a directory structure from srcFS to dstFS. srcPath
// specifies the path in srcFS to copy from and must be compatible with the
// srcFS path format. dstDir is the target directory in dstFS and must be
// compatible with the dstFS path format. Returns (true,nil) on a successful
// copy, (false,nil) if srcPath does not exist, and (false,err) if an error
// occurred.
func Clone(srcFS, dstFS FS, srcPath, dstPath string, opts ...CloneOption) (bool, error) {
	var o cloneOpts
	for _, opt := range opts {
		opt(&o)
	}

	srcFile, err := srcFS.Open(srcPath)
	if err != nil {
		if oserror.IsNotExist(err) {
			// Ignore non-existent errors. Those will translate into non-existent
			// files in the destination filesystem.
			return false, nil
		}
		return false, err
	}

	stat, err := srcFile.Stat()
	if err != nil {
		return false, err
	}

	if stat.IsDir() {
		if err := srcFile.Close(); err != nil {
			return false, err
		}
		if err := dstFS.MkdirAll(dstPath, 0755); err != nil {
			return false, err
		}
		list, err := srcFS.List(srcPath)
		if err != nil {
			return false, err
		}
		// Sort the paths so we get deterministic test output.
		sort.Strings(list)
		for _, name := range list {
			if o.skip != nil && o.skip(srcFS.PathJoin(srcPath, name)) {
				continue
			}
			_, err := Clone(srcFS, dstFS, srcFS.PathJoin(srcPath, name), dstFS.PathJoin(dstPath, name), opts...)
			if err != nil {
				return false, err
			}
		}

		if o.sync {
			dir, err := dstFS.OpenDir(dstPath)
			if err != nil {
				return false, err
			}
			if err := dir.Sync(); err != nil {
				return false, err
			}
			if err := dir.Close(); err != nil {
				return false, err
			}
		}

		return true, nil
	}

	data, err := ioutil.ReadAll(srcFile)
	if err != nil {
		return false, err
	}
	if err := srcFile.Close(); err != nil {
		return false, err
	}
	dstFile, err := dstFS.Create(dstPath)
	if err != nil {
		return false, err
	}
	if _, err = dstFile.Write(data); err != nil {
		return false, err
	}
	if o.sync {
		if err := dstFile.Sync(); err != nil {
			return false, err
		}
	}

	if err := dstFile.Close(); err != nil {
		return false, err
	}
	return true, nil
}
