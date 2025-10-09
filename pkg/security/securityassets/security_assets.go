// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package securityassets

import (
	"io/fs"
	"os"

	"github.com/cockroachdb/errors/oserror"
)

// Loader describes the functions necessary to read certificate and key files.
type Loader struct {
	ReadDir  func(dirname string) ([]os.FileInfo, error)
	ReadFile func(filename string) ([]byte, error)
	Stat     func(name string) (os.FileInfo, error)
}

// defaultLoader uses real filesystem calls.
var defaultLoader = Loader{
	ReadDir:  readDir,
	ReadFile: os.ReadFile,
	Stat:     os.Stat,
}

// loaderImpl is used to list/read/stat security assets.
var loaderImpl = defaultLoader

// GetLoader returns the active asset loader.
func GetLoader() Loader {
	return loaderImpl
}

// SetLoader overrides the asset loader with the passed-in one.
func SetLoader(al Loader) {
	loaderImpl = al
}

// ResetLoader restores the asset loader to the default value.
func ResetLoader() {
	loaderImpl = defaultLoader
}

// FileExists returns true iff the target file already exists.
func (al Loader) FileExists(filename string) (bool, error) {
	_, err := al.Stat(filename)
	if err != nil {
		if oserror.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// readDir reads the directory named by dirname and returns a list of
// fs.FileInfo for the directory's contents, sorted by filename. If an error
// occurs reading the directory, ReadDir returns no directory entries along with
// the error.
func readDir(dirname string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	infos := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}
