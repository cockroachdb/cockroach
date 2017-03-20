// Copyright 2015 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Package securitytest embeds the TLS test certificates.
package securitytest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
)

//go:generate go-bindata -mode 0600 -modtime 1400000000 -pkg securitytest -o embedded.go -ignore README.md test_certs
//go:generate gofmt -s -w embedded.go
//go:generate goimports -w embedded.go

// RestrictedCopy creates an on-disk copy of the embedded security asset
// with the provided path. The copy will be created in the provided directory.
// Returns the path of the file and a cleanup function that will delete the file.
//
// The file will have restrictive file permissions (0600), making it
// appropriate for usage by libraries that require security assets to have such
// restrictive permissions.
func RestrictedCopy(t testing.TB, path, tempdir, name string) string {
	contents, err := Asset(path)
	if err != nil {
		t.Fatal(err)
	}
	tempPath := filepath.Join(tempdir, name)
	if err := ioutil.WriteFile(tempPath, contents, 0600); err != nil {
		t.Fatal(err)
	}
	return tempPath
}

// AssetReadDir mimics ioutil.ReadDir, returning a list of []os.FileInfo for
// the specified directory.
func AssetReadDir(name string) ([]os.FileInfo, error) {
	names, err := AssetDir(name)
	if err != nil {
		if strings.HasSuffix(err.Error(), "not found") {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	infos := make([]os.FileInfo, len(names))
	for i, n := range names {
		info, err := AssetInfo(filepath.Join(name, n))
		if err != nil {
			return nil, err
		}
		// Convert back to bindataFileInfo and strip directory from filename.
		binInfo := info.(bindataFileInfo)
		binInfo.name = filepath.Base(binInfo.name)
		infos[i] = binInfo
	}
	return infos, nil
}

// AssetStat wraps AssetInfo, but returns os.ErrNotExist if the requested
// file is not found.
func AssetStat(name string) (os.FileInfo, error) {
	info, err := AssetInfo(name)
	if err != nil && strings.HasSuffix(err.Error(), "not found") {
		return info, os.ErrNotExist
	}
	return info, err
}

// EmbeddedAssets is an AssetLoader pointing to embedded asset functions.
var EmbeddedAssets = security.AssetLoader{
	ReadDir:  AssetReadDir,
	ReadFile: Asset,
	Stat:     AssetStat,
}
