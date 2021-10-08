// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package securitytest embeds the TLS test certificates.
package securitytest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
)

//go:generate go-bindata -mode 0600 -modtime 1400000000 -pkg securitytest -o embedded.go -ignore README.md -ignore regenerate.sh test_certs
//go:generate gofmt -s -w embedded.go
//go:generate goimports -w embedded.go

// RestrictedCopy creates an on-disk copy of the embedded security asset
// with the provided path. The copy will be created in the provided directory.
// Returns the path of the file and a cleanup function that will delete the file.
//
// The file will have restrictive file permissions (0600), making it
// appropriate for usage by libraries that require security assets to have such
// restrictive permissions.
func RestrictedCopy(path, tempdir, name string) (string, error) {
	contents, err := Asset(path)
	if err != nil {
		return "", err
	}
	tempPath := filepath.Join(tempdir, name)
	if err := ioutil.WriteFile(tempPath, contents, 0600); err != nil {
		return "", err
	}
	return tempPath, nil
}

// AssetReadDir mimics ioutil.ReadDir, returning a list of []os.FileInfo for
// the specified directory. Contrary to ioutil.ReadDir however, it skips sub-
// directories.
func AssetReadDir(name string) ([]os.FileInfo, error) {
	names, err := AssetDir(name)
	if err != nil {
		if strings.HasSuffix(err.Error(), "not found") {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(names))
	for _, n := range names {
		joined := filepath.Join(name, n)
		info, err := AssetInfo(joined)
		if err != nil {
			if _, dirErr := AssetDir(joined); dirErr != nil {
				return nil, errors.Wrapf(err, "missing directory (%s)", dirErr)
			}
			continue // skip subdirectory
		}
		// Convert back to bindataFileInfo and strip directory from filename.
		binInfo := info.(bindataFileInfo)
		binInfo.name = filepath.Base(binInfo.name)
		infos = append(infos, binInfo)
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
