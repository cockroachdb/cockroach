// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package securitytest embeds the TLS test certificates.
package securitytest

import (
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/errors"
)

//go:embed test_certs/*
var testCerts embed.FS

// fileInfo is a struct that implements os.FileInfo, wrapping an inner
// type. It exists so we can update the overly-broad permissions that embed.FS
// reports to be a little more strict.
type fileInfo struct {
	inner os.FileInfo
}

// Name implements os.FileInfo.
func (i *fileInfo) Name() string {
	return i.inner.Name()
}

// Size implements os.FileInfo
func (i *fileInfo) Size() int64 {
	return i.inner.Size()
}

// Mode implements os.FileInfo. We wrap to make the permissions more
// restrictive.
func (i *fileInfo) Mode() fs.FileMode {
	m := i.inner.Mode()
	return (m & (0700 | fs.ModeDir))
}

// ModTime implements os.FileInfo.
func (i *fileInfo) ModTime() time.Time {
	return i.inner.ModTime()
}

// IsDir implements os.FileInfo.
func (i *fileInfo) IsDir() bool {
	return i.inner.IsDir()
}

// Sys implements os.FileInfo.
func (i *fileInfo) Sys() any {
	return i.inner.Sys()
}

var _ os.FileInfo = &fileInfo{}

// RestrictedCopy creates an on-disk copy of the embedded security asset
// with the provided path. The copy will be created in the provided directory.
// Returns the path of the file and a cleanup function that will delete the file.
//
// The file will have restrictive file permissions (0600), making it
// appropriate for usage by libraries that require security assets to have such
// restrictive permissions.
func RestrictedCopy(path, tempdir, name string) (string, error) {
	contents, err := testCerts.ReadFile(path)
	if err != nil {
		return "", err
	}
	tempPath := filepath.Join(tempdir, name)
	if err := os.WriteFile(tempPath, contents, 0600); err != nil {
		return "", err
	}
	return tempPath, nil
}

// AppendFile appends an on-disk copy of the embedded security asset
// with the provided path, to the file designated by the second path.
func AppendFile(assetPath, dstPath string) error {
	contents, err := testCerts.ReadFile(assetPath)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_APPEND, 0 /* unused */)
	if err != nil {
		return err
	}
	_, err = f.Write(contents)
	return errors.CombineErrors(err, f.Close())
}

// AssetReadDir mimics ioutil.ReadDir, returning a list of []os.FileInfo for
// the specified directory.
func AssetReadDir(name string) ([]os.FileInfo, error) {
	entries, err := testCerts.ReadDir(name)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(e.Name(), ".md") ||
			strings.HasSuffix(e.Name(), ".sh") ||
			strings.HasSuffix(e.Name(), ".cnf") {
			continue
		}
		infos = append(infos, &fileInfo{inner: info})
	}
	return infos, nil
}

// AssetStat wraps Stat().
func AssetStat(name string) (os.FileInfo, error) {
	f, err := testCerts.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	return &fileInfo{inner: info}, err
}

// Asset wraps ReadFile.
func Asset(name string) ([]byte, error) {
	return testCerts.ReadFile(name)
}

// EmbeddedAssets is an AssetLoader pointing to embedded asset functions.
var EmbeddedAssets = securityassets.Loader{
	ReadDir:  AssetReadDir,
	ReadFile: testCerts.ReadFile,
	Stat:     AssetStat,
}
