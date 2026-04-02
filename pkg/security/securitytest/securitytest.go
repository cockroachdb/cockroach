// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package securitytest provides test TLS certificates for CockroachDB tests.
// Certificates are generated programmatically at runtime on first access,
// eliminating the need for static certificate files that expire periodically.
package securitytest

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/errors"
)

// fileInfo implements os.FileInfo backed by in-memory cert data.
type fileInfo struct {
	name string
	size int64
	dir  bool
}

func (i *fileInfo) Name() string { return i.name }
func (i *fileInfo) Size() int64  { return i.size }
func (i *fileInfo) Mode() fs.FileMode {
	if i.dir {
		return 0700 | fs.ModeDir
	}
	return 0700
}
func (i *fileInfo) ModTime() time.Time { return time.Time{} }
func (i *fileInfo) IsDir() bool        { return i.dir }
func (i *fileInfo) Sys() any           { return nil }

var _ os.FileInfo = &fileInfo{}

// dirEntry implements os.DirEntry backed by in-memory cert data.
type dirEntry struct {
	info *fileInfo
}

func (e *dirEntry) Name() string               { return e.info.name }
func (e *dirEntry) IsDir() bool                { return e.info.dir }
func (e *dirEntry) Type() fs.FileMode          { return e.info.Mode().Type() }
func (e *dirEntry) Info() (fs.FileInfo, error) { return e.info, nil }

var _ os.DirEntry = &dirEntry{}

// RestrictedCopy creates an on-disk copy of the generated security asset
// with the provided path. The copy will be created in the provided directory.
// Returns the path of the file and a cleanup function that will delete the file.
//
// The file will have restrictive file permissions (0600), making it
// appropriate for usage by libraries that require security assets to have such
// restrictive permissions.
func RestrictedCopy(path, tempdir, name string) (string, error) {
	contents, ok := generatedCerts()[path]
	if !ok {
		return "", errors.Wrapf(os.ErrNotExist,
			"securitytest: asset not found: %s", path)
	}
	tempPath := filepath.Join(tempdir, name)
	if err := os.WriteFile(tempPath, contents, 0600); err != nil {
		return "", err
	}
	return tempPath, nil
}

// AppendFile appends the generated security asset with the provided path
// to the file designated by the second path.
func AppendFile(assetPath, dstPath string) error {
	contents, ok := generatedCerts()[assetPath]
	if !ok {
		return errors.Wrapf(os.ErrNotExist,
			"securitytest: asset not found: %s", assetPath)
	}
	f, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_APPEND, 0 /* unused */)
	if err != nil {
		return err
	}
	_, err = f.Write(contents)
	return errors.CombineErrors(err, f.Close())
}

// AssetReadDir returns a list of []os.DirEntry for the specified directory
// within the generated certificate store.
func AssetReadDir(name string) ([]os.DirEntry, error) {
	certs := generatedCerts()
	prefix := name + "/"
	var entries []os.DirEntry

	for path, data := range certs {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		remainder := strings.TrimPrefix(path, prefix)
		// Only include direct children, not nested paths.
		if strings.Contains(remainder, "/") {
			continue
		}
		entries = append(entries, &dirEntry{
			info: &fileInfo{name: remainder, size: int64(len(data))},
		})
	}

	if len(entries) == 0 {
		return nil, errors.Wrapf(os.ErrNotExist,
			"securitytest: directory not found: %s", name)
	}

	// Sort alphabetically for deterministic ordering (matches embed.FS behavior).
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries, nil
}

// AssetStat returns file info for the named asset.
func AssetStat(name string) (os.FileInfo, error) {
	certs := generatedCerts()
	if data, ok := certs[name]; ok {
		return &fileInfo{
			name: filepath.Base(name),
			size: int64(len(data)),
		}, nil
	}
	// Check if this is a directory by looking for entries with this prefix.
	prefix := name + "/"
	for path := range certs {
		if strings.HasPrefix(path, prefix) {
			return &fileInfo{
				name: filepath.Base(name),
				dir:  true,
			}, nil
		}
	}
	return nil, errors.Wrapf(os.ErrNotExist,
		"securitytest: asset not found: %s", name)
}

// Asset returns the PEM-encoded bytes for the named certificate or key.
func Asset(name string) ([]byte, error) {
	if data, ok := generatedCerts()[name]; ok {
		return data, nil
	}
	return nil, errors.Wrapf(os.ErrNotExist,
		"securitytest: asset not found: %s", name)
}

// EmbeddedAssets is an AssetLoader backed by runtime-generated certificates.
var EmbeddedAssets = securityassets.Loader{
	ReadDir:  AssetReadDir,
	ReadFile: Asset,
	Stat:     AssetStat,
}
