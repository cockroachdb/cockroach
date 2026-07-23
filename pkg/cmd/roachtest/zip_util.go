// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"archive/zip"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// moveToZipArchive creates a zip archive inside rootPath with the files/dirs
// given as relative paths, after which the given files/dirs are deleted.
//
// See filterDirEntries for a convenient way to get a list of files/dirs.
func moveToZipArchive(archiveName string, rootPath string, relPaths ...string) error {
	f, err := os.Create(filepath.Join(rootPath, archiveName))
	if err != nil {
		return err
	}

	z := zip.NewWriter(f)
	for _, relPath := range relPaths {
		// Walk the given path.
		if err := filepath.WalkDir(filepath.Join(rootPath, relPath), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				// Let Walk recurse inside.
				return nil
			}
			relPath, err := filepath.Rel(rootPath, path)
			if err != nil {
				return err
			}
			w, err := z.Create(relPath)
			if err != nil {
				return err
			}
			r, err := os.Open(path)
			if err != nil {
				return err
			}
			if _, err := io.Copy(w, r); err != nil {
				_ = r.Close()
				return err
			}
			return r.Close()
		}); err != nil {
			_ = f.Close()
			return err
		}
	}

	if err := z.Close(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Now that the zip file is there, remove all the files that went into it.
	for _, relPath := range relPaths {
		if err := os.RemoveAll(filepath.Join(rootPath, relPath)); err != nil {
			return err
		}
	}
	return nil
}

// filterDirEntries lists the given directory, runs a filter function on each
// entry, and returns the base names of those which passed the filter.
func filterDirEntries(
	path string, filter func(entry os.DirEntry) bool,
) (baseNames []string, _ error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if filter(e) {
			baseNames = append(baseNames, e.Name())
		}
	}
	sort.Strings(baseNames)
	return baseNames, nil
}
