// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geos

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInitGEOS(t *testing.T) {
	t.Run("test no initGEOS paths", func(t *testing.T) {
		_, _, err := initGEOS([]string{})
		require.Error(t, err)
		require.Regexp(t, "Ensure you have the spatial libraries installed as per the instructions in .*install-cockroachdb-", strings.Join(errors.GetAllHints(err), "\n"))
	})

	t.Run("test invalid initGEOS paths", func(t *testing.T) {
		_, _, err := initGEOS([]string{"/invalid/path"})
		require.Error(t, err)
		require.Regexp(t, "Ensure you have the spatial libraries installed as per the instructions in .*install-cockroachdb-", strings.Join(errors.GetAllHints(err), "\n"))
	})

	t.Run("test valid initGEOS paths", func(t *testing.T) {
		ret, loc, err := initGEOS(findLibraryDirectories("", ""))
		require.NoError(t, err)
		require.NotEmpty(t, loc)
		require.NotNil(t, ret)
	})
}

// TestFindLibraryDirectoriesInParentingDirectories checks the ancestor walk
// against the two layouts in which the libraries ship: next to the binary in
// lib/ (release archive) and under lib/cockroach/ (Homebrew keg).
func TestFindLibraryDirectoriesInParentingDirectories(t *testing.T) {
	// Resolve the temp dir up front: on macOS it lives under a symlinked
	// /var and the walk reports paths as given, not canonicalized.
	root := resolveSymlinks(t.TempDir())
	touch := func(elems ...string) string {
		p := filepath.Join(append([]string{root}, elems...)...)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
		require.NoError(t, os.WriteFile(p, nil, 0755))
		return p
	}
	libs := func(dir string) {
		touch(dir, getLibraryExt(libgeosFileName))
		touch(dir, getLibraryExt(libgeoscFileName))
	}

	// Release archive: cockroach-vX/cockroach next to cockroach-vX/lib/.
	archiveBinary := touch("archive", "cockroach")
	libs("archive/lib")
	// Homebrew keg: Cellar/cockroach/X/bin/cockroach, libraries under
	// Cellar/cockroach/X/lib/cockroach/, and bin/cockroach a symlink to the
	// binary.
	kegBinary := touch("Cellar", "cockroach", "1.0", "bin", "cockroach")
	libs("Cellar/cockroach/1.0/lib/cockroach")
	symlinkBinary := filepath.Join(root, "bin", "cockroach")
	require.NoError(t, os.MkdirAll(filepath.Dir(symlinkBinary), 0755))
	require.NoError(t, os.Symlink(kegBinary, symlinkBinary))
	// Only one of the two libraries present: must not be returned.
	incompleteBinary := touch("incomplete", "cockroach")
	touch("incomplete", "lib", getLibraryExt(libgeosFileName))

	tests := []struct {
		name         string
		binary       string
		expectedDirs []string
	}{
		{
			name:         "libraries next to the binary",
			binary:       archiveBinary,
			expectedDirs: []string{filepath.Join(root, "archive", "lib")},
		},
		{
			name:         "homebrew keg layout",
			binary:       kegBinary,
			expectedDirs: []string{filepath.Join(root, "Cellar", "cockroach", "1.0", "lib", "cockroach")},
		},
		{
			name:         "homebrew symlink resolves to the keg",
			binary:       resolveSymlinks(symlinkBinary),
			expectedDirs: []string{filepath.Join(root, "Cellar", "cockroach", "1.0", "lib", "cockroach")},
		},
		{
			name:         "incomplete library directory is skipped",
			binary:       incompleteBinary,
			expectedDirs: []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedDirs, findLibraryDirectoriesInParentingDirectories(tc.binary))
		})
	}
}

func TestEnsureInit(t *testing.T) {
	// Fetch at least once.
	_, err := ensureInit(EnsureInitErrorDisplayPublic, "", "")
	require.NoError(t, err)

	fakeErr := errors.Newf("contain path info do not display me")
	defer func() { geosOnce.err = nil }()

	geosOnce.err = fakeErr
	_, err = ensureInit(EnsureInitErrorDisplayPrivate, "", "")
	require.ErrorContains(t, err, fakeErr.Error())

	_, err = ensureInit(EnsureInitErrorDisplayPublic, "", "")
	require.Equal(t, errors.Newf("geos: this operation is not available").Error(), err.Error())
}

// TestClipByRectNaNInf is a regression test for issue #166976.
// GEOS hangs when ClipByRect is called with NaN or Inf bounding box
// coordinates.
func TestClipByRectNaNInf(t *testing.T) {
	ewkb := []byte("\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@")

	t.Run("rejects NaN coordinates", func(t *testing.T) {
		_, err := ClipByRect(ewkb, math.NaN(), 0, 5, 5)
		require.ErrorContains(t, err, "ClipByRect")
		require.ErrorContains(t, err, "must be a finite value")
	})

	t.Run("rejects Inf coordinates", func(t *testing.T) {
		_, err := ClipByRect(ewkb, math.Inf(1), 0, 5, 5)
		require.ErrorContains(t, err, "ClipByRect")
		require.ErrorContains(t, err, "must be a finite value")

	})

	t.Run("error is deterministic", func(t *testing.T) {
		_, err := ClipByRect(ewkb, math.Inf(1), math.Inf(1), math.Inf(1), math.Inf(1))
		require.ErrorContains(t, err, "ClipByRect")
		require.ErrorContains(t, err, "must be a finite value")
		require.ErrorContains(t, err, "xMax")
	})
}
