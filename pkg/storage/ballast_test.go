// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func temporarilyEnableBallasts() func() {
	prev := ballastsEnabled
	ballastsEnabled = true
	return func() { ballastsEnabled = prev }
}

func TestBallastSizeBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		base.StoreSpec
		totalBytes uint64
		want       int64
	}{
		{
			StoreSpec:  base.StoreSpec{},
			totalBytes: 500 << 30, // 500 GiB
			want:       1 << 30,   // 1 GiB
		},
		{
			StoreSpec:  base.StoreSpec{},
			totalBytes: 25 << 30,  // 25 GiB
			want:       256 << 20, // 256 MiB
		},
		{
			StoreSpec:  base.StoreSpec{BallastSize: &storagepb.SizeSpec{Capacity: 1 << 30 /* 1 GiB */}},
			totalBytes: 25 << 30, // 25 GiB
			want:       1 << 30,  // 1 GiB
		},
		{
			StoreSpec:  base.StoreSpec{BallastSize: &storagepb.SizeSpec{Percent: 20}},
			totalBytes: 25 << 30, // 25 GiB
			want:       5 << 30,  // 5 GiB
		},
		{
			StoreSpec:  base.StoreSpec{BallastSize: &storagepb.SizeSpec{Percent: 20}},
			totalBytes: 500 << 30, // 500 GiB
			want:       100 << 30, // 100 GiB
		},
	}

	for _, tc := range testCases {
		du := vfs.DiskUsage{TotalBytes: tc.totalBytes}
		got := BallastSizeBytes(tc.StoreSpec, du)
		require.Equal(t, tc.want, got)
	}

}

func TestIsDiskFull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer temporarilyEnableBallasts()()

	// TODO(jackson): This test could be adapted to use a MemFS if we add
	// Truncate to vfs.FS.

	setup := func(t *testing.T, spec *base.StoreSpec, ballastSize int64, du ...vfs.DiskUsage) (vfs.FS, func()) {
		dir, dirCleanupFn := testutils.TempDir(t)
		fs := mockDiskUsageFS{
			FS:         vfs.Default,
			diskUsages: du,
		}
		spec.Path = dir

		if ballastSize > 0 {
			path := base.EmergencyBallastFile(fs.PathJoin, spec.Path)
			require.NoError(t, fs.MkdirAll(fs.PathDir(path), 0755))
			err := sysutil.ResizeLargeFile(path, ballastSize)
			fmt.Printf("Created ballast at %s\n", path)
			require.NoError(t, err)
		}
		return &fs, dirCleanupFn
	}

	t.Run("default ballast, full disk", func(t *testing.T) {
		spec := base.StoreSpec{
			// NB: A missing ballast size defaults to Min(1GiB, 1% of
			// total) = 1GiB, which is greater than available bytes.
			BallastSize: nil,
		}
		fs, cleanup := setup(t, &spec, 0 /* ballastSize */, vfs.DiskUsage{
			AvailBytes: (1 << 28), // 256 MiB
			TotalBytes: 500 << 30, // 500 GiB
		})
		defer cleanup()
		got, err := IsDiskFull(fs, spec)
		require.NoError(t, err)
		require.True(t, got)
	})
	t.Run("default ballast, plenty of space", func(t *testing.T) {
		spec := base.StoreSpec{
			// NB: A missing ballast size defaults to Min(1GiB, 1% of
			// total) = 1GiB which is greater than available bytes.
			BallastSize: nil,
		}
		fs, cleanup := setup(t, &spec, 0 /* ballastSize */, vfs.DiskUsage{
			AvailBytes: 25 << 30,  // 25 GiB
			TotalBytes: 500 << 30, // 500 GiB
		})
		defer cleanup()
		got, err := IsDiskFull(fs, spec)
		require.NoError(t, err)
		require.False(t, got)
	})
	t.Run("truncating ballast frees enough space", func(t *testing.T) {
		spec := base.StoreSpec{
			BallastSize: &storagepb.SizeSpec{Capacity: 1024},
		}
		// Provide two disk usages. The second one will be returned
		// post-truncation.
		fs, cleanup := setup(t, &spec, 2048, /* ballastSize */
			vfs.DiskUsage{AvailBytes: 256, TotalBytes: 500 << 30 /* 500 GiB */},
			vfs.DiskUsage{AvailBytes: 1280, TotalBytes: 500 << 30 /* 500 GiB */})
		defer cleanup()

		got, err := IsDiskFull(fs, spec)
		require.NoError(t, err)
		require.False(t, got)
		// The ballast should've been truncated.
		fi, err := fs.Stat(base.EmergencyBallastFile(fs.PathJoin, spec.Path))
		require.NoError(t, err)
		require.Equal(t, int64(1024), fi.Size())
	})
	t.Run("configured ballast, plenty of space", func(t *testing.T) {
		spec := base.StoreSpec{
			BallastSize: &storagepb.SizeSpec{Capacity: 5 << 30 /* 5 GiB */},
		}
		fs, cleanup := setup(t, &spec, 0 /* ballastSize */, vfs.DiskUsage{
			AvailBytes: 25 << 30,  // 25 GiB
			TotalBytes: 500 << 30, // 500 GiB
		})
		defer cleanup()
		got, err := IsDiskFull(fs, spec)
		require.NoError(t, err)
		require.False(t, got)
	})
}

type mockDiskUsageFS struct {
	vfs.FS
	diskUsagesIdx int
	diskUsages    []vfs.DiskUsage
}

func (fs *mockDiskUsageFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	ret := fs.diskUsages[fs.diskUsagesIdx]
	if fs.diskUsagesIdx+1 < len(fs.diskUsages) {
		fs.diskUsagesIdx++
	}
	return ret, nil
}

func TestMaybeEstablishBallast(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer temporarilyEnableBallasts()()

	setup := func(t *testing.T, ballastSize int64) (string, func()) {
		dir, dirCleanupFn := testutils.TempDir(t)
		path := filepath.Join(dir, "ballast")
		if ballastSize > 0 {
			err := sysutil.ResizeLargeFile(path, ballastSize)
			require.NoError(t, err)
		}
		return path, dirCleanupFn
	}
	getSize := func(t *testing.T, path string) int {
		fi, err := vfs.Default.Stat(path)
		if oserror.IsNotExist(err) {
			return 0
		}
		require.NoError(t, err)
		return int(fi.Size())
	}

	t.Run("insufficient disk space, no ballast", func(t *testing.T) {
		path, cleanup := setup(t, 0)
		defer cleanup()
		resized, err := maybeEstablishBallast(vfs.Default, path, 1<<30 /* 1 GiB */, vfs.DiskUsage{
			AvailBytes: 3 << 30, /* 3 GiB */
		})
		require.NoError(t, err)
		require.False(t, resized)
		require.Equal(t, 0, getSize(t, path))
	})
	t.Run("sufficient disk space, no ballast", func(t *testing.T) {
		path, cleanup := setup(t, 0)
		defer cleanup()
		resized, err := maybeEstablishBallast(vfs.Default, path, 1024, vfs.DiskUsage{
			AvailBytes: 500 << 20, /* 500 MiB */
		})
		require.NoError(t, err)
		require.True(t, resized)
		require.Equal(t, 1024, getSize(t, path))
	})
	t.Run("truncates ballast if necessary", func(t *testing.T) {
		path, cleanup := setup(t, 2048)
		defer cleanup()
		resized, err := maybeEstablishBallast(vfs.Default, path, 1024, vfs.DiskUsage{
			AvailBytes: 500 << 20, /* 500 MiB */
		})
		require.NoError(t, err)
		require.True(t, resized)
		require.Equal(t, 1024, getSize(t, path))
	})
	t.Run("does nothing if ballast is correct size", func(t *testing.T) {
		path, cleanup := setup(t, 4096)
		defer cleanup()
		resized, err := maybeEstablishBallast(vfs.Default, path, 4096, vfs.DiskUsage{
			AvailBytes: 500 << 20, /* 500 MiB */
		})
		require.NoError(t, err)
		require.False(t, resized)
		require.Equal(t, 4096, getSize(t, path))
	})
}
