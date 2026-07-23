// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
)

func TestASHReportProfiler(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	dir := t.TempDir()

	p, err := NewASHReportProfiler(ctx, dir, st)
	require.NoError(t, err)

	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	t.Run("writeFile/success", func(t *testing.T) {
		ok := p.writeFile(ctx, now, "test_trigger", ".txt", func(f *os.File) error {
			_, err := f.WriteString("hello")
			return err
		})
		require.True(t, ok)

		path := filepath.Join(dir, "ash_report.2026-03-05T12_00_00.000.test_trigger.txt")
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "hello", string(data))
	})

	t.Run("writeFile/error cleans up partial file", func(t *testing.T) {
		ok := p.writeFile(ctx, now, "fail_trigger", ".txt", func(f *os.File) error {
			return os.ErrPermission
		})
		require.False(t, ok)

		path := filepath.Join(dir, "ash_report.2026-03-05T12_00_00.000.fail_trigger.txt")
		_, err := os.Stat(path)
		require.True(t, oserror.IsNotExist(err), "partial file should be cleaned up")
	})

	t.Run("CheckOwnsFile", func(t *testing.T) {
		cases := []struct {
			name string
			owns bool
		}{
			{"ash_report.2026-03-05T12_00_00.000.goroutine_dump.txt", true},
			{"ash_report.2026-03-05T12_00_00.000.cpu_profile.json", true},
			{"memprof.2026-03-05T12_00_00.000.1234", false},
			{"ash_report_something_else", false},
		}
		subDir := t.TempDir()
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				path := filepath.Join(subDir, c.name)
				require.NoError(t, os.WriteFile(path, nil, 0644))
				defer func() { _ = os.Remove(path) }()

				entries, err := os.ReadDir(subDir)
				require.NoError(t, err)
				var entry os.DirEntry
				for _, e := range entries {
					if e.Name() == c.name {
						entry = e
						break
					}
				}
				require.NotNil(t, entry)
				require.Equal(t, c.owns, p.CheckOwnsFile(ctx, entry))
			})
		}
	})

	t.Run("PreFilter", func(t *testing.T) {
		subDir := t.TempDir()
		fileNames := []string{
			"ash_report.2026-03-05T10_00_00.000.goroutine_dump.json",
			"ash_report.2026-03-05T10_00_00.000.goroutine_dump.txt",
			"ash_report.2026-03-05T11_00_00.000.cpu_profile.json",
			"ash_report.2026-03-05T11_00_00.000.cpu_profile.txt",
			"memprof.2026-03-05T12_00_00.000.1234", // not owned
		}
		for _, name := range fileNames {
			require.NoError(t, os.WriteFile(filepath.Join(subDir, name), nil, 0644))
		}

		entries, err := os.ReadDir(subDir)
		require.NoError(t, err)

		preserved, err := p.PreFilter(ctx, entries, func(fileName string) error {
			return nil
		})
		require.NoError(t, err)

		// The newest .txt (index 3) and newest .json (index 2) should be
		// preserved. The non-owned file (index 4) should not be in the map.
		require.True(t, preserved[3], "newest .txt should be preserved")
		require.True(t, preserved[2], "newest .json should be preserved")
		require.False(t, preserved[0], "older .json should not be preserved")
		require.False(t, preserved[1], "older .txt should not be preserved")
		require.False(t, preserved[4], "non-owned file should not be preserved")
	})

	t.Run("WriteReport/no global sampler", func(t *testing.T) {
		ok := p.WriteReport(ctx, "test")
		require.False(t, ok)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		ashFiles := 0
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ashReportPrefix) {
				ashFiles++
			}
		}
		// Only the file from the writeFile/success subtest should exist.
		require.Equal(t, 1, ashFiles)
	})
}
