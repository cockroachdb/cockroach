// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
)

func TestArtifactsZipSize(t *testing.T) {
	artifactsDir := t.TempDir()
	ti := &testImpl{
		artifactsDir: artifactsDir,
	}

	sizeBytes, err := artifactsZipSize(ti)
	require.True(t, oserror.IsNotExist(err))
	require.Zero(t, sizeBytes)

	require.NoError(t, os.WriteFile(filepath.Join(artifactsDir, artifactsZipName), []byte("0123456789"), 0644))
	sizeBytes, err = artifactsZipSize(ti)
	require.NoError(t, err)
	require.Equal(t, int64(10), sizeBytes)
}

func TestRoachtestArtifactFailoverBucket(t *testing.T) {
	require.Equal(t, defaultRoachtestArtifactFailoverBucket, roachtestArtifactFailoverBucket())

	t.Setenv(roachtestArtifactFailoverBucketEnv, "roachtest-artifact-failover-smoke")
	require.Equal(t, "roachtest-artifact-failover-smoke", roachtestArtifactFailoverBucket())
}

func TestRoachtestArtifactFailoverMaxBytes(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		maxBytes, err := roachtestArtifactFailoverMaxBytes()
		require.NoError(t, err)
		require.Equal(t, teamCityMaxArtifactFileSizeBytes, maxBytes)
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv(roachtestArtifactFailoverMaxBytesEnv, "123")
		maxBytes, err := roachtestArtifactFailoverMaxBytes()
		require.NoError(t, err)
		require.Equal(t, int64(123), maxBytes)
	})

	t.Run("invalid", func(t *testing.T) {
		t.Setenv(roachtestArtifactFailoverMaxBytesEnv, "not-bytes")
		maxBytes, err := roachtestArtifactFailoverMaxBytes()
		require.Error(t, err)
		require.Equal(t, teamCityMaxArtifactFileSizeBytes, maxBytes)
	})

	t.Run("non-positive", func(t *testing.T) {
		t.Setenv(roachtestArtifactFailoverMaxBytesEnv, "0")
		maxBytes, err := roachtestArtifactFailoverMaxBytes()
		require.Error(t, err)
		require.Equal(t, teamCityMaxArtifactFileSizeBytes, maxBytes)
	})
}

func TestFailoverOversizedArtifactsZip(t *testing.T) {
	t.Setenv("TC_BUILD_ID", "12345")

	artifactsDir := filepath.Join(t.TempDir(), "kv", "restart", "nodes=12", "run_1")
	require.NoError(t, os.MkdirAll(artifactsDir, 0755))
	zipPath := filepath.Join(artifactsDir, artifactsZipName)
	require.NoError(t, os.WriteFile(zipPath, []byte("0123456789"), 0644))

	ti := &testImpl{
		spec:         &registry.TestSpec{Name: "kv/restart/nodes=12"},
		artifactsDir: artifactsDir,
	}

	var uploaded bool
	start := time.Now()
	result, err := failoverOversizedArtifactsZipWithUploader(
		context.Background(), ti, 10, func(ctx context.Context, bucket, object, filePath string) error {
			uploaded = true
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, start.Add(roachtestArtifactFailoverUploadTimeout), deadline, time.Second)
			require.Equal(t, defaultRoachtestArtifactFailoverBucket, bucket)
			require.Equal(t, path.Join("teamcity", "12345", "kv/restart/nodes=12", "run_1", artifactsZipName), object)
			require.Equal(t, zipPath, filePath)
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, uploaded)
	require.Equal(t, int64(10), result.sizeBytes)
	require.Equal(t, "gs://roachtest-artifact-failover/teamcity/12345/kv/restart/nodes=12/run_1/artifacts.zip", result.gcsURI)

	require.FileExists(t, zipPath)
	require.NoFileExists(t, filepath.Join(artifactsDir, artifactFailoverMarkerName))

	require.NoError(t, replaceArtifactsZipWithFailoverMarker(ti, result))
	require.FileExists(t, zipPath)
	require.NoFileExists(t, filepath.Join(artifactsDir, artifactFailoverMarkerName))

	zipReader, err := zip.OpenReader(zipPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, zipReader.Close()) }()
	require.Len(t, zipReader.File, 1)
	require.Equal(t, artifactFailoverMarkerName, zipReader.File[0].Name)

	markerFile, err := zipReader.File[0].Open()
	require.NoError(t, err)
	defer func() { require.NoError(t, markerFile.Close()) }()
	marker, err := io.ReadAll(markerFile)
	require.NoError(t, err)

	require.Contains(t, string(marker), result.gcsURI)
	require.Contains(t, string(marker), "Size: 10 bytes")
	require.NotContains(t, string(marker), "TeamCity limit:")
}
