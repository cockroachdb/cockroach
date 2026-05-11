// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	artifactsZipName = "artifacts.zip"

	defaultRoachtestArtifactFailoverBucket = "roachtest-artifact-failover"

	roachtestArtifactFailoverBucketEnv   = "ROACHTEST_ARTIFACT_FAILOVER_BUCKET"
	roachtestArtifactFailoverMaxBytesEnv = "ROACHTEST_ARTIFACT_FAILOVER_MAX_BYTES"

	teamCityMaxArtifactFileSizeBytes int64 = 7_000_000_000

	roachtestArtifactFailoverUploadTimeout = 2 * time.Hour
)

type artifactFailoverResult struct {
	sizeBytes int64
	gcsURI    string
}

type artifactUploader func(ctx context.Context, bucket, object, filePath string) error

func artifactsZipSize(t *testImpl) (int64, error) {
	info, err := os.Stat(artifactsZipPath(t))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func artifactsZipPath(t *testImpl) string {
	return filepath.Join(t.ArtifactsDir(), artifactsZipName)
}

func roachtestArtifactFailoverBucket() string {
	if bucket := os.Getenv(roachtestArtifactFailoverBucketEnv); bucket != "" {
		return bucket
	}
	return defaultRoachtestArtifactFailoverBucket
}

func roachtestArtifactFailoverMaxBytes() (int64, error) {
	value := os.Getenv(roachtestArtifactFailoverMaxBytesEnv)
	if value == "" {
		return teamCityMaxArtifactFileSizeBytes, nil
	}

	maxBytes, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return teamCityMaxArtifactFileSizeBytes, errors.Wrapf(err, "parsing %s", roachtestArtifactFailoverMaxBytesEnv)
	}
	if maxBytes <= 0 {
		return teamCityMaxArtifactFileSizeBytes, errors.Errorf(
			"%s must be positive, got %d", roachtestArtifactFailoverMaxBytesEnv, maxBytes,
		)
	}
	return maxBytes, nil
}

// failoverOversizedArtifactsZip uploads artifacts.zip to GCS, then writes a
// small pointer file for TeamCity artifact publishing.
func failoverOversizedArtifactsZip(
	ctx context.Context, t *testImpl, sizeBytes int64,
) (artifactFailoverResult, error) {
	return failoverOversizedArtifactsZipWithUploader(
		ctx, t, sizeBytes, uploadFileToGCS,
	)
}

func failoverOversizedArtifactsZipWithUploader(
	ctx context.Context, t *testImpl, sizeBytes int64, upload artifactUploader,
) (artifactFailoverResult, error) {
	bucket := roachtestArtifactFailoverBucket()
	object := roachtestArtifactFailoverObjectName(t)
	result := artifactFailoverResult{
		sizeBytes: sizeBytes,
		gcsURI:    fmt.Sprintf("gs://%s/%s", bucket, object),
	}

	uploadCtx, cancel := context.WithTimeout(ctx, roachtestArtifactFailoverUploadTimeout)
	defer cancel()
	if err := upload(uploadCtx, bucket, object, artifactsZipPath(t)); err != nil {
		return result, err
	}

	pointerPath := filepath.Join(t.ArtifactsDir(), "artifacts-failover.txt")
	if err := os.WriteFile(pointerPath, []byte(artifactFailoverPointer(result)), 0644); err != nil {
		return result, errors.Wrap(err, "writing artifact failover pointer")
	}

	return result, nil
}

// roachtestArtifactFailoverObjectName returns a stable, build-scoped object
// name, e.g. teamcity/12345/kv/restart/nodes=12/run_1/artifacts.zip. Object
// names are unique across TeamCity builds because they include TC_BUILD_ID, and
// unique within a build because they include the test name and run_N suffix.
func roachtestArtifactFailoverObjectName(t *testImpl) string {
	buildID := os.Getenv("TC_BUILD_ID")
	if buildID == "" {
		buildID = "unknown-build"
	}
	return path.Join(
		"teamcity",
		buildID,
		filepath.ToSlash(t.Name()),
		filepath.Base(t.ArtifactsDir()),
		artifactsZipName,
	)
}

func artifactFailoverPointer(result artifactFailoverResult) string {
	return fmt.Sprintf(`artifacts.zip exceeded TeamCity's per-file artifact limit and was uploaded to GCS.

GCS URI: %s
Size: %d bytes
Uploaded at: %s
`,
		result.gcsURI,
		result.sizeBytes,
		timeutil.Now().UTC().Format(time.RFC3339),
	)
}

func uploadFileToGCS(ctx context.Context, bucket, object, filePath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	zipFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() { _ = zipFile.Close() }()

	gcsObjectWriter := client.Bucket(bucket).Object(object).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	gcsObjectWriter.ContentType = "application/zip"
	if _, err := io.Copy(gcsObjectWriter, zipFile); err != nil {
		_ = gcsObjectWriter.Close()
		return err
	}
	return gcsObjectWriter.Close()
}
