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
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	artifactsZipName           = "artifacts.zip"
	artifactFailoverMarkerName = "artifacts-failover.txt"

	defaultRoachtestArtifactFailoverBucket = "roachtest-artifact-failover"

	// Optional env vars for setting failover limit and GCS bucket for custom
	// failover behavior and testing.
	roachtestArtifactFailoverBucketEnv   = "ROACHTEST_ARTIFACT_FAILOVER_BUCKET"
	roachtestArtifactFailoverMaxBytesEnv = "ROACHTEST_ARTIFACT_FAILOVER_MAX_BYTES"

	teamCityMaxArtifactFileSizeBytes int64 = 7_000_000_000

	roachtestArtifactFailoverUploadTimeout = 2 * time.Hour
)

// artifactFailoverResult contains the metadata needed to log the failover and
// generate a marker for an oversized artifacts.zip uploaded to GCS.
type artifactFailoverResult struct {
	sizeBytes int64
	gcsURI    string
}

// artifactUploader allows tests to stub the GCS upload.
type artifactUploader func(ctx context.Context, bucket, object, filePath string) error

// publishTeamCityArtifactsWithFailover publishes this test's artifacts to
// TeamCity. If artifacts.zip's size is at or above the failover threshold, the
// archive is uploaded to GCS instead, and the local artifacts.zip is replaced
// with a small marker archive that contains the artifact's GCS URI.
func publishTeamCityArtifactsWithFailover(
	ctx context.Context, t *testImpl, l *logger.Logger, stdout io.Writer,
) {
	publishTeamCityArtifacts := func() {
		// Tell TeamCity to collect this test's artifacts now. The TC job
		// also collects the artifacts directory wholesale at the end, but
		// here we make sure that the artifacts for any test that has already
		// finished are available in the UI even before the job as a whole
		// has completed. We're using the exact same destination to avoid
		// duplication of any of the artifacts.
		shout(ctx, l, stdout, "##teamcity[publishArtifacts '%s']", t.artifactsSpec)
	}

	artifactsZipSizeBytes, err := artifactsZipSize(t)
	if err != nil {
		shout(ctx, l, stdout, "unable to check roachtest artifacts for failover: %s", err)
	}
	artifactFailoverMaxBytes, maxBytesErr := roachtestArtifactFailoverMaxBytes()
	if maxBytesErr != nil {
		shout(ctx, l, stdout,
			"unable to parse roachtest artifact failover max bytes; using default %d bytes: %s",
			teamCityMaxArtifactFileSizeBytes, maxBytesErr)
	}
	if err != nil || artifactsZipSizeBytes < artifactFailoverMaxBytes {
		// Publish the artifacts directory without failover.
		publishTeamCityArtifacts()
		return
	}

	shout(ctx, l, stdout,
		"roachtest artifacts failover uploading oversized %s (%d bytes) to GCS bucket %s",
		artifactsZipName, artifactsZipSizeBytes, roachtestArtifactFailoverBucket())
	uploadStart := timeutil.Now()
	failover, err := failoverOversizedArtifactsZip(ctx, t, artifactsZipSizeBytes)
	uploadDuration := timeutil.Since(uploadStart)
	if err != nil {
		shout(ctx, l, stdout,
			"roachtest artifacts failover failed after %.2fs for oversized %s (%d bytes): %s; "+
				"skipping TeamCity artifact publish for %s to avoid exceeding the TeamCity artifact file size limit",
			uploadDuration.Seconds(), artifactsZipName, failover.sizeBytes, err, t.artifactsSpec)
		return
	}
	if err := replaceArtifactsZipWithFailoverMarker(t, failover); err != nil {
		shout(ctx, l, stdout,
			"roachtest artifacts failover uploaded oversized %s (%d bytes) to %s in %.2fs, but failed to replace the local zip with a marker archive: %s; "+
				"skipping TeamCity artifact publish for %s to avoid exceeding the TeamCity artifact file size limit",
			artifactsZipName, failover.sizeBytes, failover.gcsURI, uploadDuration.Seconds(), err, t.artifactsSpec)
		return
	}

	shout(ctx, l, stdout,
		"roachtest artifacts failover uploaded oversized %s (%d bytes) to %s in %.2fs",
		artifactsZipName, failover.sizeBytes, failover.gcsURI, uploadDuration.Seconds())
	// Publish the artifacts directory with a small artifacts.zip marker
	// archive pointing to the full archive in GCS.
	publishTeamCityArtifacts()
}

// artifactsZipSize returns the size of artifacts.zip in the test's artifact
// directory. A missing artifacts.zip is returned as an error.
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

// roachtestArtifactFailoverBucket returns the GCS bucket used for oversized
// artifact uploads, allowing an environment override.
func roachtestArtifactFailoverBucket() string {
	if bucket := os.Getenv(roachtestArtifactFailoverBucketEnv); bucket != "" {
		return bucket
	}
	return defaultRoachtestArtifactFailoverBucket
}

// roachtestArtifactFailoverMaxBytes returns the artifacts.zip size threshold
// at which GCS failover is used, allowing an environment override.
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

// failoverOversizedArtifactsZip uploads artifacts.zip to GCS using the
// default GCS uploader. Tests call failoverOversizedArtifactsZipWithUploader
// to stub the upload.
func failoverOversizedArtifactsZip(
	ctx context.Context, t *testImpl, sizeBytes int64,
) (artifactFailoverResult, error) {
	return failoverOversizedArtifactsZipWithUploader(
		ctx, t, sizeBytes, uploadFileToGCS,
	)
}

// failoverOversizedArtifactsZipWithUploader uploads artifacts.zip to GCS using
// the supplied uploader.
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

	return result, nil
}

// replaceArtifactsZipWithFailoverMarker replaces the oversized local
// artifacts.zip with a small artifacts.zip containing a marker file that
// points to the GCS upload.
func replaceArtifactsZipWithFailoverMarker(t *testImpl, result artifactFailoverResult) error {
	if err := os.Remove(artifactsZipPath(t)); err != nil {
		return errors.Wrap(err, "removing oversized local artifacts.zip after GCS upload")
	}

	markerPath := filepath.Join(t.ArtifactsDir(), artifactFailoverMarkerName)
	if err := os.WriteFile(markerPath, []byte(artifactFailoverMarker(result)), 0644); err != nil {
		return errors.Wrap(err, "writing artifact failover marker")
	}
	if err := moveToZipArchive(artifactsZipName, t.ArtifactsDir(), artifactFailoverMarkerName); err != nil {
		return errors.Wrap(err, "zipping artifact failover marker")
	}
	return nil
}

// artifactFailoverMarker generates the marker file contents from
// artifactFailoverResult's metadata.
func artifactFailoverMarker(result artifactFailoverResult) string {
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

// uploadFileToGCS uploads filePath to gs://bucket/object as a zip object,
// failing if the object already exists.
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
