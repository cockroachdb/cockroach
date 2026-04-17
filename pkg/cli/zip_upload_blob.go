// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

// blobUploader abstracts uploading a file to blob storage.
// Implementations exist for GCS (via the cloud package) and can be
// extended to S3 or other providers in the future.
type blobUploader interface {
	// Upload uploads the file at srcPath to blob storage. The
	// destination path is determined by the prefix configured at
	// creation time. It returns the full destination path.
	Upload(ctx context.Context, srcPath string) (destPath string, err error)

	// Close releases any resources held by the uploader.
	Close() error
}

// gcsBlobUploader uploads files to GCS using CockroachDB's cloud storage
// infrastructure. It constructs a GCS URI with an embedded bearer token
// and uses cloud.EarlyBootExternalStorageFromURI to create the storage
// handle.
type gcsBlobUploader struct {
	bucket     string
	pathPrefix string
	token      string
	storage    cloud.ExternalStorage
}

func newGCSBlobUploader(
	ctx context.Context, bucket, pathPrefix, bearerToken string,
) (*gcsBlobUploader, error) {
	uri := fmt.Sprintf(
		"gs://%s/%s?AUTH=specified&BEARER_TOKEN=%s",
		bucket, pathPrefix, bearerToken,
	)

	es, err := cloud.EarlyBootExternalStorageFromURI(
		ctx, uri,
		base.ExternalIODirConfig{},
		cluster.MakeClusterSettings(),
		nil, // limiters
		cloud.NilMetrics,
	)
	if err != nil {
		return nil, errors.Wrap(err, "creating GCS storage client")
	}

	return &gcsBlobUploader{
		bucket:     bucket,
		pathPrefix: pathPrefix,
		token:      bearerToken,
		storage:    es,
	}, nil
}

func (u *gcsBlobUploader) Upload(ctx context.Context, srcPath string) (string, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return "", errors.Wrap(err, "opening zip file")
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return "", errors.Wrap(err, "getting file info")
	}

	destName := filepath.Base(srcPath)

	w, err := u.storage.Writer(ctx, destName)
	if err != nil {
		return "", errors.Wrap(err, "creating storage writer")
	}

	pw := &progressWriter{
		w:     w,
		total: fi.Size(),
	}

	if _, err := io.Copy(pw, f); err != nil {
		_ = w.Close()
		return "", errors.Wrap(err, "uploading file")
	}

	if err := w.Close(); err != nil {
		return "", errors.Wrapf(err,
			"finalizing upload (credentials may have expired)")
	}

	destPath := fmt.Sprintf(
		"gs://%s/%s/%s", u.bucket, u.pathPrefix, destName,
	)
	return destPath, nil
}

func (u *gcsBlobUploader) Close() error {
	return u.storage.Close()
}

// newBlobUploader creates the appropriate blobUploader based on the
// storage provider. Currently only GCS is supported;
// Declared as a var to allow test injection.
var newBlobUploader = func(
	ctx context.Context, provider, bucket, prefix, token string,
) (blobUploader, error) {
	switch provider {
	case "gcs":
		return newGCSBlobUploader(ctx, bucket, prefix, token)
	default:
		return nil, errors.Newf("unsupported storage provider: %q", provider)
	}
}

// progressWriter wraps an io.Writer and prints upload progress to
// stderr on every write.
type progressWriter struct {
	w       io.Writer
	total   int64
	written int64
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	pw.written += int64(n)
	if err == nil && pw.total > 0 {
		pct := float64(pw.written) / float64(pw.total) * 100
		fmt.Fprintf(os.Stderr, "\rUploading... %s / %s (%.1f%%)",
			humanReadableSize(int(pw.written)),
			humanReadableSize(int(pw.total)),
			pct,
		)
		if pw.written == pw.total {
			fmt.Fprintln(os.Stderr)
		}
	}
	return n, err
}
