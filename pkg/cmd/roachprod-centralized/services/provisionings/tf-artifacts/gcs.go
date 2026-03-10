// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package artifacts

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
)

// GCSStore stores provisioning artifacts in Google Cloud Storage.
type GCSStore struct {
	client *storage.Client
	bucket string
	prefix string
}

// NewGCSStore creates a new GCS artifact store.
func NewGCSStore(client *storage.Client, bucket, prefix string) *GCSStore {
	return &GCSStore{
		client: client,
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
	}
}

// Put writes an object and returns its gs:// ref.
func (s *GCSStore) Put(
	ctx context.Context, objectKey string, r io.Reader, contentType string,
) (string, error) {
	objectName := s.objectName(objectKey)
	w := s.client.Bucket(s.bucket).Object(objectName).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return "", errors.Wrap(err, "write GCS artifact")
	}
	if err := w.Close(); err != nil {
		return "", errors.Wrap(err, "close GCS artifact writer")
	}
	return fmt.Sprintf("gs://%s/%s", s.bucket, objectName), nil
}

// NewReader opens an existing artifact by ref.
func (s *GCSStore) NewReader(ctx context.Context, ref string) (io.ReadCloser, error) {
	bucket, objectName, err := parseGSRef(ref)
	if err != nil {
		return nil, err
	}
	rc, err := s.client.Bucket(bucket).Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "open GCS artifact reader")
	}
	return rc, nil
}

// Delete removes an artifact by ref.
func (s *GCSStore) Delete(ctx context.Context, ref string) error {
	bucket, objectName, err := parseGSRef(ref)
	if err != nil {
		return err
	}
	if err := s.client.Bucket(bucket).Object(objectName).Delete(ctx); err != nil {
		return errors.Wrap(err, "delete GCS artifact")
	}
	return nil
}

func (s *GCSStore) objectName(objectKey string) string {
	objectKey = strings.TrimPrefix(objectKey, "/")
	if s.prefix == "" {
		return objectKey
	}
	return path.Join(s.prefix, objectKey)
}

func parseGSRef(ref string) (bucket string, objectName string, _ error) {
	u, err := url.Parse(ref)
	if err != nil {
		return "", "", errors.Wrap(err, "parse artifact ref")
	}
	if u.Scheme != "gs" {
		return "", "", errors.Newf("unsupported artifact ref scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", "", errors.New("artifact ref bucket is empty")
	}
	objectName = strings.TrimPrefix(u.Path, "/")
	if objectName == "" {
		return "", "", errors.New("artifact ref object path is empty")
	}
	return u.Host, objectName, nil
}
