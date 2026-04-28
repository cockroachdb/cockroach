// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
)

// DLQWriter persists failed GitHub issue post requests for later replay.
type DLQWriter interface {
	Add(ctx context.Context, entry *DLQEntry) error
}

// GCSDLQWriter writes DLQ entries to a GCS bucket.
type GCSDLQWriter struct {
	bucket *storage.BucketHandle
}

// NewGCSDLQWriter creates a DLQWriter backed by the given GCS bucket.
// Uses Application Default Credentials for authentication.
func NewGCSDLQWriter(ctx context.Context, bucket string) (*GCSDLQWriter, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "creating GCS client")
	}
	return &GCSDLQWriter{bucket: client.Bucket(bucket)}, nil
}

func (w *GCSDLQWriter) Add(ctx context.Context, entry *DLQEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return errors.Wrap(err, "marshaling DLQ entry")
	}
	key := ObjectKey(entry)
	obj := w.bucket.Object(key)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return errors.Wrap(err, "writing DLQ entry to GCS")
	}
	return errors.Wrap(writer.Close(), "closing GCS writer")
}
