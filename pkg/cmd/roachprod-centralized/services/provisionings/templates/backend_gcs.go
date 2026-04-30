// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
)

// GCSBackend implements Backend using Google Cloud Storage for terraform
// state. Authentication is handled externally via the
// GOOGLE_BACKEND_CREDENTIALS environment variable (pointing to a SA key
// file), rather than embedding a short-lived access token in the HCL.
// This allows tofu commands that run longer than 60 minutes to refresh
// credentials automatically.
type GCSBackend struct {
	bucket    string
	client    *storage.Client
	saKeyPath string
}

// NewGCSBackend creates a GCS backend. The client must be non-nil. The
// saKeyPath must point to a valid SA key JSON file that grants access
// to the state bucket.
func NewGCSBackend(client *storage.Client, bucket, saKeyPath string) *GCSBackend {
	return &GCSBackend{
		bucket:    bucket,
		client:    client,
		saKeyPath: saKeyPath,
	}
}

// GenerateTF returns a backend.tf configured for GCS state storage.
// Authentication is handled externally via GOOGLE_BACKEND_CREDENTIALS
// (returned by EnvOverrides), so no credentials are embedded in the HCL.
func (b *GCSBackend) GenerateTF(_ context.Context, prefix string) (string, error) {
	return fmt.Sprintf(`terraform {
  backend "gcs" {
    bucket = %q
    prefix = %q
  }
}
`, b.bucket, prefix), nil
}

// EnvOverrides returns the GOOGLE_BACKEND_CREDENTIALS env var pointing
// to the SA key file. OpenTofu's GCS backend uses this to authenticate
// independently of GOOGLE_APPLICATION_CREDENTIALS (which is used by
// provider operations).
func (b *GCSBackend) EnvOverrides() map[string]string {
	return map[string]string{
		"GOOGLE_BACKEND_CREDENTIALS": b.saKeyPath,
	}
}

// CleanupState removes all objects under the given prefix from the GCS bucket.
// Best-effort: logs warnings for individual object deletion failures but
// returns the first listing error.
func (b *GCSBackend) CleanupState(ctx context.Context, l *logger.Logger, prefix string) error {
	bkt := b.client.Bucket(b.bucket)
	it := bkt.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return errors.Wrap(err, "list GCS state objects")
		}
		if err := bkt.Object(attrs.Name).Delete(ctx); err != nil {
			l.Warn("failed to delete GCS state object",
				slog.String("object", attrs.Name), slog.Any("error", err))
		}
	}
	return nil
}
