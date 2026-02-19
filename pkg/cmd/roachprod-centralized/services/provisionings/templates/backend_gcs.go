// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
)

// GCSBackend implements Backend using Google Cloud Storage for terraform
// state. It generates a GCS backend.tf block and can clean up state objects
// after provisioning destroy.
type GCSBackend struct {
	bucket string
	client *storage.Client
}

// NewGCSBackend creates a GCS backend. The client must be non-nil.
func NewGCSBackend(client *storage.Client, bucket string) *GCSBackend {
	return &GCSBackend{
		bucket: bucket,
		client: client,
	}
}

// GenerateTF returns a backend.tf configured for GCS state storage.
func (b *GCSBackend) GenerateTF(prefix string) string {
	return fmt.Sprintf(`terraform {
  backend "gcs" {
    bucket = %q
    prefix = %q
  }
}
`, b.bucket, prefix)
}

// EnvVars returns the GOOGLE_BACKEND_CREDENTIALS env var if
// GOOGLE_APPLICATION_CREDENTIALS is set. The GCS backend uses this dedicated
// env var for backend authentication, taking precedence over the provider-level
// GOOGLE_APPLICATION_CREDENTIALS. This ensures the state backend authenticates
// with the app's service account even when an environment overrides
// GOOGLE_APPLICATION_CREDENTIALS for the GCP provider.
func (b *GCSBackend) EnvVars() map[string]string {
	appCreds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if appCreds == "" {
		return nil
	}
	return map[string]string{
		"GOOGLE_BACKEND_CREDENTIALS": appCreds,
	}
}

// CleanupState removes all objects under the given prefix from the GCS bucket.
// Best-effort: logs warnings for individual object deletion failures but
// returns the first listing error.
func (b *GCSBackend) CleanupState(
	ctx context.Context, l *logger.Logger, prefix string,
) error {
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
