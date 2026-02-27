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
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
)

// GCSBackend implements Backend using Google Cloud Storage for terraform
// state. It generates a GCS backend.tf block with an embedded access token
// and can clean up state objects after provisioning destroy.
//
// The access token is fetched from the application's default credentials
// (e.g. Cloud Run service account) on each GenerateTF call. This decouples
// backend authentication from the environment-specific credentials that
// OpenTofu uses for provider operations (via GOOGLE_APPLICATION_CREDENTIALS).
type GCSBackend struct {
	bucket string
	client *storage.Client

	// tokenSource provides OAuth2 tokens for the GCS backend. When nil,
	// GenerateTF creates a fresh google.DefaultTokenSource on each call to
	// avoid returning nearly-expired cached tokens (the oauth2 library's
	// ReuseTokenSource uses a 3m45s expiry delta). When set (e.g. in
	// tests), it is used directly.
	tokenSource oauth2.TokenSource
}

// NewGCSBackend creates a GCS backend. The client must be non-nil.
func NewGCSBackend(client *storage.Client, bucket string) *GCSBackend {
	return &GCSBackend{
		bucket: bucket,
		client: client,
	}
}

// GenerateTF returns a backend.tf configured for GCS state storage. It
// fetches an access token from the application's default credentials and
// embeds it in the backend block so that OpenTofu authenticates to the GCS
// state bucket independently of environment credentials.
func (b *GCSBackend) GenerateTF(ctx context.Context, prefix string) (string, error) {
	// In production tokenSource is nil: we create a fresh DefaultTokenSource
	// on every call so the returned token always has ~60 min of validity.
	// Reusing a cached TokenSource would risk returning a nearly-expired
	// token due to the oauth2 library's 3m45s refresh delta. Tests inject
	// a static tokenSource to avoid hitting the metadata server.
	ts := b.tokenSource
	if ts == nil {
		var err error
		ts, err = google.DefaultTokenSource(ctx, storage.ScopeReadWrite)
		if err != nil {
			return "", errors.Wrap(err, "create token source for GCS backend")
		}
	}
	token, err := ts.Token()
	if err != nil {
		return "", errors.Wrap(err, "fetch access token for GCS backend")
	}
	return fmt.Sprintf(`terraform {
  backend "gcs" {
    bucket       = %q
    prefix       = %q
    access_token = %q
  }
}
`, b.bucket, prefix, token.AccessToken), nil
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
