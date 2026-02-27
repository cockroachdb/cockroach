// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// Backend abstracts terraform state backend operations. The provisioning
// service uses a Backend to generate backend.tf configuration and clean up
// state after destroy.
//
// The GCS implementation fetches an access token from the application's
// default credentials (e.g. Cloud Run service account) and embeds it in the
// backend configuration. This decouples backend auth from the environment
// credentials (GOOGLE_APPLICATION_CREDENTIALS) that OpenTofu uses for
// provider operations.
type Backend interface {
	// GenerateTF returns the content of a backend.tf file for the given
	// state prefix (typically "provisioning-<uuid>"). Implementations that
	// require authentication (e.g. GCS) fetch credentials on each call.
	GenerateTF(ctx context.Context, prefix string) (string, error)

	// CleanupState removes all state objects for the given prefix.
	// Best-effort: callers should log warnings on error and continue.
	CleanupState(ctx context.Context, l *logger.Logger, prefix string) error
}
