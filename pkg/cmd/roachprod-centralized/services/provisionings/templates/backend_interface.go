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
// service uses a Backend to generate backend.tf configuration, provide
// backend-specific environment variables, and clean up state after destroy.
//
// The GCS implementation provides backend credentials via EnvOverrides
// (GOOGLE_BACKEND_CREDENTIALS pointing to a SA key file). This decouples
// backend auth from the environment credentials
// (GOOGLE_APPLICATION_CREDENTIALS) that OpenTofu uses for provider
// operations, and supports automatic token refresh for long-running
// commands.
type Backend interface {
	// GenerateTF returns the content of a backend.tf file for the given
	// state prefix (typically "provisioning-<uuid>").
	GenerateTF(ctx context.Context, prefix string) (string, error)

	// EnvOverrides returns environment variables that the backend requires
	// for authentication. These are applied AFTER all user/environment
	// variables to prevent override by user-provided values. Returns nil
	// if no overrides are needed (e.g. local backend).
	EnvOverrides() map[string]string

	// CleanupState removes all state objects for the given prefix.
	// Best-effort: callers should log warnings on error and continue.
	CleanupState(ctx context.Context, l *logger.Logger, prefix string) error
}
