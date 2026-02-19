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
type Backend interface {
	// GenerateTF returns the content of a backend.tf file for the given
	// state prefix (typically "provisioning-<uuid>").
	GenerateTF(prefix string) string

	// EnvVars returns environment variables required by the backend for
	// authentication. Returns nil if no extra env vars are needed.
	EnvVars() map[string]string

	// CleanupState removes all state objects for the given prefix.
	// Best-effort: callers should log warnings on error and continue.
	CleanupState(ctx context.Context, l *logger.Logger, prefix string) error
}
