// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// LocalBackend implements Backend using local file-based terraform state.
// Used by the local-provision CLI command. State cleanup is a no-op since
// the working directory is managed by the caller.
type LocalBackend struct{}

// NewLocalBackend creates a local backend.
func NewLocalBackend() *LocalBackend {
	return &LocalBackend{}
}

// GenerateTF returns a backend.tf configured for local state storage.
func (b *LocalBackend) GenerateTF(_ string) string {
	return `terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}
`
}

// CleanupState is a no-op for local backends. The working directory
// (containing the state file) is managed by the caller.
func (b *LocalBackend) CleanupState(_ context.Context, _ *logger.Logger, _ string) error {
	return nil
}
