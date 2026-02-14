// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

// BackendConfig holds the configuration needed to generate a backend.tf file.
type BackendConfig struct {
	// Bucket is the GCS bucket name for terraform state storage.
	Bucket string
	// Prefix is the GCS prefix for this provisioning's state
	// (e.g., "provisioning-<id>").
	Prefix string
}

// GenerateBackendTF returns the content of a backend.tf file configured for
// GCS state storage.
func GenerateBackendTF(config BackendConfig) string {
	return fmt.Sprintf(`terraform {
  backend "gcs" {
    bucket = %q
    prefix = %q
  }
}
`, config.Bucket, config.Prefix)
}

// GenerateLocalBackendTF returns the content of a backend.tf file configured
// for local state storage. Used by the local-provision CLI command.
func GenerateLocalBackendTF() string {
	return `terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}
`
}

// GCSBackendEnvVars returns the environment variables needed by the GCS
// backend. The GCS backend supports GOOGLE_BACKEND_CREDENTIALS as a
// dedicated env var that takes precedence over GOOGLE_APPLICATION_CREDENTIALS
// for backend operations only. This ensures the GCS state backend
// authenticates with the app's service account even when an environment
// overrides GOOGLE_APPLICATION_CREDENTIALS for the GCP provider.
//
// appCredentials is typically os.Getenv("GOOGLE_APPLICATION_CREDENTIALS").
// Returns nil if appCredentials is empty.
func GCSBackendEnvVars(appCredentials string) map[string]string {
	if appCredentials == "" {
		return nil
	}
	return map[string]string{
		"GOOGLE_BACKEND_CREDENTIALS": appCredentials,
	}
}

// WriteBackendTF writes a backend.tf file into the specified working directory.
// If backend.tf already exists, it is overwritten.
func WriteBackendTF(workingDir string, content string) error {
	path := filepath.Join(workingDir, "backend.tf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return errors.Wrapf(err, "write backend.tf to %s", workingDir)
	}
	return nil
}
