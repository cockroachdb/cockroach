// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCSBackend_GenerateTF(t *testing.T) {
	b := NewGCSBackend(nil, "my-bucket", "/path/to/sa-key.json")
	content, err := b.GenerateTF(context.Background(), "provisionings/abc12345")
	require.NoError(t, err)
	assert.Contains(t, content, `backend "gcs"`)
	assert.Contains(t, content, `bucket = "my-bucket"`)
	assert.Contains(t, content, `prefix = "provisionings/abc12345"`)
	assert.NotContains(t, content, "access_token",
		"access_token should not be embedded in the backend config")
}

func TestGCSBackend_EnvOverrides(t *testing.T) {
	b := NewGCSBackend(nil, "my-bucket", "/path/to/sa-key.json")
	overrides := b.EnvOverrides()
	require.NotNil(t, overrides)
	assert.Equal(t, "/path/to/sa-key.json",
		overrides["GOOGLE_BACKEND_CREDENTIALS"])
}

func TestLocalBackend_GenerateTF(t *testing.T) {
	b := NewLocalBackend()
	content, err := b.GenerateTF(context.Background(), "")
	require.NoError(t, err)
	assert.Contains(t, content, `backend "local"`)
	assert.Contains(t, content, `path = "terraform.tfstate"`)
}

func TestLocalBackend_EnvOverrides(t *testing.T) {
	b := NewLocalBackend()
	overrides := b.EnvOverrides()
	assert.Nil(t, overrides)
}

func TestLocalBackend_CleanupState(t *testing.T) {
	b := NewLocalBackend()
	err := b.CleanupState(context.Background(), logger.DefaultLogger, "provisioning-test")
	require.NoError(t, err)
}
