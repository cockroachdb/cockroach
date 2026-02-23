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
	b := NewGCSBackend(nil, "my-bucket")
	content := b.GenerateTF("provisioning-abc12345")
	assert.Contains(t, content, `backend "gcs"`)
	assert.Contains(t, content, `bucket = "my-bucket"`)
	assert.Contains(t, content, `prefix = "provisioning-abc12345"`)
}

func TestLocalBackend_GenerateTF(t *testing.T) {
	b := NewLocalBackend()
	content := b.GenerateTF("")
	assert.Contains(t, content, `backend "local"`)
	assert.Contains(t, content, `path = "terraform.tfstate"`)
}

func TestLocalBackend_CleanupState(t *testing.T) {
	b := NewLocalBackend()
	err := b.CleanupState(context.Background(), logger.DefaultLogger, "provisioning-test")
	require.NoError(t, err)
}
