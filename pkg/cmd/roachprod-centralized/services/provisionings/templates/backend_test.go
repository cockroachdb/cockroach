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
	"golang.org/x/oauth2"
)

func TestGCSBackend_GenerateTF(t *testing.T) {
	b := NewGCSBackend(nil, "my-bucket")
	b.tokenSource = staticTokenSource("test-token-abc")
	content, err := b.GenerateTF(context.Background(), "provisionings/abc12345")
	require.NoError(t, err)
	assert.Contains(t, content, `backend "gcs"`)
	assert.Contains(t, content, `bucket       = "my-bucket"`)
	assert.Contains(t, content, `prefix       = "provisionings/abc12345"`)
	assert.Contains(t, content, `access_token = "test-token-abc"`)
}

func TestLocalBackend_GenerateTF(t *testing.T) {
	b := NewLocalBackend()
	content, err := b.GenerateTF(context.Background(), "")
	require.NoError(t, err)
	assert.Contains(t, content, `backend "local"`)
	assert.Contains(t, content, `path = "terraform.tfstate"`)
}

func TestLocalBackend_CleanupState(t *testing.T) {
	b := NewLocalBackend()
	err := b.CleanupState(context.Background(), logger.DefaultLogger, "provisioning-test")
	require.NoError(t, err)
}

// staticTokenSource returns an oauth2.TokenSource that always returns
// a token with the given access token string.
func staticTokenSource(accessToken string) *staticTS {
	return &staticTS{accessToken: accessToken}
}

type staticTS struct {
	accessToken string
}

func (s *staticTS) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: s.accessToken}, nil
}
