// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// mockBlobUploader records calls for testing without GCS.
type mockBlobUploader struct {
	uploadedPath string
	content      []byte
	closed       bool
	uploadErr    error
	uploadCount  int
}

func (m *mockBlobUploader) Upload(_ context.Context, srcPath string) (string, error) {
	m.uploadCount++
	if m.uploadErr != nil {
		return "", m.uploadErr
	}
	m.uploadedPath = srcPath
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return "", err
	}
	m.content = data
	return "gs://test-bucket/test-prefix/" + filepath.Base(srcPath), nil
}

func (m *mockBlobUploader) Close() error {
	m.closed = true
	return nil
}

// TestNewBlobUploaderUnsupportedProvider verifies that newBlobUploader
// returns an error for unsupported storage providers.
func TestNewBlobUploaderUnsupportedProvider(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, err := newBlobUploader(
		context.Background(), "s3", "bucket", "prefix", "token",
	)
	require.ErrorContains(t, err, "unsupported storage provider")
}
