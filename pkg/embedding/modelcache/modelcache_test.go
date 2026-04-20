// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package modelcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnsureFile_CacheHit(t *testing.T) {
	cacheDir := t.TempDir()
	filePath := filepath.Join(cacheDir, "test.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("cached content"), 0644))

	// Should return immediately without making any HTTP request.
	mf := modelFile{Name: "test.txt", URL: "http://should-not-be-called", SHA256: "bogus"}
	path, err := ensureFile(context.Background(), cacheDir, mf)
	require.NoError(t, err)
	require.Equal(t, filePath, path)
}

func TestEnsureFile_Download(t *testing.T) {
	content := []byte("test model data for download verification")
	hash := sha256.Sum256(content)
	expectedSHA := hex.EncodeToString(hash[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	cacheDir := t.TempDir()
	mf := modelFile{Name: "model.bin", URL: srv.URL + "/model.bin", SHA256: expectedSHA}
	path, err := ensureFile(context.Background(), cacheDir, mf)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(cacheDir, "model.bin"), path)

	// Verify the file was written correctly.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)

	// Verify no temp file left behind.
	_, err = os.Stat(path + ".download")
	require.True(t, os.IsNotExist(err))
}

func TestEnsureFile_ChecksumMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("wrong content"))
	}))
	defer srv.Close()

	cacheDir := t.TempDir()
	mf := modelFile{
		Name:   "model.bin",
		URL:    srv.URL + "/model.bin",
		SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
	}
	_, err := ensureFile(context.Background(), cacheDir, mf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHA256 mismatch")

	// Verify no file was left behind (neither final nor temp).
	_, err = os.Stat(filepath.Join(cacheDir, "model.bin"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(cacheDir, "model.bin.download"))
	require.True(t, os.IsNotExist(err))
}

func TestEnsureFile_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	cacheDir := t.TempDir()
	mf := modelFile{Name: "model.bin", URL: srv.URL + "/model.bin", SHA256: "irrelevant"}
	_, err := ensureFile(context.Background(), cacheDir, mf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 404")
}

func TestEnsureModel_CreatesCacheDir(t *testing.T) {
	content := []byte("test content")
	hash := sha256.Sum256(content)
	expectedSHA := hex.EncodeToString(hash[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	// Override defaultModel for the test.
	origModel := defaultModel
	defaultModel = []modelFile{
		{Name: "model.onnx", URL: srv.URL + "/model.onnx", SHA256: expectedSHA},
		{Name: "vocab.txt", URL: srv.URL + "/vocab.txt", SHA256: expectedSHA},
	}
	defer func() { defaultModel = origModel }()

	cacheDir := filepath.Join(t.TempDir(), "nested", "cache")
	result, err := EnsureModel(context.Background(), cacheDir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(cacheDir, "model.onnx"), result.ModelPath)
	require.Equal(t, filepath.Join(cacheDir, "vocab.txt"), result.VocabPath)

	// Verify the directory was created.
	info, err := os.Stat(cacheDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestFormatBytes(t *testing.T) {
	require.Equal(t, "0 B", formatBytes(0))
	require.Equal(t, "512 B", formatBytes(512))
	require.Equal(t, "1.0 KB", formatBytes(1024))
	require.Equal(t, "1.5 KB", formatBytes(1536))
	require.Equal(t, "1.0 MB", formatBytes(1024*1024))
	require.Equal(t, "90.3 MB", formatBytes(94720000))
}
