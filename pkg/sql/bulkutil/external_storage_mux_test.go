// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExternalStorageMux_splitURI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mux := &ExternalStorageMux{
		storeInstances: make(map[string]cloud.ExternalStorage),
	}

	t.Run("splits nodelocal URI correctly", func(t *testing.T) {
		prefix, path, err := mux.splitURI("nodelocal://1/import/123/file.sst")
		require.NoError(t, err)
		require.Equal(t, "nodelocal", prefix.Scheme)
		require.Equal(t, "1", prefix.Host)
		require.Equal(t, "", prefix.Path)
		require.Equal(t, "/import/123/file.sst", path)
	})

	t.Run("splits s3 URI correctly", func(t *testing.T) {
		prefix, path, err := mux.splitURI("s3://bucket/path/to/file.sst")
		require.NoError(t, err)
		require.Equal(t, "s3", prefix.Scheme)
		require.Equal(t, "bucket", prefix.Host)
		require.Equal(t, "", prefix.Path)
		require.Equal(t, "/path/to/file.sst", path)
	})

	t.Run("handles URI with query parameters", func(t *testing.T) {
		prefix, path, err := mux.splitURI("nodelocal://1/import/file.sst?param=value")
		require.NoError(t, err)
		require.Equal(t, "nodelocal", prefix.Scheme)
		require.Equal(t, "1", prefix.Host)
		require.Equal(t, "param=value", prefix.RawQuery)
		require.Equal(t, "/import/file.sst", path)
	})

	t.Run("handles empty path", func(t *testing.T) {
		prefix, path, err := mux.splitURI("nodelocal://1")
		require.NoError(t, err)
		require.Equal(t, "nodelocal", prefix.Scheme)
		require.Equal(t, "1", prefix.Host)
		require.Equal(t, "", path)
	})

	t.Run("handles root path", func(t *testing.T) {
		prefix, path, err := mux.splitURI("nodelocal://1/")
		require.NoError(t, err)
		require.Equal(t, "nodelocal", prefix.Scheme)
		require.Equal(t, "1", prefix.Host)
		require.Equal(t, "/", path)
	})

	t.Run("returns error for invalid URI", func(t *testing.T) {
		_, _, err := mux.splitURI("://invalid")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to parse external storage uri")
	})
}

// mockExternalStorage is a minimal mock that implements just Close for testing.
// We embed cloud.ExternalStorage to satisfy the interface, but only implement Close.
type mockExternalStorage struct {
	cloud.ExternalStorage // embed to get default nil implementations
	uri                   string
	closed                bool
}

func (m *mockExternalStorage) Close() error {
	m.closed = true
	return nil
}

func TestExternalStorageMux_StoreFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("returns correct StoreFile components", func(t *testing.T) {
		callCount := 0
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			callCount++
			return &mockExternalStorage{uri: uri}, nil
		}

		mux := NewExternalStorageMux(factory, username.RootUserName())
		defer func() {
			require.NoError(t, mux.Close())
		}()

		storeFile, err := mux.StoreFile(ctx, "nodelocal://1/import/123/file.sst")
		require.NoError(t, err)
		require.NotNil(t, storeFile.Store)
		require.Equal(t, "/import/123/file.sst", storeFile.FilePath)
		require.Equal(t, 1, callCount, "factory should be called once")
	})

	t.Run("caches storage instances by prefix", func(t *testing.T) {
		callCount := 0
		var createdURIs []string
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			callCount++
			createdURIs = append(createdURIs, uri)
			return &mockExternalStorage{uri: uri}, nil
		}

		mux := NewExternalStorageMux(factory, username.RootUserName())
		defer func() {
			require.NoError(t, mux.Close())
		}()

		// First file on node 1
		storeFile1, err := mux.StoreFile(ctx, "nodelocal://1/import/123/file1.sst")
		require.NoError(t, err)
		require.Equal(t, "/import/123/file1.sst", storeFile1.FilePath)

		// Second file on node 1 - should reuse cached instance
		storeFile2, err := mux.StoreFile(ctx, "nodelocal://1/import/456/file2.sst")
		require.NoError(t, err)
		require.Equal(t, "/import/456/file2.sst", storeFile2.FilePath)

		// Verify same storage instance is reused
		require.Same(t, storeFile1.Store, storeFile2.Store, "should reuse cached storage instance")
		require.Equal(t, 1, callCount, "factory should only be called once for same prefix")
		require.Equal(t, []string{"nodelocal://1"}, createdURIs)
	})

	t.Run("authenticated URIs should reuse cached instances", func(t *testing.T) {
		callCount := 0
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			callCount++
			return &mockExternalStorage{uri: uri}, nil
		}

		mux := NewExternalStorageMux(factory, username.RootUserName())
		defer func() {
			require.NoError(t, mux.Close())
		}()

		first, err := mux.StoreFile(ctx, "s3://user:password@bucket/path/to/file1.sst")
		require.NoError(t, err)

		second, err := mux.StoreFile(ctx, "s3://user:password@bucket/path/to/file2.sst")
		require.NoError(t, err)

		require.Equal(t, 1, callCount, "factory should only be called once for authenticated URIs")
		require.Same(t, first.Store, second.Store, "authenticated URIs should reuse the same storage instance")
	})

	t.Run("creates separate instances for different prefixes", func(t *testing.T) {
		var createdURIs []string
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			createdURIs = append(createdURIs, uri)
			return &mockExternalStorage{uri: uri}, nil
		}

		mux := NewExternalStorageMux(factory, username.RootUserName())
		defer func() {
			require.NoError(t, mux.Close())
		}()

		// Files on different nodes
		storeFile1, err := mux.StoreFile(ctx, "nodelocal://1/import/file.sst")
		require.NoError(t, err)

		storeFile2, err := mux.StoreFile(ctx, "nodelocal://2/import/file.sst")
		require.NoError(t, err)

		storeFile3, err := mux.StoreFile(ctx, "s3://bucket/file.sst")
		require.NoError(t, err)

		// Verify different instances
		require.NotSame(t, storeFile1.Store, storeFile2.Store, "different nodes should have different instances")
		require.NotSame(t, storeFile1.Store, storeFile3.Store, "different schemes should have different instances")
		require.NotSame(t, storeFile2.Store, storeFile3.Store, "different schemes should have different instances")

		// Verify all three prefixes were created
		require.ElementsMatch(t, []string{"nodelocal://1", "nodelocal://2", "s3://bucket"}, createdURIs)
		require.Len(t, mux.storeInstances, 3, "should have three cached instances")
	})

	t.Run("propagates factory errors", func(t *testing.T) {
		expectedErr := cloud.ErrListingUnsupported
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			return nil, expectedErr
		}

		mux := NewExternalStorageMux(factory, username.RootUserName())
		defer func() {
			_ = mux.Close()
		}()

		_, err := mux.StoreFile(ctx, "nodelocal://1/file.sst")
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("Close closes all cached instances", func(t *testing.T) {
		mocks := make(map[string]*mockExternalStorage)
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			mock := &mockExternalStorage{uri: uri}
			mocks[uri] = mock
			return mock, nil
		}

		mux := NewExternalStorageMux(factory, username.RootUserName())

		// Create multiple cached instances
		_, err := mux.StoreFile(ctx, "nodelocal://1/file1.sst")
		require.NoError(t, err)
		_, err = mux.StoreFile(ctx, "nodelocal://2/file2.sst")
		require.NoError(t, err)
		_, err = mux.StoreFile(ctx, "s3://bucket/file3.sst")
		require.NoError(t, err)

		// Close should close all instances
		err = mux.Close()
		require.NoError(t, err)

		// Verify all mocks were closed
		require.Len(t, mocks, 3)
		for uri, mock := range mocks {
			require.True(t, mock.closed, "storage instance %s should be closed", uri)
		}
	})
}
