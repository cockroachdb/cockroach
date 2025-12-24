// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"bytes"
	"context"
	"net/url"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockStorageWithTracking tracks all Delete and List operations.
type mockStorageWithTracking struct {
	cloud.ExternalStorage
	uri         string
	deleted     []string
	listed      []string
	files       map[string]struct{} // simulated files in storage
	deleteError error
	listError   error
}

func (m *mockStorageWithTracking) Delete(ctx context.Context, basename string) error {
	m.deleted = append(m.deleted, basename)
	if m.deleteError != nil {
		return m.deleteError
	}
	delete(m.files, basename)
	return nil
}

func (m *mockStorageWithTracking) List(
	ctx context.Context, prefix, delimiter string, fn cloud.ListingFn,
) error {
	m.listed = append(m.listed, prefix)
	if m.listError != nil {
		return m.listError
	}

	// Return files matching the prefix.
	var matches []string
	for file := range m.files {
		matches = append(matches, file)
	}
	sort.Strings(matches) // Ensure deterministic ordering.

	for _, file := range matches {
		if err := fn(file); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockStorageWithTracking) Close() error {
	return nil
}

func TestBulkJobCleaner_CleanupURIs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("deletes specific URIs successfully", func(t *testing.T) {
		mocks := make(map[string]*mockStorageWithTracking)
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			mock := &mockStorageWithTracking{
				uri:     uri,
				deleted: []string{},
				files:   make(map[string]struct{}),
			}
			mocks[uri] = mock
			return mock, nil
		}

		cleaner := NewBulkJobCleaner(factory, username.RootUserName())
		defer func() {
			_ = cleaner.Close()
		}()

		uris := []string{
			"nodelocal://1/job/123/map/file1.sst",
			"nodelocal://1/job/123/map/file2.sst",
			"nodelocal://2/job/123/merge/file3.sst",
		}

		err := cleaner.CleanupURIs(ctx, uris)
		require.NoError(t, err)

		// Verify deletions on node 1.
		require.Contains(t, mocks, "nodelocal://1")
		node1Deleted := mocks["nodelocal://1"].deleted
		require.ElementsMatch(t, []string{
			"/job/123/map/file1.sst",
			"/job/123/map/file2.sst",
		}, node1Deleted)

		// Verify deletions on node 2.
		require.Contains(t, mocks, "nodelocal://2")
		node2Deleted := mocks["nodelocal://2"].deleted
		require.ElementsMatch(t, []string{
			"/job/123/merge/file3.sst",
		}, node2Deleted)
	})

	t.Run("aggregates errors from multiple deletions", func(t *testing.T) {
		deleteErr := errors.New("delete failed")
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			return &mockStorageWithTracking{
				uri:         uri,
				deleted:     []string{},
				files:       make(map[string]struct{}),
				deleteError: deleteErr,
			}, nil
		}

		cleaner := NewBulkJobCleaner(factory, username.RootUserName())
		defer func() {
			_ = cleaner.Close()
		}()

		uris := []string{
			"nodelocal://1/job/123/file1.sst",
			"nodelocal://1/job/123/file2.sst",
		}

		err := cleaner.CleanupURIs(ctx, uris)
		require.Error(t, err)
		require.ErrorContains(t, err, "delete failed")
	})
}

func TestBulkJobCleaner_CleanupJobDirectories(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	jobID := jobspb.JobID(123)

	t.Run("removes all files under job directories", func(t *testing.T) {
		mocks := make(map[string]*mockStorageWithTracking)
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			mock := &mockStorageWithTracking{
				uri:     uri,
				deleted: []string{},
				listed:  []string{},
				files: map[string]struct{}{
					"/map/file1.sst":       {},
					"/map/file2.sst":       {},
					"/merge/file3.sst":     {},
					"/merge/sub/file4.sst": {},
				},
			}
			mocks[uri] = mock
			return mock, nil
		}

		cleaner := NewBulkJobCleaner(factory, username.RootUserName())
		defer func() {
			_ = cleaner.Close()
		}()

		// Pass storage prefixes (without /job/{jobID}/ path).
		storagePrefixes := []string{
			"nodelocal://1/export/",
			"nodelocal://2/export/",
		}

		err := cleaner.CleanupJobDirectories(ctx, jobID, storagePrefixes)
		require.NoError(t, err)

		// Verify node 1 was listed with correct job directory.
		require.Contains(t, mocks, "nodelocal://1")
		node1 := mocks["nodelocal://1"]
		require.Contains(t, node1.listed, "/export/job/123/")

		// Verify all files on node 1 were deleted.
		require.ElementsMatch(t, []string{
			"/export/job/123/map/file1.sst",
			"/export/job/123/map/file2.sst",
			"/export/job/123/merge/file3.sst",
			"/export/job/123/merge/sub/file4.sst",
		}, node1.deleted)

		// Verify node 2 was listed with correct job directory.
		require.Contains(t, mocks, "nodelocal://2")
		node2 := mocks["nodelocal://2"]
		require.Contains(t, node2.listed, "/export/job/123/")

		// Verify all files on node 2 were deleted.
		require.ElementsMatch(t, []string{
			"/export/job/123/map/file1.sst",
			"/export/job/123/map/file2.sst",
			"/export/job/123/merge/file3.sst",
			"/export/job/123/merge/sub/file4.sst",
		}, node2.deleted)
	})

	t.Run("deduplicates job directory prefixes", func(t *testing.T) {
		mocks := make(map[string]*mockStorageWithTracking)
		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			mock := &mockStorageWithTracking{
				uri:     uri,
				deleted: []string{},
				listed:  []string{},
				files:   map[string]struct{}{},
			}
			mocks[uri] = mock
			return mock, nil
		}

		cleaner := NewBulkJobCleaner(factory, username.RootUserName())
		defer func() {
			_ = cleaner.Close()
		}()

		// Same storage prefix specified multiple times should be deduplicated.
		storagePrefixes := []string{
			"nodelocal://1/export/",
			"nodelocal://1/export/",
			"nodelocal://1/export/",
		}

		err := cleaner.CleanupJobDirectories(ctx, jobID, storagePrefixes)
		require.NoError(t, err)

		// Verify the job directory was only listed once despite duplicate prefixes.
		require.Contains(t, mocks, "nodelocal://1")
		require.Len(t, mocks["nodelocal://1"].listed, 1)
		require.Equal(t, "/export/job/123/", mocks["nodelocal://1"].listed[0])
	})

	t.Run("aggregates delete errors from multiple files", func(t *testing.T) {
		deleteErr := errors.New("delete failed")

		factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
			return &mockStorageWithTracking{
				uri:     uri,
				deleted: []string{},
				listed:  []string{},
				files: map[string]struct{}{
					"/file1.sst": {},
					"/file2.sst": {},
				},
				deleteError: deleteErr,
			}, nil
		}

		cleaner := NewBulkJobCleaner(factory, username.RootUserName())
		defer func() {
			_ = cleaner.Close()
		}()

		storagePrefixes := []string{
			"nodelocal://1/export/",
		}

		err := cleaner.CleanupJobDirectories(ctx, jobID, storagePrefixes)
		require.Error(t, err)
		// Should see multiple delete errors combined.
		require.ErrorContains(t, err, "delete failed")
	})
}

func TestBulkJobCleaner_WithRealNodelocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	jobID := jobspb.JobID(789)

	// Create temp directory for nodelocal storage.
	tmpDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	// Setup factory that creates real nodelocal storage.
	clientFactory := blobs.TestBlobServiceClient(tmpDir)
	testSettings := cluster.MakeTestingClusterSettings()
	ioConf := base.ExternalIODirConfig{}

	factory := func(ctx context.Context, uri string, user username.SQLUsername, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
		conf, err := cloud.ExternalStorageConfFromURI(uri, user)
		if err != nil {
			return nil, err
		}
		return cloud.MakeExternalStorage(ctx, conf, ioConf, testSettings, clientFactory, nil, nil, cloud.NilMetrics)
	}

	// Helper to split URI into storage prefix and file path.
	getStorageAndPath := func(uri string) (string, string) {
		parsed, err := url.Parse(uri)
		require.NoError(t, err)
		// Storage prefix is scheme + host only.
		prefix := url.URL{
			Scheme: parsed.Scheme,
			Host:   parsed.Host,
		}
		return prefix.String(), parsed.Path
	}

	// Create files in job directory.
	files := []string{
		"nodelocal://1/export/job/789/map/file1.sst",
		"nodelocal://1/export/job/789/map/file2.sst",
		"nodelocal://1/export/job/789/merge/file3.sst",
		"nodelocal://1/export/job/789/merge/sub/file4.sst",
		"nodelocal://1/export/job/789/merge/sub/file5.sst",
	}

	// Write files.
	for _, uri := range files {
		storagePrefix, filePath := getStorageAndPath(uri)
		store, err := factory(ctx, storagePrefix, username.RootUserName())
		require.NoError(t, err)

		err = cloud.WriteFile(ctx, store, filePath, bytes.NewReader([]byte("test data")))
		require.NoError(t, err)
		require.NoError(t, store.Close())
	}

	// Verify all files exist.
	for _, uri := range files {
		storagePrefix, filePath := getStorageAndPath(uri)
		store, err := factory(ctx, storagePrefix, username.RootUserName())
		require.NoError(t, err)

		_, _, err = store.ReadFile(ctx, filePath, cloud.ReadOptions{NoFileSize: true})
		require.NoError(t, err, "file should exist before cleanup")
		require.NoError(t, store.Close())
	}

	cleaner := NewBulkJobCleaner(factory, username.RootUserName())
	defer func() {
		_ = cleaner.Close()
	}()

	// Cleanup one file.
	err := cleaner.CleanupURIs(ctx, []string{files[4]})
	require.NoError(t, err)

	// Verify the file is deleted.
	{
		storagePrefix, filePath := getStorageAndPath(files[4])
		store, err := factory(ctx, storagePrefix, username.RootUserName())
		require.NoError(t, err)

		_, _, err = store.ReadFile(ctx, filePath, cloud.ReadOptions{NoFileSize: true})
		require.Error(t, err, "file should not exist after cleanup")
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "expected file does not exist error")
		require.NoError(t, store.Close())
	}

	// Clean up entire job directory using storage prefix.
	storagePrefixes := []string{"nodelocal://1/export/"}
	err = cleaner.CleanupJobDirectories(ctx, jobID, storagePrefixes)
	require.NoError(t, err)

	// Verify all files in job directory are deleted.
	for _, uri := range files {
		storagePrefix, filePath := getStorageAndPath(uri)
		store, err := factory(ctx, storagePrefix, username.RootUserName())
		require.NoError(t, err)

		_, _, err = store.ReadFile(ctx, filePath, cloud.ReadOptions{NoFileSize: true})
		require.Error(t, err, "file should not exist after cleanup")
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "expected file does not exist error")
		require.NoError(t, store.Close())
	}
}
