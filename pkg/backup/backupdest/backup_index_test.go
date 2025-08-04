// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestGetBackupIndexFileName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type coverageTime struct {
		// startTime and endTime represents hours in a single day. Because the
		// generated filenames are only millisecond specific, it's just easier this
		// way to ensure that the gaps between start and end time are large enough
		// to have a meaningful difference in the generated filenames.
		startTime int
		endTime   int
	}
	// Inputs should be sorted in the order you expect their outputted filenames
	// to be sorted in.
	inputs := []coverageTime{
		{startTime: 10, endTime: 12},
		{startTime: 2, endTime: 10}, // Compacted backup timestamp
		{startTime: 8, endTime: 10},
		{startTime: 4, endTime: 8},
		{startTime: 2, endTime: 4},
		{startTime: 0, endTime: 2},
	}

	var sortedFilenames []string
	timeToFilenames := make(map[coverageTime]string)
	for _, input := range inputs {
		start := time.Date(2025, 7, 18, input.startTime, 0, 0, 0, time.UTC)
		end := time.Date(2025, 7, 18, input.endTime, 0, 0, 0, time.UTC)
		name := getBackupIndexFileName(
			hlc.Timestamp{WallTime: start.UnixNano()},
			hlc.Timestamp{WallTime: end.UnixNano()},
		)
		if _, exists := timeToFilenames[input]; exists {
			t.Fatalf("duplicate index file name generated for %v", input)
		}
		timeToFilenames[input] = name
		sortedFilenames = append(sortedFilenames, name)
	}

	slices.Sort(sortedFilenames)
	expectedSortedFilenames := util.Map(
		inputs,
		func(input coverageTime) string {
			return timeToFilenames[input]
		},
	)

	require.Equal(
		t, expectedSortedFilenames, sortedFilenames, "sort order of index filenames does not match",
	)
}

func TestGetBackupIndexFilePath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	start, end := hlc.Timestamp{WallTime: 10}, hlc.Timestamp{WallTime: 20}
	t.Run("fails if subdir is 'LATEST' and unresolved", func(t *testing.T) {
		_, err := getBackupIndexFilePath("LATEST", start, end)
		require.Error(t, err)
	})
	t.Run("returns correct path for resolved subdir", func(t *testing.T) {
		subdir := "/2025/07/17-152115.00"
		flattenedSubdir := "2025-07-17-152115.00"
		indexPath, err := getBackupIndexFilePath(subdir, start, end)
		require.NoError(t, err)
		require.True(
			t, strings.HasPrefix(
				indexPath,
				path.Join(backupbase.BackupIndexDirectoryPath, flattenedSubdir),
			),
		)
	})
}

func TestWriteBackupIndexMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	externalStorage := newFakeExternalStorage()
	makeExternalStorage := func(
		_ context.Context, _ string, _ username.SQLUsername, _ ...cloud.ExternalStorageOption,
	) (cloud.ExternalStorage, error) {
		return externalStorage, nil
	}

	start := hlc.Timestamp{WallTime: 0}
	end := hlc.Timestamp{WallTime: time.Date(2025, 7, 30, 0, 0, 0, 0, time.UTC).UnixNano()}
	collectionURI := "nodelocal://1/backup"
	subdir := "/2025/07/18-143826.00"

	details := jobspb.BackupDetails{
		Destination: jobspb.BackupDetails_Destination{
			To:     []string{collectionURI},
			Subdir: subdir,
		},
		StartTime:     start,
		EndTime:       end,
		CollectionURI: collectionURI,
		URI:           collectionURI + subdir,
	}

	require.NoError(t, WriteBackupIndexMetadata(
		ctx, username.RootUserName(), makeExternalStorage, details,
	))

	filepath, err := getBackupIndexFilePath(subdir, start, end)
	require.NoError(t, err)

	reader, nBytes, err := externalStorage.ReadFile(ctx, filepath, cloud.ReadOptions{})
	require.NoError(t, err)

	contents := make([]byte, nBytes)
	_, err = reader.Read(ctx, contents)
	require.NoError(t, err)

	var metadata backuppb.BackupIndexMetadata
	require.NoError(t, protoutil.Unmarshal(contents, &metadata))

	require.Equal(t, start, metadata.StartTime)
	require.Equal(t, end, metadata.EndTime)
	require.Equal(t, subdir, metadata.Path)
}

func TestWriteBackupIndexMetadataWithLocalityAwareAndIncrementalStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	_, sqlDB, _, cleanup := backuptestutils.StartBackupRestoreTestCluster(
		t, 1, backuptestutils.WithTempDir(tempDir),
	)
	defer cleanup()

	collections := `('nodelocal://1/us-west?COCKROACH_LOCALITY=region%3Dus-west',
		'nodelocal://1/us-east?COCKROACH_LOCALITY=default')`
	incStorage := `('nodelocal://1/us-west/inc?COCKROACH_LOCALITY=region%3Dus-west',
		'nodelocal://1/us-east-inc?COCKROACH_LOCALITY=default')`

	sqlDB.Exec(t, fmt.Sprintf(`BACKUP INTO %s WITH incremental_location = %s`, collections, incStorage))
	sqlDB.Exec(t, fmt.Sprintf(
		`BACKUP INTO LATEST IN %s WITH incremental_location = %s`, collections, incStorage,
	))

	indexDir := path.Join(tempDir, "us-east", backupbase.BackupIndexDirectoryPath)
	fullIndexes, err := os.ReadDir(indexDir)
	require.NoError(t, err)
	require.Len(t, fullIndexes, 1)

	chainIndexes, err := os.ReadDir(path.Join(indexDir, fullIndexes[0].Name()))
	require.NoError(t, err)
	require.Len(t, chainIndexes, 2)

	var indexes []backuppb.BackupIndexMetadata
	for _, indexFile := range chainIndexes {
		indexPath := path.Join(indexDir, fullIndexes[0].Name(), indexFile.Name())
		contents, err := os.ReadFile(indexPath)
		require.NoError(t, err)

		var metadata backuppb.BackupIndexMetadata
		require.NoError(t, protoutil.Unmarshal(contents, &metadata))
		indexes = append(indexes, metadata)
	}

	fullIndex, incrIndex := indexes[0], indexes[1]
	if incrIndex.StartTime.IsEmpty() {
		fullIndex, incrIndex = incrIndex, fullIndex
	}

	require.True(t, fullIndex.StartTime.IsEmpty())
	require.False(t, incrIndex.StartTime.IsEmpty())

	// Ensure that the incremental path index starts immediately with the full
	// index path, implying that its path starts from the root of the incremental
	// storage directory.
	require.True(t, strings.HasPrefix(incrIndex.Path, fullIndex.Path))
}

type fakeExternalStorage struct {
	cloud.ExternalStorage
	files map[string]*closableBytesWriter
}

var _ cloud.ExternalStorage = &fakeExternalStorage{}

func newFakeExternalStorage() *fakeExternalStorage {
	return &fakeExternalStorage{
		files: make(map[string]*closableBytesWriter),
	}
}

func (f *fakeExternalStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider: cloudpb.ExternalStorageProvider_Unknown,
	}
}

type closableBytesWriter struct {
	bytes.Buffer
}

func (b *closableBytesWriter) Close() error {
	// No-op for bytes.Buffer, but satisfies io.WriteCloser interface.
	return nil
}

func (f *fakeExternalStorage) Writer(ctx context.Context, filename string) (io.WriteCloser, error) {
	if _, exists := f.files[filename]; exists {
		return nil, errors.Errorf("file %s already exists", filename)
	}
	buf := closableBytesWriter{}
	f.files[filename] = &buf
	return &buf, nil
}

type bytesReaderCtx struct {
	*bufio.Reader
}

func (br *bytesReaderCtx) Read(_ context.Context, p []byte) (n int, err error) {
	// Use the context to satisfy the ReaderCtx interface, but ignore it.
	return br.Reader.Read(p)
}

func (b *bytesReaderCtx) Close(_ context.Context) error {
	// No-op for bufio.Reader, but satisfies io.ReadCloser interface.
	return nil
}

func (f *fakeExternalStorage) ReadFile(
	ctx context.Context, filename string, _ cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	bytes, exists := f.files[filename]
	if !exists {
		return nil, 0, errors.Errorf("file %s does not exist", filename)
	}
	reader := bytesReaderCtx{
		Reader: bufio.NewReader(bytes),
	}
	return &reader, int64(bytes.Len()), nil
}
