// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func encodeKey(strKey string) []byte {
	key := storage.MVCCKey{
		Key: []byte(strKey),
	}
	return storage.EncodeMVCCKeyToBuf(nil, key)
}

// createLocalBatcher creates a batcher with local file allocator for testing.
// Returns the batcher, allocator, and the temp directory where files are written.
func createLocalBatcher(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface, batchSize int64,
) (*bulksst.Writer, bulksst.FileAllocator, string) {
	t.Helper()
	tempDir := t.TempDir()
	externalStorage := nodelocal.TestingMakeNodelocalStorage(
		tempDir, s.ClusterSettings(), cloudpb.ExternalStorage{},
	)
	t.Cleanup(func() { externalStorage.Close() })

	fileAllocator := bulksst.NewExternalFileAllocator(externalStorage, "", s.Clock())
	if batchSize > 0 {
		bulksst.BatchSize.Override(ctx, &s.ClusterSettings().SV, batchSize)
	}
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)
	return batcher, fileAllocator, tempDir
}

// createExternalBatcher creates a batcher with external file allocator for testing.
// Returns the batcher, allocator, and the temp directory where files are written.
func createExternalBatcher(
	t *testing.T,
	ctx context.Context,
	s serverutils.TestServerInterface,
	baseURI string,
	batchSize int64,
) (*bulksst.Writer, bulksst.FileAllocator, string) {
	t.Helper()
	tempDir := t.TempDir()
	externalStorage := nodelocal.TestingMakeNodelocalStorage(
		tempDir, s.ClusterSettings(), cloudpb.ExternalStorage{},
	)
	t.Cleanup(func() { externalStorage.Close() })

	allocator := bulksst.NewExternalFileAllocator(externalStorage, baseURI, s.Clock())
	if batchSize > 0 {
		bulksst.BatchSize.Override(ctx, &s.ClusterSettings().SV, batchSize)
	}
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), allocator)
	return batcher, allocator, tempDir
}

// verifyRowSamplesInSpans validates that row samples fall within the provided spans.
func verifyRowSamplesInSpans(t *testing.T, samples []string, spans []roachpb.Span) {
	t.Helper()
	require.NotEmpty(t, samples)
	require.LessOrEqual(t, len(samples), 1024)
	for _, sample := range samples {
		sampleKey := roachpb.Key(sample)
		found := false
		for _, span := range spans {
			if span.ContainsKey(sampleKey) {
				found = true
				break
			}
		}
		require.True(t, found, "row sample key should be within a recorded SST span: %v", sampleKey)
	}
}

// verifyRowSamplesFromKeys validates that row samples originate from the expected keys.
func verifyRowSamplesFromKeys(
	t *testing.T, samples []string, expectedKeys map[string]struct{}, maxFiles int,
) {
	t.Helper()
	require.NotEmpty(t, samples)
	require.LessOrEqual(t, len(samples), maxFiles)
	require.LessOrEqual(t, len(samples), 1024)
	for _, sample := range samples {
		_, ok := expectedKeys[sample]
		require.True(t, ok, "row sample must come from ingested keys: %s", sample)
	}
}

type fakeWriteCloser struct {
	bytes.Buffer
}

func (f *fakeWriteCloser) Close() error {
	return nil
}

type fakeFileAllocator struct {
	spans []roachpb.Span
}

var _ bulksst.FileAllocator = (*fakeFileAllocator)(nil)

func (f *fakeFileAllocator) AddFile(ctx context.Context) (objstorage.Writable, string, error) {
	return objstorageprovider.NewRemoteWritable(&fakeWriteCloser{}), "", nil
}

func (f *fakeFileAllocator) CommitFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	f.spans = append(f.spans, span)
}

func (f *fakeFileAllocator) GetFileList() *bulksst.SSTFiles {
	return nil
}

func TestFlushBufferRecordsRowBoundarySpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	allocator := &fakeFileAllocator{}
	writer := bulksst.NewUnsortedSSTBatcher(settings, allocator)

	ts := hlc.Timestamp{WallTime: 1}
	rowPrefix := keys.SystemSQLCodec.IndexPrefix(7, 1)
	rowKey := encoding.EncodeUvarintAscending(rowPrefix, 42)
	cf1 := roachpb.Key(keys.MakeFamilyKey(append([]byte(nil), rowKey...), 1))
	cf2 := roachpb.Key(keys.MakeFamilyKey(append([]byte(nil), rowKey...), 2))

	require.NoError(t, writer.AddMVCCKey(ctx, storage.MVCCKey{Key: cf1, Timestamp: ts}, []byte("v1")))
	require.NoError(t, writer.AddMVCCKey(ctx, storage.MVCCKey{Key: cf2, Timestamp: ts}, []byte("v2")))
	require.NoError(t, writer.Flush(ctx))

	require.Len(t, allocator.spans, 1)
	require.True(t, allocator.spans[0].Valid())
	rowStart, err := keys.EnsureSafeSplitKey(cf1)
	require.NoError(t, err)
	require.Equal(t, rowStart, allocator.spans[0].Key)

	expectedEnd := rowStart.PrefixEnd()
	require.Equal(t, expectedEnd, allocator.spans[0].EndKey,
		"expected flushBuffer to advance span end to the next row boundary")
}

func TestBulkSSTWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	batcher, fileAllocator, tempDir := createLocalBatcher(t, ctx, s, 1024)

	// Intentionally go in an unsorted order.
	expectedSet := intsets.MakeFast()
	for i := 8192; i > 0; i-- {
		require.NoError(t, batcher.AddMVCCKey(ctx, storage.MVCCKey{
			Timestamp: s.Clock().Now(),
			Key:       encodeKey(fmt.Sprintf("key-%d", i)),
		},
			[]byte(fmt.Sprintf("value-%d", i))))
		expectedSet.Add(i)
	}
	require.NoError(t, batcher.CloseWithError(ctx))

	// Next validate each SST file.
	set := intsets.MakeFast()
	lastFileMin := -1
	sstFiles := fileAllocator.GetFileList()
	spans := make([]roachpb.Span, len(sstFiles.SST))
	for idx, sstInfo := range sstFiles.SST {
		currFileMin := -1
		currFileMax := -1
		values := readKeyValuesFromSST(t, filepath.Join(tempDir, sstInfo.URI))
		firstSafe, err := keys.EnsureSafeSplitKey(values[0].Key.Key.Clone())
		require.NoError(t, err)
		require.Equal(t, firstSafe, sstInfo.StartKey)

		lastSafe, err := keys.EnsureSafeSplitKey(values[len(values)-1].Key.Key.Clone())
		require.NoError(t, err)
		require.Equal(t, lastSafe.PrefixEnd(), sstInfo.EndKey)
		for _, value := range values {
			keyString := string(value.Key.Key)
			var num int
			scanned, err := fmt.Sscanf(strings.Split(keyString, "-")[1], "%d", &num)
			require.NoError(t, err)
			require.Equal(t, 1, scanned)
			set.Add(num)
			if currFileMin == -1 || currFileMin > num {
				currFileMin = num
			}
			if currFileMax == -1 || currFileMax < num {
				currFileMax = num
			}
		}
		// Validate the row sample is within the span of the SST file.
		fileSpan := roachpb.Span{
			Key:    sstInfo.StartKey,
			EndKey: sstInfo.EndKey,
		}
		spans[idx] = fileSpan
		// Ensure continuity between SSTs, where the minimum on the previous file
		// should continue to this file.
		if lastFileMin > 0 {
			require.Equal(t, lastFileMin-1, currFileMax)
		}
		lastFileMin = currFileMin
	}
	verifyRowSamplesInSpans(t, sstFiles.RowSamples, spans)
	// Ensure we have all the inserted key / values.
	require.Equal(t, 8192, set.Len())
	require.Greaterf(t, len(fileAllocator.GetFileList().SST), 100, "expected multiple files")
	require.Zero(t, expectedSet.Difference(set).Len())
}

func TestBulkAdderInterface(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("Add with timestamp", func(t *testing.T) {
		batcher, fileAllocator, tempDir := createLocalBatcher(t, ctx, s, 1024)
		writeTS := s.Clock().Now()
		batcher.SetWriteTS(writeTS)

		require.NoError(t, batcher.Add(ctx, roachpb.Key("key1"), []byte("value1")))
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key2"), []byte("value2")))
		require.NoError(t, batcher.CloseWithError(ctx))

		// Verify the timestamp was applied.
		sstFiles := fileAllocator.GetFileList()
		require.Len(t, sstFiles.SST, 1)
		values := readKeyValuesFromSST(t, filepath.Join(tempDir, sstFiles.SST[0].URI))
		require.Len(t, values, 2)
		require.Equal(t, writeTS, values[0].Key.Timestamp)
		require.Equal(t, writeTS, values[1].Key.Timestamp)
	})

	t.Run("IsEmpty", func(t *testing.T) {
		batcher, _, _ := createLocalBatcher(t, ctx, s, 0)
		require.True(t, batcher.IsEmpty())
		batcher.SetWriteTS(s.Clock().Now())
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key1"), []byte("value1")))
		require.False(t, batcher.IsEmpty())
		require.NoError(t, batcher.Flush(ctx))
		require.True(t, batcher.IsEmpty())
		require.NoError(t, batcher.CloseWithError(ctx))
	})

	t.Run("CurrentBufferFill", func(t *testing.T) {
		batcher, _, _ := createLocalBatcher(t, ctx, s, 1024)
		batcher.SetWriteTS(s.Clock().Now())
		require.Equal(t, float32(0), batcher.CurrentBufferFill())

		// Add some data.
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key1"), []byte("value1")))
		fill1 := batcher.CurrentBufferFill()
		require.Greater(t, fill1, float32(0))
		require.Less(t, fill1, float32(1))

		// Add more data.
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key2"), []byte("value2")))
		fill2 := batcher.CurrentBufferFill()
		require.Greater(t, fill2, fill1)

		// Flush resets to 0%.
		require.NoError(t, batcher.Flush(ctx))
		require.Equal(t, float32(0), batcher.CurrentBufferFill())
		require.NoError(t, batcher.CloseWithError(ctx))
	})

	t.Run("GetSummary and SetOnFlush", func(t *testing.T) {
		batcher, _, _ := createLocalBatcher(t, ctx, s, 1024)
		batcher.SetWriteTS(s.Clock().Now())

		// Track flush callbacks.
		var flushCount int
		var lastSummary kvpb.BulkOpSummary
		batcher.SetOnFlush(func(summary kvpb.BulkOpSummary) {
			flushCount++
			lastSummary = summary
		})

		summary := batcher.GetSummary()
		require.Equal(t, int64(0), summary.DataSize)

		// Add data and flush.
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key1"), []byte("value1")))
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key2"), []byte("value2")))
		require.NoError(t, batcher.Flush(ctx))
		require.Equal(t, 1, flushCount)
		require.Greater(t, lastSummary.DataSize, int64(0))
		require.Greater(t, lastSummary.SSTDataSize, int64(0))
		summary = batcher.GetSummary()
		require.Equal(t, lastSummary.DataSize, summary.DataSize)

		// Add more and flush again.
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key3"), []byte("value3")))
		require.NoError(t, batcher.Flush(ctx))
		require.Equal(t, 2, flushCount)
		summary = batcher.GetSummary()
		require.Greater(t, summary.DataSize, lastSummary.DataSize)

		require.NoError(t, batcher.CloseWithError(ctx))
	})
}

func TestExternalFileAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("Basic file allocation", func(t *testing.T) {
		baseURI := "test/"
		batcher, allocator, _ := createExternalBatcher(t, ctx, s, baseURI, 1024)
		batcher.SetWriteTS(s.Clock().Now())

		require.NoError(t, batcher.Add(ctx, roachpb.Key("key1"), []byte("value1")))
		require.NoError(t, batcher.Add(ctx, roachpb.Key("key2"), []byte("value2")))
		require.NoError(t, batcher.CloseWithError(ctx))
		fileList := allocator.GetFileList()
		require.Len(t, fileList.SST, 1)

		// Verify URI has correct prefix and suffix (timestamp-based naming).
		require.True(t, strings.HasPrefix(fileList.SST[0].URI, baseURI))
		require.True(t, strings.HasSuffix(fileList.SST[0].URI, ".sst"))
		require.Equal(t, roachpb.Key("key1"), fileList.SST[0].StartKey)
		require.Equal(t, roachpb.Key("key2").PrefixEnd(), fileList.SST[0].EndKey)
		require.Greater(t, fileList.SST[0].FileSize, uint64(0))
		require.Equal(t, fileList.SST[0].FileSize, fileList.TotalSize)
		require.Len(t, fileList.RowSamples, 1)
		require.Contains(t, []string{"key1", "key2"}, fileList.RowSamples[0])
	})

	t.Run("Multiple files", func(t *testing.T) {
		baseURI := "test-multi/"
		batcher, allocator, _ := createExternalBatcher(t, ctx, s, baseURI, 512) // Small buffer to force multiple files.
		batcher.SetWriteTS(s.Clock().Now())

		// Add data to create multiple SST files.
		expectedKeys := make(map[string]struct{})
		for i := 0; i < 100; i++ {
			key := roachpb.Key(fmt.Sprintf("key-%03d", i))
			value := []byte(fmt.Sprintf("value-%03d", i))
			require.NoError(t, batcher.Add(ctx, key, value))
			expectedKeys[string(key)] = struct{}{}
		}
		require.NoError(t, batcher.CloseWithError(ctx))

		fileList := allocator.GetFileList()
		require.Greater(t, len(fileList.SST), 1, "expected multiple SST files")
		var totalSize uint64
		seenURIs := make(map[string]bool)
		for _, sstFile := range fileList.SST {
			// Verify URI has correct prefix and suffix (timestamp-based naming).
			require.True(t, strings.HasPrefix(sstFile.URI, baseURI))
			require.True(t, strings.HasSuffix(sstFile.URI, ".sst"))
			// Verify each URI is unique.
			require.False(t, seenURIs[sstFile.URI], "duplicate URI: %s", sstFile.URI)
			seenURIs[sstFile.URI] = true
			require.Greater(t, sstFile.FileSize, uint64(0))
			totalSize += sstFile.FileSize
			require.True(t, sstFile.StartKey.Compare(sstFile.EndKey) < 0)
		}
		require.Equal(t, totalSize, fileList.TotalSize)
		verifyRowSamplesFromKeys(t, fileList.RowSamples, expectedKeys, len(fileList.SST))
	})

	t.Run("Metadata accumulation", func(t *testing.T) {
		batcher, allocator, _ := createExternalBatcher(t, ctx, s, "test-accum/", 256) // Very small buffer.
		batcher.SetWriteTS(s.Clock().Now())

		// Add data incrementally and verify metadata accumulates.
		require.NoError(t, batcher.Add(ctx, roachpb.Key("aaa"), []byte("value1")))
		require.NoError(t, batcher.Flush(ctx))
		fileList1 := allocator.GetFileList()
		require.Len(t, fileList1.SST, 1)
		size1 := fileList1.TotalSize

		require.NoError(t, batcher.Add(ctx, roachpb.Key("bbb"), []byte("value2")))
		require.NoError(t, batcher.Flush(ctx))
		fileList2 := allocator.GetFileList()
		require.Len(t, fileList2.SST, 2)
		require.Greater(t, fileList2.TotalSize, size1)

		require.NoError(t, batcher.CloseWithError(ctx))
	})
}

func readKeyValuesFromSST(t *testing.T, filename string) []storage.MVCCKeyValue {
	t.Helper()
	file, err := vfs.Default.Open(filename, vfs.SequentialReadsOption)
	require.NoError(t, err)
	readable, err := sstable.NewSimpleReadable(file)
	require.NoError(t, err)

	reader, err := sstable.NewReader(
		context.Background(),
		readable,
		storage.DefaultPebbleOptions().MakeReaderOptions())
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, iter.Close())
	}()

	result := make([]storage.MVCCKeyValue, 0)
	for internalKV := iter.First(); internalKV != nil; internalKV = iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(internalKV.K.UserKey)
		require.NoError(t, err)
		rawValue, _, err := internalKV.V.Value(nil)
		require.NoError(t, err)
		result = append(result, storage.MVCCKeyValue{
			Key:   mvccKey.Clone(),
			Value: rawValue,
		})
	}
	return result
}
