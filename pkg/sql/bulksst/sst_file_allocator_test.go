// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/stretchr/testify/require"
)

func TestRowSampleReservoirCapsSamples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var base fileAllocatorBase
	expected := make(map[string]struct{})
	total := maxRowSamples*2 + 17
	for i := 0; i < total; i++ {
		key := roachpb.Key(fmt.Sprintf("key-%04d", i))
		expected[string(key)] = struct{}{}
		span := roachpb.Span{Key: key, EndKey: key.Next()}
		base.addFile(fmt.Sprintf("uri-%d", i), span, key, 1)
	}

	require.Equal(t, total, base.totalRowSamples)
	require.Len(t, base.fileInfo.RowSamples, maxRowSamples)

	for _, sample := range base.fileInfo.RowSamples {
		_, ok := expected[sample]
		require.True(t, ok, "sample %q must correspond to ingested keys", sample)
	}
}

func TestRowSampleSkipsEmptyKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var base fileAllocatorBase
	key := roachpb.Key("some-key")
	span := roachpb.Span{Key: key, EndKey: key.Next()}

	base.addFile("uri-1", span, key, 1)
	base.addFile("uri-2", span, nil, 1)

	require.Len(t, base.fileInfo.RowSamples, 1)
	require.Equal(t, string(key), base.fileInfo.RowSamples[0])
}

func TestWriterFlushFailureDoesNotPersistMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	failErr := errors.New("finish failed")
	allocator := &mockFailingAllocator{finishErr: failErr}

	writer := NewUnsortedSSTBatcher(st, allocator)
	ts := hlc.Timestamp{WallTime: 1}
	require.NoError(t, writer.AddMVCCKey(ctx, storage.MVCCKey{Key: roachpb.Key("k"), Timestamp: ts}, []byte("v")))

	err := writer.Flush(ctx)
	require.ErrorIs(t, err, failErr)

	fileList := allocator.GetFileList()
	require.Empty(t, fileList.SST, "no files should be tracked after a failed flush")
	require.Zero(t, fileList.TotalSize)
	require.Empty(t, fileList.RowSamples)
}

type mockFailingAllocator struct {
	fileAllocatorBase
	finishErr error
	nextID    int
}

func (m *mockFailingAllocator) AddFile(ctx context.Context) (objstorage.Writable, string, error) {
	uri := fmt.Sprintf("mock://%d", m.nextID)
	m.nextID++
	w := &failingWritable{finishErr: m.finishErr}
	return w, uri, nil
}

func (m *mockFailingAllocator) CommitFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	m.fileAllocatorBase.addFile(uri, span, rowSample, fileSize)
}

type failingWritable struct {
	finishErr error
}

func (f *failingWritable) Write(p []byte) error {
	return nil
}

func (f *failingWritable) Finish() error {
	return f.finishErr
}

func (f *failingWritable) Abort() {}
