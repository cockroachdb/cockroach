// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/stretchr/testify/require"
)

func TestSSTIndexBackfillSinkEmitsManifests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	allocator := &manifestTrackingAllocator{}

	writer := bulksst.NewUnsortedSSTBatcher(settings, allocator)
	ts := hlc.Timestamp{WallTime: 42}
	writer.SetWriteTS(ts)
	sink := &sstIndexBackfillSink{
		writer:           writer,
		allocator:        allocator,
		writeTS:          ts,
		emittedFileCount: 0,
	}
	sink.SetOnFlush(func(summary kvpb.BulkOpSummary) {})

	require.NoError(t, sink.Add(ctx, roachpb.Key("a"), []byte("1")))
	require.NoError(t, sink.Add(ctx, roachpb.Key("b"), []byte("2")))
	require.NoError(t, sink.Flush(ctx))

	manifests := sink.ConsumeFlushManifests()
	require.Len(t, manifests, 1)
	manifest := manifests[0]
	require.True(t, manifest.Span.ContainsKey(roachpb.Key("a")))
	require.True(t, manifest.Span.ContainsKey(roachpb.Key("b")))
	require.NotNil(t, manifest.WriteTimestamp)
	require.Equal(t, sink.writeTS, *manifest.WriteTimestamp)
	require.Contains(t, []string{"a", "b"}, string(manifest.RowSample))

	require.Nil(t, sink.ConsumeFlushManifests(), "manifests buffer should be cleared")
}

func TestSSTIndexBackfillSinkOnFlushCollectsCurrentManifests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	allocator := &manifestTrackingAllocator{}
	writer := bulksst.NewUnsortedSSTBatcher(settings, allocator)
	ts := hlc.Timestamp{WallTime: 42}
	writer.SetWriteTS(ts)
	var fromCallback []jobspb.IndexBackfillSSTManifest
	sink := &sstIndexBackfillSink{
		writer:           writer,
		allocator:        allocator,
		writeTS:          ts,
		emittedFileCount: 0,
	}
	sink.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		fromCallback = append(fromCallback, sink.ConsumeFlushManifests()...)
	})

	require.NoError(t, sink.Add(ctx, roachpb.Key("a"), []byte("1")))
	require.NoError(t, sink.Add(ctx, roachpb.Key("b"), []byte("2")))
	require.NoError(t, sink.Flush(ctx))

	require.Len(t, fromCallback, 1, "callback should observe manifests produced by latest flush")
	require.Equal(t, "mem://sst-0", fromCallback[0].URI)
	require.NotNil(t, fromCallback[0].WriteTimestamp)
	require.Equal(t, ts, *fromCallback[0].WriteTimestamp)
	require.Nil(t, sink.ConsumeFlushManifests(), "callback already drained manifests")
}

type manifestTrackingAllocator struct {
	files bulksst.SSTFiles
	next  int
}

var _ bulksst.FileAllocator = (*manifestTrackingAllocator)(nil)

func (m *manifestTrackingAllocator) AddFile(
	ctx context.Context,
) (objstorage.Writable, string, error) {
	uri := fmt.Sprintf("mem://sst-%d", m.next)
	m.next++
	return objstorageprovider.NewRemoteWritable(&discardWriteCloser{}), uri, nil
}

func (m *manifestTrackingAllocator) CommitFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	m.files.SST = append(m.files.SST, &bulksst.SSTFileInfo{
		URI:       uri,
		StartKey:  append([]byte(nil), span.Key...),
		EndKey:    append([]byte(nil), span.EndKey...),
		FileSize:  fileSize,
		RowSample: append([]byte(nil), rowSample...),
	})
	m.files.RowSamples = append(m.files.RowSamples, string(rowSample))
}

func (m *manifestTrackingAllocator) GetFileList() *bulksst.SSTFiles {
	return &m.files
}

type discardWriteCloser struct {
	bytes.Buffer
}

func (d *discardWriteCloser) Close() error { return nil }
