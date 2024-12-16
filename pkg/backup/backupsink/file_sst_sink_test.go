// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// TestFileSSTSinkExtendOneFile is a regression test for a bug in FileSSTSink in
// which the sink fails to extend its last span added if there's only one file
// in the sink so far.
func TestFileSSTSinkExtendOneFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	getKeys := func(prefix string, n int) []byte {
		var b bytes.Buffer
		sst := storage.MakeTransportSSTWriter(ctx, cluster.MakeTestingClusterSettings(), &b)
		for i := 0; i < n; i++ {
			require.NoError(t, sst.PutUnversioned([]byte(fmt.Sprintf("%s%08d", prefix, i)), nil))
		}
		sst.Close()
		return b.Bytes()
	}

	exportResponse1 := ExportedSpan{
		Metadata: backuppb.BackupManifest_File{
			Span: roachpb.Span{
				Key:    []byte("b"),
				EndKey: []byte("b"),
			},
			EntryCounts: roachpb.RowCount{
				DataSize: 100,
				Rows:     1,
			},
			StartTime:  hlc.Timestamp{},
			EndTime:    hlc.Timestamp{},
			LocalityKV: "",
		},
		DataSST:        getKeys("b", 100),
		RevStart:       hlc.Timestamp{},
		CompletedSpans: 1,
		ResumeKey:      []byte("b"),
	}

	exportResponse2 := ExportedSpan{
		Metadata: backuppb.BackupManifest_File{
			Span: roachpb.Span{
				Key:    []byte("b"),
				EndKey: []byte("z"),
			},
			EntryCounts: roachpb.RowCount{
				DataSize: 100,
				Rows:     1,
			},
			StartTime:  hlc.Timestamp{},
			EndTime:    hlc.Timestamp{},
			LocalityKV: "",
		},
		DataSST:        getKeys("c", 100),
		RevStart:       hlc.Timestamp{},
		CompletedSpans: 1,
	}

	st := cluster.MakeTestingClusterSettings()
	targetFileSize.Override(ctx, &st.SV, 20)
	sink, _ := fileSSTSinkTestSetup(t, st, execinfrapb.ElidePrefix_None)

	resumeKey, err := sink.Write(ctx, exportResponse1)
	require.NoError(t, err)
	require.Equal(t, exportResponse1.ResumeKey, resumeKey)
	resumeKey, err = sink.Write(ctx, exportResponse2)
	require.NoError(t, err)
	require.Equal(t, exportResponse2.ResumeKey, resumeKey)
	// Close the sink.
	require.NoError(t, err)

	close(sink.conf.ProgCh)

	var progs []execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	for p := range sink.conf.ProgCh {
		progs = append(progs, p)
	}

	require.Equal(t, 1, len(progs))
	var progDetails backuppb.BackupManifest_Progress
	if err := types.UnmarshalAny(&progs[0].ProgressDetails, &progDetails); err != nil {
		t.Fatal(err)
	}

	// Verify that the file in the sink was properly extended and there is only 1
	// file in the progress details.
	require.Equal(t, 1, len(progDetails.Files))
}

// TestFileSSTSinkWrite tests the contents of flushed files and the internal
// unflushed files of the FileSSTSink under different write scenarios. Each test
// writes a sequence of exportedSpans into a FileSSTSink. The test then verifies
// the spans of the flushed files and the unflushed files still left in the
// sink, as well as makes sure all keys in each file fall within the span
// boundaries.
func TestFileSSTSinkWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type testCase struct {
		name              string
		exportSpans       []ExportedSpan
		flushedSpans      []roachpb.Spans
		elideFlushedSpans []roachpb.Spans
		unflushedSpans    []roachpb.Spans
		// errorExplanation, if non-empty, explains why an error is expected when
		// writing the case inputs, and makes the test case fail if none is hit.
		//
		// TODO (msbutler): we currently don't test expected error handling. If this
		// is non-empty, we just skip the test.
		errorExplanation  string
		noSSTSizeOverride bool
	}

	for _, tt := range []testCase{{name: "out-of-order-key-boundary",
		exportSpans: []ExportedSpan{
			newExportedSpanBuilder("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "c", timestamp: 10}}).build(),
			newExportedSpanBuilder("b", "d").withKVs([]kvAndTS{{key: "b", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
		},
		flushedSpans:   []roachpb.Spans{{roachpb.Span{Key: s2k0("a"), EndKey: s2k0("c")}}},
		unflushedSpans: []roachpb.Spans{{roachpb.Span{Key: s2k0("b"), EndKey: s2k0("d")}}},
	},
		{
			// Test that even if the most recently ingested export span does not
			// end at a key boundary, a flush will still occur on the writing of
			// an out-of-order export span.
			//
			// TODO (msbutler): this test is currently skipped as it has a non nil errorExplanation. Unskip it.
			name: "out-of-order-not-key-boundary",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k0("a"), s2k0("c"), s2k0("c")).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "c", timestamp: 10}}).build(),
				newExportedSpanBuilder("b", "d").withKVs([]kvAndTS{{key: "b", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e").withKVs([]kvAndTS{{key: "c", timestamp: 9}, {key: "e", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{roachpb.Span{Key: s2k0("a"), EndKey: s2k0("c")}},
				{roachpb.Span{Key: s2k0("b"), EndKey: s2k0("d")}},
			},
			unflushedSpans:   []roachpb.Spans{{roachpb.Span{Key: []byte("c"), EndKey: []byte("e")}}},
			errorExplanation: "unsupported write ordering; backup processor should not do this due to one sink per worker and #118990.",
		},
		{
			name: "prefix-differ",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k0("2/a"), s2k0("2/c"), s2k0("2/c")).withKVs([]kvAndTS{{key: "2/a", timestamp: 10}, {key: "2/c", timestamp: 10}}).build(),
				newExportedSpanBuilder("2/c", "2/d").withKVs([]kvAndTS{{key: "2/c", timestamp: 9}, {key: "2/d", timestamp: 10}}).build(),
				newExportedSpanBuilder("3/c", "3/e").withKVs([]kvAndTS{{key: "3/c", timestamp: 9}, {key: "3/d", timestamp: 10}}).build(),
				newExportedSpanBuilder("2/e", "2/g").withKVs([]kvAndTS{{key: "2/e", timestamp: 10}, {key: "2/f", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{roachpb.Span{Key: s2k0("2/a"), EndKey: s2k0("2/d")}, roachpb.Span{Key: s2k0("3/c"), EndKey: s2k0("3/e")}},
			},
			elideFlushedSpans: []roachpb.Spans{
				{roachpb.Span{Key: s2k0("2/a"), EndKey: s2k0("2/d")}},
				{roachpb.Span{Key: s2k0("3/c"), EndKey: s2k0("3/e")}},
			},
			unflushedSpans: []roachpb.Spans{{roachpb.Span{Key: s2k0("2/e"), EndKey: s2k0("2/g")}}},
		},
		{
			name: "extend-key-boundary-1-file",
			exportSpans: []ExportedSpan{
				newExportedSpanBuilder("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e").withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: s2k0("a"), EndKey: s2k0("e")}}},
		},
		{
			name: "extend-key-boundary-2-files",
			exportSpans: []ExportedSpan{
				newExportedSpanBuilder("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e").withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
				newExportedSpanBuilder("e", "g").withKVs([]kvAndTS{{key: "e", timestamp: 10}, {key: "f", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: s2k0("a"), EndKey: s2k0("g")}}},
		},
		{
			name: "extend-not-key-boundary",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k0("a"), s2k0("c"), s2k0("c")).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "c", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e").withKVs([]kvAndTS{{key: "c", timestamp: 9}, {key: "d", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: s2k0("a"), EndKey: s2k0("e")}}},
		},
		{
			// TODO(rui): currently it is possible to make the sink error if we
			// write different times of the revision history for the same key
			// out of order.
			// Issue: https://github.com/cockroachdb/cockroach/issues/105372
			//
			// TODO(msbutler): this test is skipped, as it has a non nil errorExplanation. Unskip this.
			name:             "extend-same-key",
			errorExplanation: "incorrectly fails with pebble: keys must be added in strictly increasing order",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k0("a"), s2k0("a"), s2k("a")).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "a", timestamp: 9}}).build(),
				newRawExportedSpanBuilder(s2k0("a"), s2k0("a"), s2k("a")).withKVs([]kvAndTS{{key: "a", timestamp: 5}, {key: "a", timestamp: 4}}).build(),
				newRawExportedSpanBuilder(s2k0("a"), s2k0("a"), s2k("a")).withKVs([]kvAndTS{{key: "a", timestamp: 8}, {key: "a", timestamp: 7}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: []byte("a"), EndKey: []byte("e")}}},
		},
		{
			name: "extend-Metadata-same-timestamp",
			exportSpans: []ExportedSpan{
				newExportedSpanBuilder("a", "c").
					withKVs([]kvAndTS{{key: "a", timestamp: 5}, {key: "b", timestamp: 5}}).
					withStartTime(5).
					withEndTime(10).
					build(),
				newExportedSpanBuilder("c", "e").
					withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).
					withStartTime(5).
					withEndTime(10).
					build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("e")}},
			},
		},
		{
			name: "no-extend-Metadata-timestamp-mismatch",
			exportSpans: []ExportedSpan{
				newExportedSpanBuilder("a", "c").
					withKVs([]kvAndTS{{key: "a", timestamp: 5}, {key: "b", timestamp: 5}}).
					withEndTime(5).
					build(),
				newExportedSpanBuilder("c", "e").
					withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).
					withStartTime(5).
					withEndTime(10).
					build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}, {Key: s2k0("c"), EndKey: s2k0("e")}},
			},
		},
		{
			name: "size-flush",
			exportSpans: []ExportedSpan{
				newExportedSpanBuilder("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 20<<20)}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("d", "f").withKVs([]kvAndTS{{key: "d", timestamp: 10}, {key: "e", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("d"), EndKey: s2k0("f")}},
			},
		},
		{
			// No flush can occur between two versions of the same key. Further, we must combine flushes which split a row.
			name: "no-size-flush-if-mid-mvcc",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k0("a"), s2k0("c"), s2k0("c")).withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 20<<20)}, {key: "c", timestamp: 10}}).build(),
				newRawExportedSpanBuilder(s2k0("c"), s2k0("f"), s2k0("f")).withKVs([]kvAndTS{{key: "c", timestamp: 8}, {key: "f", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("f")}},
			},
		},
		{
			// No flush can occur between the two column families of the same row. Further, we must combine flushes which split a row.
			name: "no-size-flush-mid-col-family",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2kWithColFamily("c", 0), s2kWithColFamily("c", 1), s2kWithColFamily("c", 1)).withKVs([]kvAndTS{
					{key: "c", timestamp: 10, value: make([]byte, 20<<20)}}).build(),
				newRawExportedSpanBuilder(s2kWithColFamily("c", 1), s2kWithColFamily("c", 2), s2kWithColFamily("c", 2)).withKVs([]kvAndTS{
					{key: "c", timestamp: 10, value: make([]byte, 20<<20)}}).buildWithEncoding(func(stingedKey string) roachpb.Key { return s2kWithColFamily(stingedKey, 1) }),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2kWithColFamily("c", 0), EndKey: s2kWithColFamily("c", 2)}},
			},
		},
		{
			// It's safe to flush at the range boundary.
			name: "size-flush-at-range-boundary",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k("a"), s2k("d"), s2k("d")).withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 20<<20)}, {key: "c", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k("a"), EndKey: s2k("d")}},
			},
			unflushedSpans: []roachpb.Spans{},
		},
		{
			// If the max key in the exported span is less than the trimmed span end
			// key (i.e. without its column family), set the resume key and the end
			// key to this trimmed key. The trimmed key ensures we never split in a
			// row between two column families.
			name: "trim-resume-key",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2k0("a"), s2k0("c"), s2k("c")).withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 20<<20)}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k("c")}},
			},
			unflushedSpans: []roachpb.Spans{},
		},
		{
			// If a logical backup file is sufficiently large, the file may be cut
			// even if the next span's start key matches the file's end key.
			name: "file-size-cut",
			exportSpans: []ExportedSpan{
				newExportedSpanBuilder("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 64<<20)}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "f").withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "e", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}, {Key: s2k0("c"), EndKey: s2k0("f")}},
			},
			noSSTSizeOverride: true,
		},
		{
			// No file cut can occur between the two column families of the same row,
			// even if the file is sufficiently large to get cut.
			name: "no-file-cut-mid-col-family",
			exportSpans: []ExportedSpan{
				newRawExportedSpanBuilder(s2kWithColFamily("c", 0), s2kWithColFamily("c", 1), s2kWithColFamily("c", 1)).withKVs([]kvAndTS{
					{key: "c", timestamp: 10, value: make([]byte, 65<<20)}}).build(),
				newRawExportedSpanBuilder(s2kWithColFamily("c", 1), s2kWithColFamily("c", 2), s2kWithColFamily("c", 2)).withKVs([]kvAndTS{
					{key: "c", timestamp: 10, value: make([]byte, 20<<20)}}).buildWithEncoding(func(stingedKey string) roachpb.Key { return s2kWithColFamily(stingedKey, 1) }),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				// The export spans, even though they are very large, must merge because
				// the first one is split mid key.
				{{Key: s2kWithColFamily("c", 0), EndKey: s2kWithColFamily("c", 2)}},
			},
		},
	} {
		for _, elide := range []execinfrapb.ElidePrefix{execinfrapb.ElidePrefix_None, execinfrapb.ElidePrefix_TenantAndTable} {
			t.Run(fmt.Sprintf("%s/elide=%s", tt.name, elide), func(t *testing.T) {
				if tt.errorExplanation != "" {
					return
				}
				st := cluster.MakeTestingClusterSettings()
				if !tt.noSSTSizeOverride {
					targetFileSize.Override(ctx, &st.SV, 10<<10)
				}

				sink, store := fileSSTSinkTestSetup(t, st, elide)
				defer func() {
					require.NoError(t, sink.Close())
				}()

				var resumeKey roachpb.Key
				var err error
				for _, es := range tt.exportSpans {
					if !resumeKey.Equal(roachpb.Key{}) {
						require.Equal(t, resumeKey, es.Metadata.Span.Key, "invalid test case: if the previous span emits a resume key, the next span must start with this key")
					}
					resumeKey, err = sink.Write(ctx, es)
					require.NoError(t, err)
					if !es.ResumeKey.Equal(resumeKey) {
						require.NoError(t, err)
					}
					require.Equal(t, es.ResumeKey, resumeKey, "unexpected resume key")
				}

				progress := make([]backuppb.BackupManifest_File, 0)

			Loop:
				for {
					select {
					case p := <-sink.conf.ProgCh:
						var progDetails backuppb.BackupManifest_Progress
						if err := types.UnmarshalAny(&p.ProgressDetails, &progDetails); err != nil {
							t.Fatal(err)
						}

						progress = append(progress, progDetails.Files...)
					default:
						break Loop
					}
				}
				expectedSpans := tt.flushedSpans
				eliding := sink.conf.ElideMode != execinfrapb.ElidePrefix_None
				if eliding && len(tt.elideFlushedSpans) > 0 {
					expectedSpans = tt.elideFlushedSpans
				}
				// ProgCh contains the files that have already been created with
				// flushes. Verify the contents.
				require.NoError(t, checkFiles(ctx, store, progress, expectedSpans, eliding))

				// flushedFiles contain the files that are in queue to be created on the
				// next flush. Save these and then flush the sink to check their contents.
				var actualUnflushedFiles []backuppb.BackupManifest_File
				actualUnflushedFiles = append(actualUnflushedFiles, sink.flushedFiles...)
				// We cannot end the test -- by calling flush -- if the sink is mid-key.
				if len(tt.exportSpans) > 0 && !tt.exportSpans[len(tt.exportSpans)-1].ResumeKey.Equal(roachpb.Key{}) {
					sink.WriteWithNoData(newExportedSpanBuilder("z", "zz").build())
				}
				require.NoError(t, sink.Flush(ctx))
				require.NoError(t, checkFiles(ctx, store, actualUnflushedFiles, tt.unflushedSpans, eliding))
				require.Empty(t, sink.flushedFiles)
			})
		}
	}
}

func s2k(s string) roachpb.Key {
	tbl := 1
	k := []byte(s)
	if p := strings.Split(s, "/"); len(p) > 1 {
		tbl, _ = strconv.Atoi(p[0])
		k = []byte(p[1])
	}
	return append(keys.SystemSQLCodec.IndexPrefix(uint32(tbl), 2), k...)
}

func s2kWithColFamily(s string, colfamily uint64) roachpb.Key {
	keys := s2k(s)
	return encoding.EncodeUvarintAscending(keys, colfamily)
}

func s2k0(s string) roachpb.Key {
	return s2kWithColFamily(s, 0)
}

func s2k1(s string) roachpb.Key {
	return s2kWithColFamily(s, 1)
}

// TestFileSSTSinkStats tests the internal counters and stats of the FileSSTSink under
// different write scenarios.
func TestFileSSTSinkStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	targetFileSize.Override(ctx, &st.SV, 10<<10)

	sink, _ := fileSSTSinkTestSetup(t, st, execinfrapb.ElidePrefix_None)

	defer func() {
		require.NoError(t, sink.Close())
	}()

	type sinkStats struct {
		flushedRevStart hlc.Timestamp
		completedSpans  int32
		files           int
		flushes         int
		oooFlushes      int
		sizeFlushes     int
		spanGrows       int
	}

	type inputAndExpectedStats struct {
		input         ExportedSpan
		expectedStats sinkStats
	}

	//  no extends, extends, flush due to size, flush out of order
	inputs := []inputAndExpectedStats{
		{
			// Write the first exported span to the sink.
			newExportedSpanBuilder("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10}}).build(),
			sinkStats{hlc.Timestamp{}, 1, 1, 0, 0, 0, 0},
		},
		{
			// Write another exported span after the first span that doesn't
			// extend the previous span. This ES also has a revStartTime.
			newExportedSpanBuilder("d", "e").withKVs([]kvAndTS{{key: "d", timestamp: 10}}).withRevStartTime(5).build(),
			sinkStats{hlc.Timestamp{WallTime: 5}, 2, 2, 0, 0, 0, 0}},
		{
			// Write an exported span that extends the previous span. This ES
			// also has a later revStartTime.
			newExportedSpanBuilder("e", "f").withKVs([]kvAndTS{{key: "e", timestamp: 10}}).withRevStartTime(10).build(),
			sinkStats{hlc.Timestamp{WallTime: 10}, 3, 3, 0, 0, 0, 1}},
		{
			// Write an exported span that comes after all spans so far. This span has enough data for a size flush.
			newExportedSpanBuilder("g", "h").withKVs([]kvAndTS{{key: "g", timestamp: 10, value: make([]byte, 20<<20)}}).build(),
			sinkStats{hlc.Timestamp{WallTime: 0}, 0, 4, 1, 0, 1, 1}},
		{
			// Write the first exported span after the flush.
			newExportedSpanBuilder("i", "k").withKVs([]kvAndTS{{key: "i", timestamp: 10}, {key: "j", timestamp: 10}}).build(),
			sinkStats{hlc.Timestamp{}, 1, 5, 1, 0, 1, 1}},
		{
			// Write another exported span that causes an out of order flush.
			newExportedSpanBuilder("j", "l").withKVs([]kvAndTS{{key: "j", timestamp: 10}, {key: "k", timestamp: 10}}).build(),
			sinkStats{hlc.Timestamp{}, 1, 6, 2, 1, 1, 1}},
	}

	for _, input := range inputs {
		resumeKey, err := sink.Write(ctx, input.input)
		require.NoError(t, err)
		require.Nil(t, input.input.ResumeKey, resumeKey)

		actualStats := sinkStats{
			flushedRevStart: sink.flushedRevStart,
			completedSpans:  sink.completedSpans,
			files:           sink.stats.files,
			flushes:         sink.stats.flushes,
			oooFlushes:      sink.stats.oooFlushes,
			sizeFlushes:     sink.stats.sizeFlushes,
			spanGrows:       sink.stats.spanGrows,
		}

		require.Equal(t, input.expectedStats, actualStats, "stats after write for span %v", input.input.Metadata.Span)
	}

	require.NoError(t, sink.Flush(ctx))
}

func TestFileSSTSinkCopyPointKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	type testInput struct {
		input     []kvAndTS
		expectErr string
	}

	type testCase struct {
		name   string
		inputs []testInput
	}

	for _, tt := range []testCase{
		{
			name: "single-exported-span",
			inputs: []testInput{
				{
					input: []kvAndTS{
						{key: "a", value: []byte("1"), timestamp: 10},
						{key: "a", value: []byte("2"), timestamp: 9},
						{key: "b", value: []byte("2"), timestamp: 10},
					},
				},
			},
		},
		{
			name: "multiple-exported-spans",
			inputs: []testInput{
				{
					input: []kvAndTS{
						{key: "a", value: []byte("1"), timestamp: 10},
						{key: "a", value: []byte("2"), timestamp: 9},
						{key: "b", value: []byte("2"), timestamp: 10},
					},
				},
				{
					input: []kvAndTS{
						{key: "c", value: []byte("3"), timestamp: 10},
						{key: "d", value: []byte("4"), timestamp: 9},
						{key: "e", value: []byte("5"), timestamp: 8},
					},
				},
				{
					input: []kvAndTS{
						{key: "e", value: []byte("3"), timestamp: 6},
						{key: "f", value: []byte("4"), timestamp: 10},
						{key: "g", value: []byte("5"), timestamp: 10},
					},
				},
			},
		},
		{
			name: "out-of-order-key",
			inputs: []testInput{
				{
					input: []kvAndTS{
						{key: "a", value: []byte("1"), timestamp: 10},
						{key: "c", value: []byte("2"), timestamp: 10},
					},
				},
				{
					input: []kvAndTS{
						{key: "b", value: []byte("3"), timestamp: 10},
						{key: "d", value: []byte("4"), timestamp: 10},
					},
					expectErr: "keys must be added in strictly increasing order",
				},
			},
		},
		{
			name: "out-of-order-timestamp",
			inputs: []testInput{
				{
					input: []kvAndTS{
						{key: "a", value: []byte("1"), timestamp: 10},
						{key: "b", value: []byte("2"), timestamp: 10},
					},
				},
				{
					input: []kvAndTS{
						{key: "b", value: []byte("3"), timestamp: 11},
						{key: "c", value: []byte("4"), timestamp: 10},
					},
					expectErr: "keys must be added in strictly increasing order",
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			sst := storage.MakeTransportSSTWriter(ctx, settings, buf)
			sink := FileSSTSink{sst: sst}
			compareSST := true

			for _, input := range tt.inputs {
				kvs := input.input
				// Add a range key in the input as well.
				es := newRawExportedSpanBuilder(s2k0(kvs[0].key), s2k0(kvs[len(kvs)-1].key), s2k0(kvs[len(kvs)-1].key)).
					withKVs(kvs).
					withRangeKeys([]rangeKeyAndTS{{"a", "z", 10}}).
					build()
				maxKey, err := sink.copyPointKeys(ctx, es.DataSST)
				if input.expectErr != "" {
					// Do not compare resulting SSTs if we expect errors.
					require.ErrorContains(t, err, input.expectErr)
					compareSST = false
					require.Nil(t, maxKey)
				} else {
					require.NoError(t, err)
					// NB: the assertion below will not be true for all exported spans,
					// but it is for the exported spans constrcted in this test, where we
					// set the end key of the exported span to the last key in the input.
					require.Equal(t, es.ResumeKey, maxKey)
				}
			}

			sst.Close()

			if !compareSST {
				return
			}

			var expected []kvAndTS
			for _, input := range tt.inputs {
				for i := range input.input {
					input.input[i].key = string(s2k0(input.input[i].key))
					v := roachpb.Value{}
					v.SetBytes(input.input[i].value)

					input.input[i].value = v.RawBytes
				}
				expected = append(expected, input.input...)

			}

			iterOpts := storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsAndRanges,
				LowerBound: keys.LocalMax,
				UpperBound: keys.MaxKey,
			}

			iter, err := storage.NewMemSSTIterator(buf.Bytes(), false, iterOpts)
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()

			var actual []kvAndTS
			for iter.SeekGE(storage.MVCCKey{}); ; iter.Next() {
				if ok, err := iter.Valid(); err != nil {
					t.Fatal(err)
				} else if !ok {
					break
				}

				key := iter.UnsafeKey()
				value, err := iter.UnsafeValue()
				if err != nil {
					t.Fatal(err)
				}

				kv := kvAndTS{key: string(key.Key), timestamp: key.Timestamp.WallTime}
				kv.value = append(kv.value, value...)

				actual = append(actual, kv)
			}

			require.Equal(t, expected, actual)
		})
	}
}

func TestFileSSTSinkCopyRangeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	type testInput struct {
		input          []rangeKeyAndTS
		expectedMaxKey roachpb.Key
		expectErr      string
	}

	type testCase struct {
		name   string
		inputs []testInput
	}

	for _, tt := range []testCase{
		{
			name: "single-exported-span",
			inputs: []testInput{
				{
					input: []rangeKeyAndTS{
						{key: "a", endKey: "b", timestamp: 10},
						{key: "b", endKey: "c", timestamp: 9},
					},
					expectedMaxKey: roachpb.Key("c"),
				},
			},
		},
		{
			name: "multiple-exported-spans",
			inputs: []testInput{
				{
					input: []rangeKeyAndTS{
						{key: "a", endKey: "b", timestamp: 10},
						{key: "b", endKey: "c", timestamp: 9},
					},
					expectedMaxKey: roachpb.Key("c"),
				},
				{
					input: []rangeKeyAndTS{
						{key: "c", endKey: "d", timestamp: 10},
						{key: "c", endKey: "d", timestamp: 9},
					},
					expectedMaxKey: roachpb.Key("d"),
				},
				{
					input: []rangeKeyAndTS{
						{key: "c", endKey: "d", timestamp: 8},
					},
					expectedMaxKey: roachpb.Key("d"),
				},
			},
		},
		{
			name: "out-of-order-range",
			inputs: []testInput{
				{
					input: []rangeKeyAndTS{
						{key: "a", endKey: "b", timestamp: 10},
						{key: "b", endKey: "d", timestamp: 9},
					},
					expectedMaxKey: roachpb.Key("d"),
				},
				{
					input: []rangeKeyAndTS{
						{key: "a", endKey: "c", timestamp: 8},
						{key: "c", endKey: "e", timestamp: 7},
					},
					expectErr: "spans must be added in order",
				},
			},
		},
		{
			name: "out-of-order-timestamp",
			inputs: []testInput{
				{
					input: []rangeKeyAndTS{
						{key: "a", endKey: "b", timestamp: 10},
						{key: "b", endKey: "d", timestamp: 9},
					},
					expectedMaxKey: roachpb.Key("d"),
				},
				{
					input: []rangeKeyAndTS{
						{key: "b", endKey: "d", timestamp: 11},
						{key: "c", endKey: "e", timestamp: 7},
					},
					expectedMaxKey: roachpb.Key("e"),
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			sst := storage.MakeTransportSSTWriter(ctx, settings, buf)
			sink := FileSSTSink{sst: sst}
			compareSST := true

			for _, input := range tt.inputs {
				rangeKeys := input.input
				// Add some point key values in the input as well.
				es := newRawExportedSpanBuilder(s2k(rangeKeys[0].key), s2k(rangeKeys[len(rangeKeys)-1].key), s2k(rangeKeys[len(rangeKeys)-1].key)).
					withRangeKeys(rangeKeys).
					withKVs([]kvAndTS{{key: rangeKeys[0].key, timestamp: rangeKeys[0].timestamp}}).
					build()
				resumeKey, err := sink.copyRangeKeys(es.DataSST)
				if input.expectErr != "" {
					// Do not compare resulting SSTs if we expect errors.
					require.ErrorContains(t, err, input.expectErr)
					compareSST = false
					require.Nil(t, resumeKey)
				} else {
					require.Equal(t, input.expectedMaxKey, resumeKey)
					require.NoError(t, err)
				}
			}

			sst.Close()

			if !compareSST {
				return
			}

			expected := make(map[int64]*roachpb.SpanGroup)
			for _, input := range tt.inputs {
				for _, rk := range input.input {
					if expected[rk.timestamp] == nil {
						expected[rk.timestamp] = &roachpb.SpanGroup{}
					}
					t.Logf("%d: Adding %v", rk.timestamp, rk.span())
					expected[rk.timestamp].Add(rk.span())
				}
			}

			iterOpts := storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsAndRanges,
				LowerBound: keys.LocalMax,
				UpperBound: keys.MaxKey,
			}

			iter, err := storage.NewMemSSTIterator(buf.Bytes(), false, iterOpts)
			if err != nil {
				t.Fatal(err)
			}

			defer iter.Close()

			var actual []rangeKeyAndTS
			for iter.SeekGE(storage.MVCCKey{}); ; iter.Next() {
				if ok, err := iter.Valid(); err != nil {
					t.Fatal(err)
				} else if !ok {
					break
				}

				rangeKeys := iter.RangeKeys()
				for _, v := range rangeKeys.Versions {
					rk := rangeKeyAndTS{
						key:       string(rangeKeys.Bounds.Key),
						endKey:    string(rangeKeys.Bounds.EndKey),
						timestamp: v.Timestamp.WallTime,
					}
					actual = append(actual, rk)

				}
			}

			for _, rk := range actual {
				sp := rk.span()
				if !expected[rk.timestamp].Encloses(sp) {
					t.Fatalf("expected to copy range %v at timestamp %d", sp, rk.timestamp)
				}
				expected[rk.timestamp].Sub(sp)
			}

			for ts, sg := range expected {
				for _, missing := range sg.Slice() {
					t.Fatalf("expected range %v at timestamp %d to be copied", missing, ts)
				}
			}
		})
	}
}

type kvAndTS struct {
	key       string
	value     []byte
	timestamp int64
}

func (k kvAndTS) toMVCCKV(enc func(string) roachpb.Key) mvccKV {
	v := roachpb.Value{}
	v.SetBytes(k.value)
	v.InitChecksum(nil)
	return mvccKV{
		key: storage.MVCCKey{
			Key:       enc(k.key),
			Timestamp: hlc.Timestamp{WallTime: k.timestamp},
		},
		value: v.RawBytes,
	}
}

type rangeKeyAndTS struct {
	key       string
	endKey    string
	timestamp int64
}

func (rk rangeKeyAndTS) span() roachpb.Span {
	return roachpb.Span{
		Key:    s2k(rk.key),
		EndKey: s2k(rk.endKey),
	}
}

type exportedSpanBuilder struct {
	es        *ExportedSpan
	keyValues []kvAndTS
	rangeKeys []rangeKeyAndTS
}

func newExportedSpanBuilder(spanStart, spanEnd string) *exportedSpanBuilder {
	return newRawExportedSpanBuilder(s2k0(spanStart), s2k0(spanEnd), nil)
}

func newRawExportedSpanBuilder(spanStart, spanEnd, resumeKey roachpb.Key) *exportedSpanBuilder {
	return &exportedSpanBuilder{
		es: &ExportedSpan{
			Metadata: backuppb.BackupManifest_File{
				Span: roachpb.Span{
					Key:    spanStart,
					EndKey: spanEnd,
				},
				EntryCounts: roachpb.RowCount{
					DataSize:     1,
					Rows:         1,
					IndexEntries: 0,
				},
			},
			CompletedSpans: 1,
			ResumeKey:      resumeKey,
		},
	}
}

func (b *exportedSpanBuilder) withKVs(keyValues []kvAndTS) *exportedSpanBuilder {
	b.keyValues = keyValues
	for _, kv := range keyValues {
		b.es.Metadata.EntryCounts.DataSize += int64(len(kv.key))
		b.es.Metadata.EntryCounts.DataSize += int64(len(kv.value))
	}
	return b
}

func (b *exportedSpanBuilder) withRangeKeys(rangeKeys []rangeKeyAndTS) *exportedSpanBuilder {
	b.rangeKeys = rangeKeys
	return b
}

func (b *exportedSpanBuilder) withStartTime(time int64) *exportedSpanBuilder {
	b.es.Metadata.StartTime = hlc.Timestamp{WallTime: time}
	return b
}

func (b *exportedSpanBuilder) withEndTime(time int64) *exportedSpanBuilder {
	b.es.Metadata.EndTime = hlc.Timestamp{WallTime: time}
	return b
}

func (b *exportedSpanBuilder) withRevStartTime(time int64) *exportedSpanBuilder {
	b.es.RevStart = hlc.Timestamp{WallTime: time}
	return b
}

func (b *exportedSpanBuilder) build() ExportedSpan {
	return b.buildWithEncoding(s2k0)
}

func (b *exportedSpanBuilder) buildWithEncoding(stringToKey func(string) roachpb.Key) ExportedSpan {
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	buf := &bytes.Buffer{}
	sst := storage.MakeTransportSSTWriter(ctx, settings, buf)
	for _, d := range b.keyValues {
		v := roachpb.Value{}
		v.SetBytes(d.value)
		v.InitChecksum(nil)
		err := sst.Put(storage.MVCCKey{
			Key:       stringToKey(d.key),
			Timestamp: hlc.Timestamp{WallTime: d.timestamp},
		}, v.RawBytes)
		if err != nil {
			panic(err)
		}
	}

	for _, d := range b.rangeKeys {
		err := sst.PutMVCCRangeKey(storage.MVCCRangeKey{
			Timestamp: hlc.Timestamp{WallTime: d.timestamp},
			StartKey:  []byte(d.key),
			EndKey:    []byte(d.endKey),
		}, storage.MVCCValue{})

		if err != nil {
			panic(err)
		}
	}

	sst.Close()

	b.es.DataSST = buf.Bytes()

	return *b.es
}

func fileSSTSinkTestSetup(
	t *testing.T, settings *cluster.Settings, elideMode execinfrapb.ElidePrefix,
) (*FileSSTSink, cloud.ExternalStorage) {
	conf, store := sinkTestSetup(t, settings, elideMode)
	sink := MakeFileSSTSink(conf, store, nil /* pacer */)
	return sink, store
}

func sinkTestSetup(
	t *testing.T, settings *cluster.Settings, elideMode execinfrapb.ElidePrefix,
) (SSTSinkConf, cloud.ExternalStorage) {
	store := nodelocal.TestingMakeNodelocalStorage(t.TempDir(), settings, cloudpb.ExternalStorage{})
	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress, 100)
	sinkConf := SSTSinkConf{
		ID:        1,
		Enc:       nil,
		ProgCh:    progCh,
		Settings:  &settings.SV,
		ElideMode: elideMode,
	}
	return sinkConf, store
}

func checkFiles(
	ctx context.Context,
	store cloud.ExternalStorage,
	files []backuppb.BackupManifest_File,
	expectedFileSpans []roachpb.Spans,
	eliding bool,
) error {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}

	var filePaths []string
	filePathToSpans := make(map[string]roachpb.Spans)
	for _, f := range files {
		if _, ok := filePathToSpans[f.Path]; !ok {
			filePaths = append(filePaths, f.Path)
		}
		filePathToSpans[f.Path] = append(filePathToSpans[f.Path], f.Span)
	}

	// First, check that we got the expected file spans.
	if len(expectedFileSpans) != len(filePaths) {
		return errors.Newf("expected %d files, got %d", len(expectedFileSpans), len(filePaths))
	}

	for i := range expectedFileSpans {
		actualSpans := filePathToSpans[filePaths[i]]

		if !reflect.DeepEqual(expectedFileSpans[i], actualSpans) {
			return errors.Newf("expected file at idx %d to have spans %v, got %v", i, expectedFileSpans[i], actualSpans)
		}
	}

	// Also check that all keys within the flushed files fall within the
	// manifest file metadata spans that point to the file.
	for f, spans := range filePathToSpans {
		iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{{Store: store, FilePath: f}}, nil, iterOpts)
		if err != nil {
			return err
		}

		defer iter.Close()
		for iter.SeekGE(storage.MVCCKey{}); ; iter.Next() {
			if ok, err := iter.Valid(); err != nil {
				return err
			} else if !ok {
				break
			}

			key := iter.UnsafeKey()

			if !endKeyInclusiveSpansContainsKey(spans, key.Key, eliding) {
				return errors.Newf("key %v in file %s not contained by its spans [%v]", key.Key, f, spans)
			}
		}

	}

	return nil
}

func endKeyInclusiveSpansContainsKey(spans roachpb.Spans, key roachpb.Key, eliding bool) bool {
	for _, sp := range spans {
		if eliding {
			sp.Key, _ = keys.StripTablePrefix(sp.Key)
			sp.EndKey, _ = keys.StripTablePrefix(sp.EndKey)
		}
		if sp.ContainsKey(key) {
			return true
		}
		if sp.EndKey.Compare(key) == 0 {
			return true
		}
	}

	return false
}
