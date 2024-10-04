// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// TestFileSSTSinkExtendOneFile is a regression test for a bug in fileSSTSink in
// which the sink fails to extend its last span added if there's only one file
// in the sink so far.
func TestFileSSTSinkExtendOneFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, _, cleanup := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanup()

	getKeys := func(prefix string, n int) []byte {
		var b bytes.Buffer
		sst := storage.MakeBackupSSTWriter(ctx, nil, &b)
		for i := 0; i < n; i++ {
			require.NoError(t, sst.PutUnversioned([]byte(fmt.Sprintf("%s%08d", prefix, i)), nil))
		}
		sst.Close()
		return b.Bytes()
	}

	exportResponse1 := exportedSpan{
		metadata: backuppb.BackupManifest_File{
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
		dataSST:        getKeys("b", 100),
		revStart:       hlc.Timestamp{},
		completedSpans: 1,
		atKeyBoundary:  false,
	}

	exportResponse2 := exportedSpan{
		metadata: backuppb.BackupManifest_File{
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
		dataSST:        getKeys("c", 100),
		revStart:       hlc.Timestamp{},
		completedSpans: 1,
		atKeyBoundary:  true,
	}

	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '20B'`)

	sink, _ := fileSSTSinkTestSetUp(ctx, t, tc, sqlDB)

	require.NoError(t, sink.write(ctx, exportResponse1))
	require.NoError(t, sink.write(ctx, exportResponse2))

	close(sink.conf.progCh)

	var progs []execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	for p := range sink.conf.progCh {
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
// writes a sequence of exportedSpans into a fileSSTSink. The test then verifies
// the spans of the flushed files and the unflushed files still left in the
// sink, as well as makes sure all keys in each file fall within the span
// boundaries.
func TestFileSSTSinkWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, _, cleanup := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '10KB'`)

	type testCase struct {
		name           string
		exportSpans    []exportedSpan
		flushedSpans   []roachpb.Spans
		unflushedSpans []roachpb.Spans
		skipReason     string
	}

	for _, tt := range []testCase{
		{
			name: "out-of-order-key-boundary",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", true).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "c", timestamp: 10}}).build(),
				newExportedSpanBuilder("b", "d", true).withKVs([]kvAndTS{{key: "b", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{{roachpb.Span{Key: []byte("a"), EndKey: []byte("c")}}},
			unflushedSpans: []roachpb.Spans{{roachpb.Span{Key: []byte("b"), EndKey: []byte("d")}}},
		},
		{
			// Test that even if the most recently ingested export span does not
			// end at a key boundary, a flush will still occur on the writing of
			// an out-of-order export span.
			name: "out-of-order-not-key-boundary",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", false).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "c", timestamp: 10}}).build(),
				newExportedSpanBuilder("b", "d", true).withKVs([]kvAndTS{{key: "b", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e", true).withKVs([]kvAndTS{{key: "c", timestamp: 9}, {key: "e", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{roachpb.Span{Key: []byte("a"), EndKey: []byte("c")}},
				{roachpb.Span{Key: []byte("b"), EndKey: []byte("d")}},
			},
			unflushedSpans: []roachpb.Spans{{roachpb.Span{Key: []byte("c"), EndKey: []byte("e")}}},
		},
		{
			name: "extend-key-boundary-1-file",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", true).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e", true).withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: []byte("a"), EndKey: []byte("e")}}},
		},
		{
			name: "extend-key-boundary-2-files",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", true).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e", true).withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).build(),
				newExportedSpanBuilder("e", "g", true).withKVs([]kvAndTS{{key: "e", timestamp: 10}, {key: "f", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: []byte("a"), EndKey: []byte("g")}}},
		},
		{
			name: "extend-not-key-boundary",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", false).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "c", timestamp: 10}}).build(),
				newExportedSpanBuilder("c", "e", true).withKVs([]kvAndTS{{key: "c", timestamp: 9}, {key: "d", timestamp: 10}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: []byte("a"), EndKey: []byte("e")}}},
		},
		{
			// TODO(rui): currently it is possible to make the sink error if we
			// write different times of the revision history for the same key
			// out of order.
			// Issue: https://github.com/cockroachdb/cockroach/issues/105372
			name:       "extend-same-key",
			skipReason: "incorrectly fails with pebble: keys must be added in strictly increasing order",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "a", false).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "a", timestamp: 9}}).build(),
				newExportedSpanBuilder("a", "a", false).withKVs([]kvAndTS{{key: "a", timestamp: 5}, {key: "a", timestamp: 4}}).build(),
				newExportedSpanBuilder("a", "a", false).withKVs([]kvAndTS{{key: "a", timestamp: 8}, {key: "a", timestamp: 7}}).build(),
			},
			flushedSpans:   []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{{{Key: []byte("a"), EndKey: []byte("e")}}},
		},
		{
			name: "extend-metadata-same-timestamp",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", true).
					withKVs([]kvAndTS{{key: "a", timestamp: 5}, {key: "b", timestamp: 5}}).
					withStartTime(5).
					withEndTime(10).
					build(),
				newExportedSpanBuilder("c", "e", true).
					withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).
					withStartTime(5).
					withEndTime(10).
					build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: []byte("a"), EndKey: []byte("e")}},
			},
		},
		{
			name: "no-extend-metadata-timestamp-mismatch",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", true).
					withKVs([]kvAndTS{{key: "a", timestamp: 5}, {key: "b", timestamp: 5}}).
					withEndTime(5).
					build(),
				newExportedSpanBuilder("c", "e", true).
					withKVs([]kvAndTS{{key: "c", timestamp: 10}, {key: "d", timestamp: 10}}).
					withStartTime(5).
					withEndTime(10).
					build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: []byte("a"), EndKey: []byte("c")}, {Key: []byte("c"), EndKey: []byte("e")}},
			},
		},
		{
			name: "size-flush",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", true).withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 20<<20)}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("d", "f", true).withKVs([]kvAndTS{{key: "d", timestamp: 10}, {key: "e", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: []byte("a"), EndKey: []byte("c")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: []byte("d"), EndKey: []byte("f")}},
			},
		},
		{
			name: "no-size-flush-if-not-at-boundary",
			exportSpans: []exportedSpan{
				newExportedSpanBuilder("a", "c", false).withKVs([]kvAndTS{{key: "a", timestamp: 10, value: make([]byte, 20<<20)}, {key: "b", timestamp: 10}}).build(),
				newExportedSpanBuilder("d", "f", false).withKVs([]kvAndTS{{key: "d", timestamp: 10}, {key: "e", timestamp: 10}}).build(),
			},
			flushedSpans: []roachpb.Spans{},
			unflushedSpans: []roachpb.Spans{
				{{Key: []byte("a"), EndKey: []byte("c")}, {Key: []byte("d"), EndKey: []byte("f")}},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipReason != "" {
				skip.IgnoreLint(t, tt.skipReason)
			}

			sink, store := fileSSTSinkTestSetUp(ctx, t, tc, sqlDB)
			defer func() {
				require.NoError(t, sink.Close())
			}()

			for _, es := range tt.exportSpans {
				require.NoError(t, sink.write(ctx, es))
			}

			progress := make([]backuppb.BackupManifest_File, 0)

		Loop:
			for {
				select {
				case p := <-sink.conf.progCh:
					var progDetails backuppb.BackupManifest_Progress
					if err := types.UnmarshalAny(&p.ProgressDetails, &progDetails); err != nil {
						t.Fatal(err)
					}

					progress = append(progress, progDetails.Files...)
				default:
					break Loop
				}
			}

			// progCh contains the files that have already been created with
			// flushes. Verify the contents.
			require.NoError(t, checkFiles(ctx, store, progress, tt.flushedSpans))

			// flushedFiles contain the files that are in queue to be created on the
			// next flush. Save these and then flush the sink to check their contents.
			var actualUnflushedFiles []backuppb.BackupManifest_File
			actualUnflushedFiles = append(actualUnflushedFiles, sink.flushedFiles...)
			require.NoError(t, sink.flush(ctx))
			require.NoError(t, checkFiles(ctx, store, actualUnflushedFiles, tt.unflushedSpans))
			require.Empty(t, sink.flushedFiles)
		})
	}

}

// TestFileSSTSinkStats tests the internal counters and stats of the FileSSTSink under
// different write scenarios.
func TestFileSSTSinkStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, _, cleanup := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '10KB'`)

	sink, _ := fileSSTSinkTestSetUp(ctx, t, tc, sqlDB)

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
		input         exportedSpan
		expectedStats sinkStats
	}

	//  no extends, extends, flush due to size, flush out of order
	inputs := []inputAndExpectedStats{
		{
			// Write the first exported span to the sink.
			newExportedSpanBuilder("a", "c", true).withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10}}).build(),
			sinkStats{hlc.Timestamp{}, 1, 1, 0, 0, 0, 0},
		},
		{
			// Write another exported span after the first span that doesn't
			// extend the previous span. This ES also has a revStartTime.
			newExportedSpanBuilder("d", "e", true).withKVs([]kvAndTS{{key: "d", timestamp: 10}}).withRevStartTime(5).build(),
			sinkStats{hlc.Timestamp{WallTime: 5}, 2, 2, 0, 0, 0, 0}},
		{
			// Write an exported span that extends the previous span. This ES
			// also has a later revStartTime.
			newExportedSpanBuilder("e", "f", true).withKVs([]kvAndTS{{key: "e", timestamp: 10}}).withRevStartTime(10).build(),
			sinkStats{hlc.Timestamp{WallTime: 10}, 3, 3, 0, 0, 0, 1}},
		{
			// Write an exported span that comes after all spans so far. This span has enough data for a size flush.
			newExportedSpanBuilder("g", "h", true).withKVs([]kvAndTS{{key: "g", timestamp: 10, value: make([]byte, 20<<20)}}).build(),
			sinkStats{hlc.Timestamp{WallTime: 0}, 0, 4, 1, 0, 1, 1}},
		{
			// Write the first exported span after the flush.
			newExportedSpanBuilder("i", "k", true).withKVs([]kvAndTS{{key: "i", timestamp: 10}, {key: "j", timestamp: 10}}).build(),
			sinkStats{hlc.Timestamp{}, 1, 5, 1, 0, 1, 1}},
		{
			// Write another exported span that causes an out of order flush.
			newExportedSpanBuilder("j", "l", true).withKVs([]kvAndTS{{key: "j", timestamp: 10}, {key: "k", timestamp: 10}}).build(),
			sinkStats{hlc.Timestamp{}, 1, 6, 2, 1, 1, 1}},
	}

	for _, input := range inputs {
		require.NoError(t, sink.write(ctx, input.input))

		actualStats := sinkStats{
			flushedRevStart: sink.flushedRevStart,
			completedSpans:  sink.completedSpans,
			files:           sink.stats.files,
			flushes:         sink.stats.flushes,
			oooFlushes:      sink.stats.oooFlushes,
			sizeFlushes:     sink.stats.sizeFlushes,
			spanGrows:       sink.stats.spanGrows,
		}

		require.Equal(t, input.expectedStats, actualStats, "stats after write for span %v", input.input.metadata.Span)
	}

	require.NoError(t, sink.flush(ctx))
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
			sst := storage.MakeBackupSSTWriter(ctx, settings, buf)
			sink := fileSSTSink{sst: sst}
			compareSST := true

			for _, input := range tt.inputs {
				kvs := input.input
				// Add a range key in the input as well.
				es := newExportedSpanBuilder(kvs[0].key, kvs[len(kvs)-1].key, false).
					withKVs(kvs).
					withRangeKeys([]rangeKeyAndTS{{"a", "z", 10}}).
					build()
				err := sink.copyPointKeys(es.dataSST)
				if input.expectErr != "" {
					// Do not compare resulting SSTs if we expect errors.
					require.ErrorContains(t, err, input.expectErr)
					compareSST = false
				} else {
					require.NoError(t, err)
				}
			}

			sst.Close()

			if !compareSST {
				return
			}

			var expected []kvAndTS
			for _, input := range tt.inputs {
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
		input     []rangeKeyAndTS
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
					input: []rangeKeyAndTS{
						{key: "a", endKey: "b", timestamp: 10},
						{key: "b", endKey: "c", timestamp: 9},
					},
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
				},
				{
					input: []rangeKeyAndTS{
						{key: "c", endKey: "d", timestamp: 10},
						{key: "c", endKey: "d", timestamp: 9},
					},
				},
				{
					input: []rangeKeyAndTS{
						{key: "c", endKey: "d", timestamp: 8},
					},
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
				},
				{
					input: []rangeKeyAndTS{
						{key: "b", endKey: "d", timestamp: 11},
						{key: "c", endKey: "e", timestamp: 7},
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			sst := storage.MakeBackupSSTWriter(ctx, settings, buf)
			sink := fileSSTSink{sst: sst}
			compareSST := true

			for _, input := range tt.inputs {
				rangeKeys := input.input
				// Add some point key values in the input as well.
				es := newExportedSpanBuilder(rangeKeys[0].key, rangeKeys[len(rangeKeys)-1].key, false).
					withRangeKeys(rangeKeys).
					withKVs([]kvAndTS{{rangeKeys[0].key, nil, rangeKeys[0].timestamp}}).
					build()
				err := sink.copyRangeKeys(es.dataSST)
				if input.expectErr != "" {
					// Do not compare resulting SSTs if we expect errors.
					require.ErrorContains(t, err, input.expectErr)
					compareSST = false
				} else {
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

type rangeKeyAndTS struct {
	key       string
	endKey    string
	timestamp int64
}

func (rk rangeKeyAndTS) span() roachpb.Span {
	return roachpb.Span{
		Key:    []byte(rk.key),
		EndKey: []byte(rk.endKey),
	}
}

type exportedSpanBuilder struct {
	es        *exportedSpan
	keyValues []kvAndTS
	rangeKeys []rangeKeyAndTS
}

func newExportedSpanBuilder(spanStart, spanEnd string, atKeyBoundary bool) *exportedSpanBuilder {
	return &exportedSpanBuilder{
		es: &exportedSpan{
			metadata: backuppb.BackupManifest_File{
				Span: roachpb.Span{
					Key:    []byte(spanStart),
					EndKey: []byte(spanEnd),
				},
				EntryCounts: roachpb.RowCount{
					DataSize:     1,
					Rows:         1,
					IndexEntries: 0,
				},
			},

			completedSpans: 1,
			atKeyBoundary:  atKeyBoundary,
		},
	}
}

func (b *exportedSpanBuilder) withKVs(keyValues []kvAndTS) *exportedSpanBuilder {
	b.keyValues = keyValues
	return b
}

func (b *exportedSpanBuilder) withRangeKeys(rangeKeys []rangeKeyAndTS) *exportedSpanBuilder {
	b.rangeKeys = rangeKeys
	return b
}

func (b *exportedSpanBuilder) withStartTime(time int64) *exportedSpanBuilder {
	b.es.metadata.StartTime = hlc.Timestamp{WallTime: time}
	return b
}

func (b *exportedSpanBuilder) withEndTime(time int64) *exportedSpanBuilder {
	b.es.metadata.EndTime = hlc.Timestamp{WallTime: time}
	return b
}

func (b *exportedSpanBuilder) withRevStartTime(time int64) *exportedSpanBuilder {
	b.es.revStart = hlc.Timestamp{WallTime: time}
	return b
}

func (b *exportedSpanBuilder) build() exportedSpan {
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	buf := &bytes.Buffer{}
	sst := storage.MakeBackupSSTWriter(ctx, settings, buf)
	for _, d := range b.keyValues {
		err := sst.Put(storage.MVCCKey{
			Key:       []byte(d.key),
			Timestamp: hlc.Timestamp{WallTime: d.timestamp},
		}, d.value)
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

	b.es.dataSST = buf.Bytes()

	return *b.es
}

func fileSSTSinkTestSetUp(
	ctx context.Context, t *testing.T, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner,
) (*fileSSTSink, cloud.ExternalStorage) {
	store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///0",
		base.ExternalIODirConfig{},
		tc.Servers[0].ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		tc.Servers[0].InternalDB().(isql.DB),
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)

	// Never block.
	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress, 100)

	sinkConf := sstSinkConf{
		id:       1,
		enc:      nil,
		progCh:   progCh,
		settings: &tc.Servers[0].ClusterSettings().SV,
	}

	sink := makeFileSSTSink(sinkConf, store)
	return sink, store
}

func checkFiles(
	ctx context.Context,
	store cloud.ExternalStorage,
	files []backuppb.BackupManifest_File,
	expectedFileSpans []roachpb.Spans,
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

			if !endKeyInclusiveSpansContainsKey(spans, key.Key) {
				return errors.Newf("key %v in file %s not contained by its spans [%v]", key.Key, f, spans)
			}
		}

	}

	return nil
}

func endKeyInclusiveSpansContainsKey(spans roachpb.Spans, key roachpb.Key) bool {
	for _, sp := range spans {
		if sp.ContainsKey(key) {
			return true
		}
		if sp.EndKey.Compare(key) == 0 {
			return true
		}
	}

	return false
}
