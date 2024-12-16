// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestFileSSTSinkWriteKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// Artificially set file size limits for testing.
	defer testutils.HookGlobal(&fileSpanByteLimit, 8<<10)()
	targetFileSize.Override(ctx, &st.SV, 24<<10)

	gtFileSizeVal := make([]byte, 10<<10)
	gtSSTSizeVal := make([]byte, 32<<10)

	type testCase struct {
		name      string
		exportKVs []*mvccKVSet
		// spans of files that should have been flushed during WriteKey and before
		// the final manual flush.
		flushedSpans []roachpb.Spans
		// any spans of files that will be flushed out by the manual flush.
		unflushedSpans []roachpb.Spans
		// If test requires specific elide modes only -- nil to use default elide modes.
		elideModes []execinfrapb.ElidePrefix
	}

	for _, tt := range []testCase{
		{
			name: "single-span",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{
					{key: "a", timestamp: 10}, {key: "b", timestamp: 10},
				}),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
			},
		},
		{
			name: "single-span-size-flush-last-key",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{
					{key: "a", timestamp: 10}, {key: "b", value: gtSSTSizeVal, timestamp: 15},
				}),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
			},
		},
		// Ensure that if the size is exceeded mid-span, a flush occurs.
		{
			name: "single-span-size-flush-mid-span",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{
					{key: "a", value: gtSSTSizeVal, timestamp: 15}, {key: "b", timestamp: 15}}),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("b")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("b"), EndKey: s2k0("c")}},
			},
		},
		{
			name: "double-size-flush-single-span",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "e").withKVs(
					[]kvAndTS{
						{key: "a", timestamp: 10}, {key: "b", value: gtSSTSizeVal, timestamp: 15},
						{key: "c", value: gtSSTSizeVal, timestamp: 15}, {key: "d", timestamp: 10},
					},
				),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
				{{Key: s2k0("c"), EndKey: s2k0("d")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("d"), EndKey: s2k0("e")}},
			},
		},
		{
			name: "double-ooo-span-flush",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{{key: "a", timestamp: 10}}),
				newMVCCKeySet("b", "d").withKVs([]kvAndTS{{key: "b", timestamp: 10}}),
				newMVCCKeySet("c", "e").withKVs([]kvAndTS{{key: "c", timestamp: 10}}),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
				{{Key: s2k0("b"), EndKey: s2k0("d")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("c"), EndKey: s2k0("e")}},
			},
		},
		{
			name: "size-ooo-span-and-ooo-key-flush",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "d").
					withKVs([]kvAndTS{ // size
						{key: "a", value: gtSSTSizeVal, timestamp: 10},
						{key: "c", timestamp: 10},
					}),
				newMVCCKeySet("a", "d").
					withKVs([]kvAndTS{{key: "b", timestamp: 10}}),
				newMVCCKeySet("c", "f").
					withKVs([]kvAndTS{{key: "e", timestamp: 10}}),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
				{{Key: s2k0("c"), EndKey: s2k0("d")}},
				{{Key: s2k0("a"), EndKey: s2k0("d")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("c"), EndKey: s2k0("f")}},
			},
		},
		// Two spans that are contiguous with each other should be merged into one span.
		{
			name: "extend-metadata",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{
					{key: "a", timestamp: 10}, {key: "b", timestamp: 10},
				}),
				newMVCCKeySet("c", "e").withKVs([]kvAndTS{
					{key: "c", timestamp: 10}, {key: "d", timestamp: 10},
				}),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("e")}},
			},
		},
		// If a span overlaps its previous span, a flush must occur first.
		{
			name: "flush-from-overlapping-ooo-spans",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{
					{key: "a", timestamp: 10}, {key: "b", timestamp: 10},
				}),
				newMVCCKeySet("b", "e").withKVs([]kvAndTS{
					{key: "b", timestamp: 10}, {key: "c", timestamp: 10}, {key: "d", timestamp: 10},
				}),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("b"), EndKey: s2k0("e")}},
			},
		},
		// If a span precedes the previous span but does not overlap, a flush must
		// occur first.
		{
			name: "flush-from-non-overlapping-ooo-spans",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("c", "e").withKVs([]kvAndTS{
					{key: "c", timestamp: 10}, {key: "d", timestamp: 10},
				}),
				newMVCCKeySet("a", "c").withKVs([]kvAndTS{
					{key: "a", timestamp: 10}, {key: "b", timestamp: 10},
				}),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("c"), EndKey: s2k0("e")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
			},
		},
		// Writing keys with different prefixes should flush the previous span.
		{
			name: "prefixes-differ-with-eliding",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("2/a", "2/c").withKVs([]kvAndTS{{key: "2/a", timestamp: 10}, {key: "2/b", timestamp: 10}}),
				newMVCCKeySet("2/c", "2/e").withKVs([]kvAndTS{{key: "2/c", timestamp: 10}, {key: "2/d", timestamp: 10}}),
				newMVCCKeySet("3/e", "3/g").withKVs([]kvAndTS{{key: "3/e", timestamp: 10}, {key: "3/f", timestamp: 10}}),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("2/a"), EndKey: s2k0("2/e")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("3/e"), EndKey: s2k0("3/g")}},
			},
			elideModes: []execinfrapb.ElidePrefix{execinfrapb.ElidePrefix_TenantAndTable},
		},
		// In-order different prefixes shouldn't force a flush if no eliding is done.
		{
			name: "prefixes-differ-with-no-eliding",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("2/a", "2/c").withKVs([]kvAndTS{{key: "2/a", timestamp: 10}, {key: "2/b", timestamp: 10}}),
				newMVCCKeySet("2/c", "2/e").withKVs([]kvAndTS{{key: "2/c", timestamp: 10}, {key: "2/d", timestamp: 10}}),
				newMVCCKeySet("3/e", "3/g").withKVs([]kvAndTS{{key: "3/e", timestamp: 10}, {key: "3/f", timestamp: 10}}),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("2/a"), EndKey: s2k0("2/e")}, {Key: s2k0("3/e"), EndKey: s2k0("3/g")}},
			},
			elideModes: []execinfrapb.ElidePrefix{execinfrapb.ElidePrefix_None},
		},
		// Flush does not occur if last key written is mid-row even if size exceeded.
		{
			name: "no-size-flush-if-mid-row",
			exportKVs: []*mvccKVSet{
				newRawMVCCKeySet(s2k0("a"), s2kWithColFamily("b", 1)).
					withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10, value: gtSSTSizeVal}}).
					withStartEndTS(10, 15),
				newRawMVCCKeySet(s2kWithColFamily("b", 1), s2k0("d")).
					withRawKVs([]mvccKV{
						kvAndTS{key: "b", timestamp: 10}.toMvccKV(s2k1),
					}).
					withStartEndTS(10, 15),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("d")}},
			},
		},
		// If size flush is blocked by mid-row key, the next key should cause a flush.
		{
			name: "size-flush-postponed-till-after-mid-row",
			exportKVs: []*mvccKVSet{
				newRawMVCCKeySet(s2k0("a"), s2kWithColFamily("b", 1)).
					withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10, value: gtSSTSizeVal}}).
					withStartEndTS(10, 15),
				newRawMVCCKeySet(s2kWithColFamily("b", 1), s2k0("d")).
					withRawKVs([]mvccKV{
						kvAndTS{key: "b", timestamp: 10}.toMvccKV(s2k1),
						kvAndTS{key: "c", timestamp: 10}.toMvccKV(s2k0),
					}).
					withStartEndTS(10, 15),
			},
			flushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}},
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("c"), EndKey: s2k0("d")}},
			},
		},
		// It is safe to flush at the range boundary.
		{
			name: "size-flush-at-range-boundary",
			exportKVs: []*mvccKVSet{
				newRawMVCCKeySet(s2k0("a"), s2k("c")).
					withKVs([]kvAndTS{
						{key: "a", timestamp: 10},
						{key: "b", timestamp: 10, value: gtSSTSizeVal},
					}),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k("c")}},
			},
		},
		// If fileSpanByteLimit is reached, extending the file should be prevented
		// and a new file created.
		{
			name: "no-extend-due-to-manifest-size-limit",
			exportKVs: []*mvccKVSet{
				newMVCCKeySet("a", "d").
					withKVs([]kvAndTS{
						{key: "a", timestamp: 10},
						{key: "b", timestamp: 10, value: gtFileSizeVal},
						{key: "c", timestamp: 10},
					}),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}, {Key: s2k0("c"), EndKey: s2k0("d")}},
			},
		},
		// If fileSpanByteLimit is reached but the last key written was mid-row,
		// the file must be extended.
		{
			name: "extend-mid-row-despite-manifest-size-limit",
			exportKVs: []*mvccKVSet{
				newRawMVCCKeySet(s2k0("a"), s2kWithColFamily("b", 1)).
					withKVs([]kvAndTS{{key: "a", timestamp: 10}, {key: "b", timestamp: 10, value: gtFileSizeVal}}).
					withStartEndTS(10, 15),
				newRawMVCCKeySet(s2kWithColFamily("b", 1), s2k0("d")).
					withRawKVs([]mvccKV{
						kvAndTS{key: "b", timestamp: 10}.toMvccKV(s2k1),
						kvAndTS{key: "c", timestamp: 10}.toMvccKV(s2k0),
					}).
					withStartEndTS(10, 15),
			},
			unflushedSpans: []roachpb.Spans{
				{{Key: s2k0("a"), EndKey: s2k0("c")}, {Key: s2k0("c"), EndKey: s2k0("d")}},
			},
		},
		// fileSpanByteLimit reached in the middle of writing a span, but the
		// key is mid-row, so the file must be extended.
		{
			name: "extend-mid-row-despite-manifest-size-limit-same-span",
			exportKVs: []*mvccKVSet{
				newRawMVCCKeySet(s2k0("a"), s2k0("d")).withRawKVs([]mvccKV{
					kvAndTS{key: "a", timestamp: 10}.toMvccKV(s2k0),
					kvAndTS{key: "b", timestamp: 10, value: gtFileSizeVal}.toMvccKV(s2k0),
					kvAndTS{key: "b", timestamp: 10, value: gtFileSizeVal}.toMvccKV(s2k1),
					kvAndTS{key: "c", timestamp: 10}.toMvccKV(s2k0),
				}),
			},
			unflushedSpans: []roachpb.Spans{
				{
					{Key: s2k0("a"), EndKey: s2k0("c")},
					{Key: s2k0("c"), EndKey: s2k0("d")},
				},
			},
		},
	} {
		elideModes := []execinfrapb.ElidePrefix{execinfrapb.ElidePrefix_None, execinfrapb.ElidePrefix_TenantAndTable}
		if tt.elideModes != nil {
			elideModes = tt.elideModes
		}
		for _, elide := range elideModes {
			t.Run(fmt.Sprintf("%s/elide=%s", tt.name, elide), func(t *testing.T) {
				sink, store := sstSinkKeyWriterTestSetup(t, st, elide)
				defer func() {
					require.NoError(t, sink.Close())
				}()

				for _, ek := range tt.exportKVs {
					require.NoError(t, sink.Reset(ctx, ek.span))
					for _, kv := range ek.kvs {
						require.NoError(t, sink.WriteKey(ctx, kv.key, kv.value))
					}
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

				eliding := sink.conf.ElideMode != execinfrapb.ElidePrefix_None
				require.NoError(t, checkFiles(ctx, store, progress, tt.flushedSpans, eliding))

				var actualUnflushedFiles []backuppb.BackupManifest_File
				actualUnflushedFiles = append(actualUnflushedFiles, sink.flushedFiles...)
				require.NoError(t, sink.Flush(ctx))
				require.NoError(t, checkFiles(ctx, store, actualUnflushedFiles, tt.unflushedSpans, eliding))
				require.Empty(t, sink.flushedFiles)
			})
		}
	}
}

func TestFileSSTSinkWriteKeyOOOFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	sink, _ := sstSinkKeyWriterTestSetup(t, st, execinfrapb.ElidePrefix_None)
	defer func() {
		require.NoError(t, sink.Close())
	}()

	keySet := newMVCCKeySet("a", "e").withKVs([]kvAndTS{
		{key: "a", timestamp: 10}, {key: "b", timestamp: 10},
		{key: "d", timestamp: 10}, {key: "c", timestamp: 10},
	})

	require.NoError(t, sink.Reset(ctx, keySet.span))
	for idx, kv := range keySet.kvs {
		if idx < len(keySet.kvs)-1 {
			require.NoError(t, sink.WriteKey(ctx, kv.key, kv.value))
		} else {
			require.ErrorContains(
				t,
				sink.WriteKey(ctx, kv.key, kv.value),
				"must be greater than previous key",
			)
		}
	}
	require.NoError(t, sink.Flush(ctx))
}

func sstSinkKeyWriterTestSetup(
	t *testing.T, settings *cluster.Settings, elideMode execinfrapb.ElidePrefix,
) (*SSTSinkKeyWriter, cloud.ExternalStorage) {
	conf, store := sinkTestSetup(t, settings, elideMode)
	sink := MakeSSTSinkKeyWriter(conf, store, nil /* pacer */)
	return sink, store
}

type mvccKV struct {
	key   storage.MVCCKey
	value []byte
}

type mvccKVSet struct {
	span      roachpb.Span
	kvs       []mvccKV
	startTime hlc.Timestamp
	endTime   hlc.Timestamp
}

func newMVCCKeySet(spanStart string, spanEnd string) *mvccKVSet {
	return &mvccKVSet{
		span: roachpb.Span{
			Key:    s2k0(spanStart),
			EndKey: s2k0(spanEnd),
		},
	}
}

func newRawMVCCKeySet(spanStart roachpb.Key, spanEnd roachpb.Key) *mvccKVSet {
	return &mvccKVSet{
		span: roachpb.Span{
			Key:    spanStart,
			EndKey: spanEnd,
		},
	}
}

func (b *mvccKVSet) withStartEndTS(startTime, endTime int64) *mvccKVSet {
	b.startTime = hlc.Timestamp{WallTime: startTime}
	b.endTime = hlc.Timestamp{WallTime: endTime}
	return b
}

func (b *mvccKVSet) withKVs(kvs []kvAndTS) *mvccKVSet {
	return b.withKVsAndEncoding(kvs, s2k0)
}

func (b *mvccKVSet) withKVsAndEncoding(kvs []kvAndTS, enc func(string) roachpb.Key) *mvccKVSet {
	rawKVs := make([]mvccKV, 0, len(kvs))
	for _, kv := range kvs {
		v := roachpb.Value{}
		v.SetBytes(kv.value)
		v.InitChecksum(nil)
		rawKVs = append(rawKVs, mvccKV{
			key: storage.MVCCKey{
				Key:       enc(kv.key),
				Timestamp: hlc.Timestamp{WallTime: kv.timestamp},
			},
			value: v.RawBytes,
		})
	}
	return b.withRawKVs(rawKVs)
}

func (b *mvccKVSet) withRawKVs(kvs []mvccKV) *mvccKVSet {
	b.kvs = kvs
	var minTS, maxTS hlc.Timestamp
	for _, kv := range kvs {
		if minTS.IsEmpty() || kv.key.Timestamp.Less(minTS) {
			minTS = kv.key.Timestamp
		}
		if maxTS.IsEmpty() || maxTS.Less(kv.key.Timestamp) {
			maxTS = kv.key.Timestamp
		}
	}
	b.startTime = minTS
	b.endTime = maxTS
	return b
}
