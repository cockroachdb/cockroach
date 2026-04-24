// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// putVal is a shorthand for writing a value at a given timestamp.
func putVal(t *testing.T, eng storage.Engine, key string, ts int64, val string) {
	t.Helper()
	_, err := storage.MVCCPut(
		context.Background(), eng, roachpb.Key(key),
		hlc.Timestamp{WallTime: ts},
		roachpb.MakeValueFromString(val),
		storage.MVCCWriteOptions{},
	)
	require.NoError(t, err)
}

// delKey is a shorthand for writing a tombstone at a given timestamp.
func delKey(t *testing.T, eng storage.Engine, key string, ts int64) {
	t.Helper()
	_, _, err := storage.MVCCDelete(
		context.Background(), eng, roachpb.Key(key),
		hlc.Timestamp{WallTime: ts}, storage.MVCCWriteOptions{},
	)
	require.NoError(t, err)
}

// evalGetTxnDetails calls GetTxnDetails with the given write spans and
// commit timestamp, using a range descriptor that spans [startKey, endKey).
func evalGetTxnDetails(
	t *testing.T,
	eng storage.Engine,
	rangeStart, rangeEnd string,
	commitTS int64,
	writeSpans []roachpb.Span,
) *kvpb.GetTxnDetailsResponse {
	t.Helper()
	resp := &kvpb.GetTxnDetailsResponse{}
	_, err := GetTxnDetails(context.Background(), eng, CommandArgs{
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
			Desc: &roachpb.RangeDescriptor{
				StartKey: roachpb.RKey(rangeStart),
				EndKey:   roachpb.RKey(rangeEnd),
			},
		}).EvalContext(),
		Args: &kvpb.GetTxnDetailsRequest{
			CommitTimestamp: hlc.Timestamp{WallTime: commitTS},
			WriteSpans:      writeSpans,
		},
	}, resp)
	require.NoError(t, err)
	return resp
}

func mkSpan(start, end string) roachpb.Span {
	s := roachpb.Span{Key: roachpb.Key(start)}
	if end != "" {
		s.EndKey = roachpb.Key(end)
	}
	return s
}

func TestCollectWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name       string
		setup      func(t *testing.T, eng storage.Engine)
		writeSpans []roachpb.Span
		commitTS   int64

		expectedKeys     []string
		expectedValues   []string // empty string = tombstone
		expectedPrevVals []string // empty string = no prev value
	}{
		{
			name: "new key, no previous value",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 10, "new")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{"new"},
			expectedPrevVals: []string{""},
		},
		{
			name: "point key span",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 5, "old")
				putVal(t, eng, "a", 10, "new")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{"new"},
			expectedPrevVals: []string{"old"},
		},
		{
			name: "overwrite with previous value",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 5, "old")
				putVal(t, eng, "a", 10, "new")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{"new"},
			expectedPrevVals: []string{"old"},
		},
		{
			name: "many prior versions returns immediate predecessor",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 3, "v1")
				putVal(t, eng, "a", 5, "v2")
				putVal(t, eng, "a", 7, "v3")
				putVal(t, eng, "a", 10, "current")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{"current"},
			expectedPrevVals: []string{"v3"},
		},
		{
			name: "tombstone with previous value",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 5, "doomed")
				delKey(t, eng, "a", 10)
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{""},
			expectedPrevVals: []string{"doomed"},
		},
		{
			name: "tombstone with no previous value",
			setup: func(t *testing.T, eng storage.Engine) {
				delKey(t, eng, "a", 10)
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{""},
			expectedPrevVals: []string{""},
		},
		{
			name: "multiple keys in one span",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 10, "val-a")
				putVal(t, eng, "b", 10, "val-b")
				putVal(t, eng, "c", 10, "val-c")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "d")},
			commitTS:         10,
			expectedKeys:     []string{"a", "b", "c"},
			expectedValues:   []string{"val-a", "val-b", "val-c"},
			expectedPrevVals: []string{"", "", ""},
		},
		{
			name: "multiple write spans",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 10, "val-a")
				putVal(t, eng, "e", 10, "val-e")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b"), mkSpan("e", "f")},
			commitTS:         10,
			expectedKeys:     []string{"a", "e"},
			expectedValues:   []string{"val-a", "val-e"},
			expectedPrevVals: []string{"", ""},
		},
		{
			name: "keys at other timestamps are ignored",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 5, "before")
				putVal(t, eng, "b", 10, "at-commit")
				putVal(t, eng, "c", 15, "after")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "d")},
			commitTS:         10,
			expectedKeys:     []string{"b"},
			expectedValues:   []string{"at-commit"},
			expectedPrevVals: []string{""},
		},
		{
			name: "prev value skips intermediate tombstone",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 3, "original")
				delKey(t, eng, "a", 5)
				putVal(t, eng, "a", 10, "resurrected")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "b")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{"resurrected"},
			expectedPrevVals: []string{""},
		},
		{
			name: "adjacent keys where NextIgnoringTime lands on different key",
			setup: func(t *testing.T, eng storage.Engine) {
				// "a" has no previous value; NextIgnoringTime will land on
				// "b"@10 which is a different key.
				putVal(t, eng, "a", 10, "val-a")
				putVal(t, eng, "b", 10, "val-b")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "c")},
			commitTS:         10,
			expectedKeys:     []string{"a", "b"},
			expectedValues:   []string{"val-a", "val-b"},
			expectedPrevVals: []string{"", ""},
		},
		{
			name: "key with prior values followed by adjacent key at commitTS",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 5, "old-a")
				putVal(t, eng, "a", 10, "new-a")
				putVal(t, eng, "b", 10, "val-b")
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "c")},
			commitTS:         10,
			expectedKeys:     []string{"a", "b"},
			expectedValues:   []string{"new-a", "val-b"},
			expectedPrevVals: []string{"old-a", ""},
		},
		{
			name: "write span clipped to range boundary",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "a", 10, "in-range")
				putVal(t, eng, "m", 10, "out-of-range")
			},
			// Write span extends beyond range [a, g), but should be clipped.
			writeSpans:       []roachpb.Span{mkSpan("a", "z")},
			commitTS:         10,
			expectedKeys:     []string{"a"},
			expectedValues:   []string{"in-range"},
			expectedPrevVals: []string{""},
		},
		{
			name: "write span entirely outside range",
			setup: func(t *testing.T, eng storage.Engine) {
				putVal(t, eng, "x", 10, "outside")
			},
			writeSpans:       []roachpb.Span{mkSpan("x", "z")},
			commitTS:         10,
			expectedKeys:     nil,
			expectedValues:   nil,
			expectedPrevVals: nil,
		},
		{
			name: "empty span returns no writes",
			setup: func(t *testing.T, eng storage.Engine) {
			},
			writeSpans:       []roachpb.Span{mkSpan("a", "z")},
			commitTS:         10,
			expectedKeys:     nil,
			expectedValues:   nil,
			expectedPrevVals: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			tc.setup(t, eng)

			resp := evalGetTxnDetails(t, eng, "a", "g", tc.commitTS, tc.writeSpans)

			require.Len(t, resp.Writes, len(tc.expectedKeys),
				"wrong number of writes")

			for i, w := range resp.Writes {
				require.Equal(t, tc.expectedKeys[i], string(w.KeyValue.Key),
					"wrong key at index %d", i)

				if tc.expectedValues[i] == "" {
					require.Len(t, w.KeyValue.Value.RawBytes, 0,
						"expected tombstone at index %d", i)
				} else {
					v, err := w.KeyValue.Value.GetBytes()
					require.NoError(t, err)
					require.Equal(t, tc.expectedValues[i], string(v),
						"wrong value at index %d", i)
				}

				if tc.expectedPrevVals[i] == "" {
					require.False(t, w.PrevValue.IsPresent(),
						"expected no prev_value at index %d", i)
				} else {
					require.True(t, w.PrevValue.IsPresent(),
						"expected prev_value at index %d", i)
					pv, err := w.PrevValue.GetBytes()
					require.NoError(t, err)
					require.Equal(t, tc.expectedPrevVals[i], string(pv),
						"wrong prev_value at index %d", i)
				}
			}
		})
	}
}

func ts(wall int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wall}
}

// evalGetTxnDetailsWithDeps calls GetTxnDetails with read spans and a
// CommitIndex, returning the full response including dependencies.
func evalGetTxnDetailsWithDeps(
	t *testing.T,
	eng storage.Engine,
	rangeStart, rangeEnd string,
	selfTxnID uuid.UUID,
	commitTS, depCutoff int64,
	readSpans []roachpb.Span,
	commitIndex *txnfeed.CommitIndex,
) *kvpb.GetTxnDetailsResponse {
	t.Helper()
	resp := &kvpb.GetTxnDetailsResponse{}
	_, err := GetTxnDetails(context.Background(), eng, CommandArgs{
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
			Desc: &roachpb.RangeDescriptor{
				StartKey: roachpb.RKey(rangeStart),
				EndKey:   roachpb.RKey(rangeEnd),
			},
			CommitIndex: commitIndex,
		}).EvalContext(),
		Args: &kvpb.GetTxnDetailsRequest{
			TxnID:            selfTxnID,
			CommitTimestamp:  ts(commitTS),
			DependencyCutoff: ts(depCutoff),
			ReadSpans:        readSpans,
		},
	}, resp)
	require.NoError(t, err)
	return resp
}

func TestCollectDependencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	selfID := uuid.MakeV4()
	writerA := uuid.MakeV4()
	writerB := uuid.MakeV4()

	t.Run("single read key finds writer", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Writer A wrote key "a" at ts=5.
		putVal(t, eng, "a", 5, "val-a")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(5), writerA)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Len(t, resp.Dependencies, 1)
		require.Equal(t, writerA, resp.Dependencies[0])
		require.Equal(t, ts(1), resp.EventHorizon)
	})

	t.Run("self txn excluded from dependencies", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		putVal(t, eng, "a", 5, "val-a")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(5), selfID)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(1), resp.EventHorizon)
	})

	t.Run("version below dependency cutoff not tracked", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Writer A wrote key "a" at ts=3, but cutoff is ts=5.
		putVal(t, eng, "a", 3, "old")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(3), writerA)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 5,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(5), resp.EventHorizon)
	})

	t.Run("commit index miss sets event horizon", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		putVal(t, eng, "a", 5, "val-a")

		// Empty commit index — no entries.
		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(5), resp.EventHorizon)
	})

	t.Run("nil commit index sets event horizon to commit timestamp", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		putVal(t, eng, "a", 5, "val-a")

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, nil)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(10), resp.EventHorizon)
	})

	t.Run("multiple read spans with different writers", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		putVal(t, eng, "a", 5, "val-a")
		putVal(t, eng, "c", 7, "val-c")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(5), writerA)
		idx.Record(ts(7), writerB)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b"), mkSpan("c", "d")}, idx)

		require.Len(t, resp.Dependencies, 2)
		depSet := make(map[uuid.UUID]struct{})
		for _, d := range resp.Dependencies {
			depSet[d] = struct{}{}
		}
		require.Contains(t, depSet, writerA)
		require.Contains(t, depSet, writerB)
		require.Equal(t, ts(1), resp.EventHorizon)
	})

	t.Run("same writer appears once even across multiple keys", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		putVal(t, eng, "a", 5, "val-a")
		putVal(t, eng, "b", 5, "val-b")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(5), writerA)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "c")}, idx)

		require.Len(t, resp.Dependencies, 1)
		require.Equal(t, writerA, resp.Dependencies[0])
	})

	t.Run("tombstone version is not a dependency", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		delKey(t, eng, "a", 5)

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(5), writerA)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(1), resp.EventHorizon)
	})

	t.Run("latest version in window used not older version", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Writer A wrote at ts=3, Writer B overwrote at ts=7. Both in window.
		// Transaction reads at commitTS=10, sees version at ts=7 (writerB).
		putVal(t, eng, "a", 3, "old")
		putVal(t, eng, "a", 7, "new")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(3), writerA)
		idx.Record(ts(7), writerB)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Len(t, resp.Dependencies, 1)
		require.Equal(t, writerB, resp.Dependencies[0])
	})

	t.Run("own write at commitTS looks through to prior version", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Writer A wrote key "a" at ts=5. Self overwrote it at ts=10
		// (commitTS). The version at commitTS is our own write; the
		// dependency is the version we read before overwriting (ts=5).
		putVal(t, eng, "a", 5, "old")
		putVal(t, eng, "a", 10, "new")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(5), writerA)
		idx.Record(ts(10), selfID)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Len(t, resp.Dependencies, 1)
		require.Equal(t, writerA, resp.Dependencies[0])
		require.Equal(t, ts(1), resp.EventHorizon)
	})

	t.Run("own write at commitTS with no prior version", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Self wrote key "a" at ts=10 (commitTS) with no prior version.
		// No dependency should be added.
		putVal(t, eng, "a", 10, "new")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(10), selfID)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(1), resp.EventHorizon)
	})

	t.Run("own write at commitTS with prior below cutoff", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Writer A wrote at ts=2, self overwrote at ts=10. Cutoff is
		// ts=5, so the prior version at ts=2 is below the cutoff and
		// should not produce a dependency.
		putVal(t, eng, "a", 2, "old")
		putVal(t, eng, "a", 10, "new")

		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(2), writerA)
		idx.Record(ts(10), selfID)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 5,
			[]roachpb.Span{mkSpan("a", "b")}, idx)

		require.Empty(t, resp.Dependencies)
		require.Equal(t, ts(5), resp.EventHorizon)
	})

	t.Run("event horizon tracks worst miss", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		putVal(t, eng, "a", 3, "val-a")
		putVal(t, eng, "b", 7, "val-b")

		// CommitIndex only knows about ts=3 (writerA), not ts=7.
		idx, err := txnfeed.NewCommitIndex()
		require.NoError(t, err)
		idx.Record(ts(3), writerA)

		resp := evalGetTxnDetailsWithDeps(
			t, eng, "a", "z", selfID, 10, 1,
			[]roachpb.Span{mkSpan("a", "c")}, idx)

		require.Len(t, resp.Dependencies, 1)
		require.Equal(t, writerA, resp.Dependencies[0])
		// event_horizon should be ts=7 (the missed timestamp).
		require.Equal(t, ts(7), resp.EventHorizon)
	})
}
