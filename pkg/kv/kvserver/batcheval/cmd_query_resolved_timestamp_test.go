// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestQueryResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	makeTS := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: ts}
	}
	writeValue := func(k string, ts int64) {
		_, _, err := storage.MVCCDelete(ctx, db, roachpb.Key(k), makeTS(ts), storage.MVCCWriteOptions{})
		require.NoError(t, err)
	}
	writeIntent := func(k string, ts int64) {
		txn := roachpb.MakeTransaction("test", roachpb.Key(k), 0, 0, makeTS(ts), 0, 1, 0, false /* omitInRangefeeds */)
		_, _, err := storage.MVCCDelete(ctx, db, roachpb.Key(k), makeTS(ts), storage.MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
	}
	writeInline := func(k string) {
		_, _, err := storage.MVCCDelete(ctx, db, roachpb.Key(k), hlc.Timestamp{}, storage.MVCCWriteOptions{})
		require.NoError(t, err)
	}
	writeLock := func(k string, str lock.Strength) {
		txn := roachpb.MakeTransaction("test", roachpb.Key(k), 0, 0, makeTS(1), 0, 1, 0, false /* omitInRangefeeds */)
		err := storage.MVCCAcquireLock(ctx, db, &txn.TxnMeta, txn.IgnoredSeqNums, str, roachpb.Key(k), nil, 0, 0)
		require.NoError(t, err)
	}

	// Setup: (with separated intents the actual key layout in the store is not what is listed below.)
	//
	//  a: intent @ 5
	//  a: value  @ 3
	//  b: inline value
	//  c: shared lock #1
	//  c: shared lock #2
	//  c: value  @ 6
	//  c: value  @ 4
	//  c: value  @ 1
	//  d: intent @ 2
	//  e: intent @ 7
	//  f: inline value
	//  g: exclusive lock
	//
	// NB: must write each key in increasing timestamp order.
	writeValue("a", 3)
	writeIntent("a", 5)
	writeInline("b")
	writeValue("c", 1)
	writeValue("c", 4)
	writeLock("c", lock.Shared)
	writeLock("c", lock.Shared)
	writeIntent("d", 2)
	writeIntent("e", 7)
	writeInline("f")
	writeLock("g", lock.Exclusive)

	for _, cfg := range []struct {
		name                         string
		span                         [2]string
		closedTS                     hlc.Timestamp
		maxEncounteredIntents        int64         // default 10
		maxEncounteredIntentKeyBytes int64         // default 10
		intentCleanupAge             time.Duration // default 10ns

		expResolvedTS         hlc.Timestamp
		expEncounteredIntents []string
	}{
		{
			name:          "closed timestamp before earliest intent",
			span:          [2]string{"a", "z"},
			closedTS:      makeTS(1),
			expResolvedTS: makeTS(1),
		},
		{
			name:          "closed timestamp equal to earliest intent",
			span:          [2]string{"a", "z"},
			closedTS:      makeTS(2),
			expResolvedTS: makeTS(2).Prev(),
		},
		{
			name:          "closed timestamp after earliest intent",
			span:          [2]string{"a", "z"},
			closedTS:      makeTS(3),
			expResolvedTS: makeTS(2).Prev(),
		},
		{
			name:          "closed timestamp before earliest intent, partial overlap",
			span:          [2]string{"a", "c"},
			closedTS:      makeTS(4),
			expResolvedTS: makeTS(4),
		},
		{
			name:          "closed timestamp equal to earliest intent, partial overlap",
			span:          [2]string{"a", "c"},
			closedTS:      makeTS(5),
			expResolvedTS: makeTS(5).Prev(),
		},
		{
			name:          "closed timestamp after earliest intent, partial overlap",
			span:          [2]string{"a", "c"},
			closedTS:      makeTS(6),
			expResolvedTS: makeTS(5).Prev(),
		},
		{
			name:          "no intents",
			span:          [2]string{"b", "c"},
			closedTS:      makeTS(4),
			expResolvedTS: makeTS(4),
		},
		{
			name:                  "low intent cleanup age",
			span:                  [2]string{"a", "z"},
			closedTS:              makeTS(4),
			intentCleanupAge:      5,
			expResolvedTS:         makeTS(2).Prev(),
			expEncounteredIntents: []string{"d"},
		},
		{
			name:                  "very low intent cleanup age",
			span:                  [2]string{"a", "z"},
			closedTS:              makeTS(4),
			intentCleanupAge:      2,
			expResolvedTS:         makeTS(2).Prev(),
			expEncounteredIntents: []string{"a", "d", "e"},
		},
		{
			name:                  "very low intent cleanup age, low max encountered intents",
			span:                  [2]string{"a", "z"},
			closedTS:              makeTS(4),
			intentCleanupAge:      2,
			maxEncounteredIntents: 2,
			expResolvedTS:         makeTS(2).Prev(),
			expEncounteredIntents: []string{"a", "d"},
		},
		{
			name:                         "very low intent cleanup age, low max encountered intent key bytes",
			span:                         [2]string{"a", "z"},
			closedTS:                     makeTS(4),
			intentCleanupAge:             2,
			maxEncounteredIntents:        2,
			maxEncounteredIntentKeyBytes: 1,
			expResolvedTS:                makeTS(2).Prev(),
			expEncounteredIntents:        []string{"a"},
		},
	} {
		t.Run(cfg.name, func(t *testing.T) {
			// Defaults.
			if cfg.maxEncounteredIntents == 0 {
				cfg.maxEncounteredIntents = 10
			}
			if cfg.maxEncounteredIntentKeyBytes == 0 {
				cfg.maxEncounteredIntentKeyBytes = 10
			}
			if cfg.intentCleanupAge == 0 {
				cfg.intentCleanupAge = 10
			}

			st := cluster.MakeTestingClusterSettings()
			gc.MaxLocksPerCleanupBatch.Override(ctx, &st.SV, cfg.maxEncounteredIntents)
			gc.MaxLockKeyBytesPerCleanupBatch.Override(ctx, &st.SV, cfg.maxEncounteredIntentKeyBytes)
			QueryResolvedTimestampIntentCleanupAge.Override(ctx, &st.SV, cfg.intentCleanupAge)

			clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 10)))

			evalCtx := &MockEvalCtx{
				ClusterSettings: st,
				Clock:           clock,
				ClosedTimestamp: cfg.closedTS,
			}
			cArgs := CommandArgs{
				EvalCtx: evalCtx.EvalContext(),
				Args: &kvpb.QueryResolvedTimestampRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:    roachpb.Key(cfg.span[0]),
						EndKey: roachpb.Key(cfg.span[1]),
					},
				},
			}

			var resp kvpb.QueryResolvedTimestampResponse
			res, err := QueryResolvedTimestamp(ctx, db, cArgs, &resp)
			require.NoError(t, err)
			require.Equal(t, cfg.expResolvedTS, resp.ResolvedTS)

			var enc []string
			for _, in := range res.Local.EncounteredIntents {
				enc = append(enc, string(in.Key))
			}
			require.Equal(t, cfg.expEncounteredIntents, enc)
		})
	}
}

func TestQueryResolvedTimestampErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	txnUUID := uuid.FromUint128(uint128.FromInts(0, 12345))

	lockTableKey := storage.LockTableKey{
		Key:      roachpb.Key("a"),
		Strength: lock.Intent,
		TxnUUID:  txnUUID,
	}
	engineKey, buf := lockTableKey.ToEngineKey(nil)

	st := cluster.MakeTestingClusterSettings()

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 10)))

	evalCtx := &MockEvalCtx{
		ClusterSettings: st,
		Clock:           clock,
		ClosedTimestamp: hlc.Timestamp{WallTime: 5},
	}
	cArgs := CommandArgs{
		EvalCtx: evalCtx.EvalContext(),
		Args: &kvpb.QueryResolvedTimestampRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    roachpb.Key("a"),
				EndKey: roachpb.Key("z"),
			},
		},
	}
	var resp kvpb.QueryResolvedTimestampResponse
	t.Run("LockTable entry without MVCC metadata", func(t *testing.T) {
		require.NoError(t, db.PutEngineKey(engineKey, buf))
		_, err := QueryResolvedTimestamp(ctx, db, cArgs, &resp)
		require.Error(t, err)
		require.Regexp(t, "unmarshaling mvcc meta", err.Error())
	})
	t.Run("LockTable entry without txn in metadata", func(t *testing.T) {
		var meta enginepb.MVCCMetadata
		// we're omitting meta.TxnMeta
		val, err := protoutil.Marshal(&meta)
		require.NoError(t, err)
		require.NoError(t, db.PutEngineKey(engineKey, val))
		resp.Reset()
		_, err = QueryResolvedTimestamp(ctx, db, cArgs, &resp)
		require.Error(t, err)
		require.Regexp(t, "nil transaction in LockTable", err.Error())
	})
}
