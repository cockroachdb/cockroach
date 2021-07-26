// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		require.NoError(t, storage.MVCCDelete(ctx, db, nil, roachpb.Key(k), makeTS(ts), nil))
	}
	writeIntent := func(k string, ts int64) {
		txn := roachpb.MakeTransaction("test", roachpb.Key(k), 0, makeTS(ts), 0)
		require.NoError(t, storage.MVCCDelete(ctx, db, nil, roachpb.Key(k), makeTS(ts), &txn))
	}
	writeInline := func(k string) {
		require.NoError(t, storage.MVCCDelete(ctx, db, nil, roachpb.Key(k), hlc.Timestamp{}, nil))
	}

	// Setup:
	//
	//  a: intent @ 5
	//  a: value  @ 3
	//  b: inline value
	//  c: value  @ 6
	//  c: value  @ 4
	//  c: value  @ 1
	//  d: intent @ 2
	//  e: intent @ 7
	//  f: inline value
	//
	// NB: must write each key in increasing timestamp order.
	writeValue("a", 3)
	writeIntent("a", 5)
	writeInline("b")
	writeValue("c", 1)
	writeValue("c", 4)
	writeIntent("d", 2)
	writeIntent("e", 7)
	writeInline("f")

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
			gc.MaxIntentsPerCleanupBatch.Override(ctx, &st.SV, cfg.maxEncounteredIntents)
			gc.MaxIntentKeyBytesPerCleanupBatch.Override(ctx, &st.SV, cfg.maxEncounteredIntentKeyBytes)
			QueryResolvedTimestampIntentCleanupAge.Override(ctx, &st.SV, cfg.intentCleanupAge)

			manual := hlc.NewManualClock(10)
			clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

			evalCtx := &MockEvalCtx{
				ClusterSettings: st,
				Clock:           clock,
				ClosedTimestamp: cfg.closedTS,
			}
			cArgs := CommandArgs{
				EvalCtx: evalCtx.EvalContext(),
				Args: &roachpb.QueryResolvedTimestampRequest{
					RequestHeader: roachpb.RequestHeader{
						Key:    roachpb.Key(cfg.span[0]),
						EndKey: roachpb.Key(cfg.span[1]),
					},
				},
			}

			var resp roachpb.QueryResolvedTimestampResponse
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
