// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatchupScanWithDiffAndTBI tests that catchup scans using the
// withDiff option return the same data with and without TBI enable.
func TestCatchupScanWithDiffAndTBI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		putFn              func(t *testing.T, db storage.Engine)
		ingestFn           func(t *testing.T, it storage.MVCCIterator, db storage.Engine)
		ts                 hlc.Timestamp
		startKey           roachpb.Key
		endKey             roachpb.Key
		expectedEventCount int
	}

	testCases := map[string]testCase{
		// SST 1:    (max_ts: 5, min_ts: 6)
		//      a@6
		//      a@5
		//
		// SST 2:    (max_ts: 3, min_ts: 1)
		//      a@3  <- skipped by TBI
		//      a@2
		//      a@1
		// SST 3:    (max_ts: 5, min_ts: 5)
		//      b@5
		"previous value skipped by TBI": {
			putFn: func(t *testing.T, db storage.Engine) {
				put(t, db, "a", "a1 value", 1)
				put(t, db, "a", "a2 value", 2)
				put(t, db, "a", "a3 value", 3)
				put(t, db, "a", "a5 value", 5)
				put(t, db, "a", "a6 value", 6)
				put(t, db, "b", "b5 value", 5)
			},
			ingestFn: func(t *testing.T, it storage.MVCCIterator, db storage.Engine) {
				ingest(t, it, db, 2)
				ingest(t, it, db, 3)
				ingest(t, it, db, 1)
			},
			ts:                 hlc.Timestamp{WallTime: 4},
			startKey:           roachpb.Key("a"),
			endKey:             roachpb.Key("d"),
			expectedEventCount: 3,
		},
		// SST 1:    (max_ts: 6, min_ts: 6)
		//      a@6
		//      a@5  <- MVCCIncrementalIterator will be invalid after this key
		//
		// SST 2:    (max_ts: 3, min_ts: 1)
		//      a@3  <- skipped by TBI
		//      a@2
		//      a@1
		"previous value skipped by TBI with invalid next": {
			putFn: func(t *testing.T, db storage.Engine) {
				put(t, db, "a", "a1 value", 1)
				put(t, db, "a", "a2 value", 2)
				put(t, db, "a", "a3 value", 3)
				put(t, db, "a", "a5 value", 5)
				put(t, db, "a", "a6 value", 6)
			},
			ingestFn: func(t *testing.T, it storage.MVCCIterator, db storage.Engine) {
				ingest(t, it, db, 2)
				ingest(t, it, db, 3)
			},
			ts:                 hlc.Timestamp{WallTime: 4},
			startKey:           roachpb.Key("a"),
			endKey:             roachpb.Key("d"),
			expectedEventCount: 2,
		},
		// SST 1:    (max_ts: 6, min_ts: 1)
		//      a@6
		//      a@5
		//      a@3
		//      a@2
		//      a@1
		"no previous value skipped by TBI": {
			putFn: func(t *testing.T, db storage.Engine) {
				put(t, db, "a", "a1 value", 1)
				put(t, db, "a", "a2 value", 2)
				put(t, db, "a", "a3 value", 3)
				put(t, db, "a", "a5 value", 5)
				put(t, db, "a", "a6 value", 6)
			},
			ingestFn: func(t *testing.T, it storage.MVCCIterator, db storage.Engine) {
				ingest(t, it, db, 5)
			},
			ts:                 hlc.Timestamp{WallTime: 4},
			startKey:           roachpb.Key("a"),
			endKey:             roachpb.Key("d"),
			expectedEventCount: 2,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			db := makeContrivedEngine(t, tc.putFn, tc.ingestFn)
			defer db.Close()
			span := roachpb.Span{
				Key:    tc.startKey,
				EndKey: tc.endKey,
			}
			rfReq := &roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					Timestamp: tc.ts,
				},
				WithDiff: true,
				Span:     span,
			}
			startKey := storage.MakeMVCCMetadataKey(tc.startKey)
			endKey := storage.MakeMVCCMetadataKey(tc.endKey)
			// CatchUpIterator without TBI
			iter := rangefeed.NewCatchUpIterator(db, rfReq, false, func() {})
			defer iter.Close()

			eventBuf1 := []*roachpb.RangeFeedEvent{}
			err := iter.CatchUpScan(startKey, endKey, tc.ts, true, func(e *roachpb.RangeFeedEvent) error {
				eventBuf1 = append(eventBuf1, e)
				return nil
			})
			require.NoError(t, err)

			// CatchupIterator with TBI
			iter2 := rangefeed.NewCatchUpIterator(db, rfReq, true, func() {})
			defer iter2.Close()

			eventBuf2 := []*roachpb.RangeFeedEvent{}
			err = iter2.CatchUpScan(startKey, endKey, tc.ts, true, func(e *roachpb.RangeFeedEvent) error {
				eventBuf2 = append(eventBuf2, e)
				return nil
			})
			require.NoError(t, err)

			assert.Equal(t, eventBuf1, eventBuf2)
			assert.Equal(t, tc.expectedEventCount, len(eventBuf1))
		})
	}
}

// makeContrivedEngine returns a storage.Engine with a specific layout
// of SSTs. We do this by putting values into one engine and then
// ingesting them into a second, using the provided putFn and
// ingestFn.
func makeContrivedEngine(
	t *testing.T,
	putFn func(t *testing.T, db storage.Engine),
	ingestFn func(t *testing.T, it storage.MVCCIterator, db storage.Engine),
) storage.Engine {
	db1 := storage.NewDefaultInMemForTesting()
	defer db1.Close()
	putFn(t, db1)

	db2 := storage.NewDefaultInMemForTesting()
	// Iterate over the entries in the first DB, ingesting
	// them into SSTables in the second DB.
	it := db1.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		UpperBound: keys.MaxKey,
	})
	defer it.Close()
	it.SeekGE(storage.MVCCKey{Key: keys.LocalMax})
	ingestFn(t, it, db2)
	return db2
}

func ingest(t *testing.T, it storage.MVCCIterator, db storage.Engine, count int) {
	memFile := &storage.MemFile{}
	sst := storage.MakeIngestionSSTWriter(memFile)
	defer sst.Close()

	for i := 0; i < count; i++ {
		ok, err := it.Valid()
		require.NoError(t, err)
		require.True(t, ok, "expected key")
		err = sst.Put(it.Key(), it.Value())
		require.NoError(t, err)
		it.Next()
	}
	require.NoError(t, sst.Finish())
	require.NoError(t, db.WriteFile(`ingest`, memFile.Data()))
	require.NoError(t, db.IngestExternalFiles(context.Background(), []string{`ingest`}))
}

func put(t *testing.T, db storage.Engine, key, value string, ts int64) {
	v := roachpb.MakeValueFromString(value)
	err := storage.MVCCPut(
		context.Background(), db, nil, /* stats */
		roachpb.Key(key),
		hlc.Timestamp{WallTime: ts},
		v,
		nil, /* transaction */
	)
	require.NoError(t, err)
}
