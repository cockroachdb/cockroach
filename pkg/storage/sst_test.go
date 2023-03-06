// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCheckSSTConflictsMaxIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	keys := []string{"aa", "bb", "cc", "dd"}
	intents := []string{"a", "b", "c"}
	start, end := "a", "z"

	testcases := []struct {
		maxIntents    int64
		expectIntents []string
	}{
		{maxIntents: -1, expectIntents: []string{"a"}},
		{maxIntents: 0, expectIntents: []string{"a"}},
		{maxIntents: 1, expectIntents: []string{"a"}},
		{maxIntents: 2, expectIntents: []string{"a", "b"}},
		{maxIntents: 3, expectIntents: []string{"a", "b", "c"}},
		{maxIntents: 4, expectIntents: []string{"a", "b", "c"}},
	}

	// Create SST with keys equal to intents at txn2TS.
	cs := cluster.MakeTestingClusterSettings()
	var sstFile bytes.Buffer
	sstWriter := MakeBackupSSTWriter(context.Background(), cs, &sstFile)
	defer sstWriter.Close()
	for _, k := range intents {
		key := MVCCKey{Key: roachpb.Key(k), Timestamp: txn2TS}
		value := roachpb.Value{}
		value.SetString("sst")
		value.InitChecksum(key.Key)
		require.NoError(t, sstWriter.Put(key, value.RawBytes))
	}
	require.NoError(t, sstWriter.Finish())
	sstWriter.Close()

	ctx := context.Background()
	engine, err := Open(context.Background(), InMemory(), cs, MaxSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Write some committed keys and intents at txn1TS.
	batch := engine.NewBatch()
	for _, key := range keys {
		mvccKey := MVCCKey{Key: roachpb.Key(key), Timestamp: txn1TS}
		mvccValue := MVCCValue{Value: roachpb.MakeValueFromString("value")}
		require.NoError(t, batch.PutMVCC(mvccKey, mvccValue))
	}
	for _, key := range intents {
		require.NoError(t, MVCCPut(ctx, batch, nil, roachpb.Key(key), txn1TS, hlc.ClockTimestamp{}, roachpb.MakeValueFromString("intent"), txn1))
	}
	require.NoError(t, batch.Commit(true))
	batch.Close()
	require.NoError(t, engine.Flush())

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("maxIntents=%d", tc.maxIntents), func(t *testing.T) {
			for _, usePrefixSeek := range []bool{false, true} {
				t.Run(fmt.Sprintf("usePrefixSeek=%v", usePrefixSeek), func(t *testing.T) {
					// Provoke and check WriteIntentErrors.
					startKey, endKey := MVCCKey{Key: roachpb.Key(start)}, MVCCKey{Key: roachpb.Key(end)}
					_, err := CheckSSTConflicts(ctx, sstFile.Bytes(), engine, startKey, endKey, startKey.Key, endKey.Key.Next(),
						false /*disallowShadowing*/, hlc.Timestamp{} /*disallowShadowingBelow*/, hlc.Timestamp{} /* sstReqTS */, tc.maxIntents, usePrefixSeek)
					require.Error(t, err)
					writeIntentErr := &kvpb.WriteIntentError{}
					require.ErrorAs(t, err, &writeIntentErr)

					actual := []string{}
					for _, i := range writeIntentErr.Intents {
						actual = append(actual, string(i.Key))
					}
					require.Equal(t, tc.expectIntents, actual)
				})
			}
		})
	}
}

func BenchmarkUpdateSSTTimestamps(b *testing.B) {
	defer log.Scope(b).Close(b)
	skip.UnderShort(b)

	ctx := context.Background()

	for _, numKeys := range []int{1, 10, 100, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, concurrency := range []int{0, 1, 2, 4, 8} { // 0 uses naïve read/write loop
				b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
					runUpdateSSTTimestamps(ctx, b, numKeys, concurrency)
				})
			}
		})
	}
}

func runUpdateSSTTimestamps(ctx context.Context, b *testing.B, numKeys int, concurrency int) {
	const valueSize = 8

	r := rand.New(rand.NewSource(7))
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemObject{}
	writer := MakeIngestionSSTWriter(ctx, st, sstFile)
	defer writer.Close()

	sstTimestamp := hlc.MinTimestamp
	reqTimestamp := hlc.Timestamp{WallTime: 1634899098417970999, Logical: 9}

	key := make([]byte, 8)
	value := make([]byte, valueSize)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		r.Read(value)

		var mvccValue MVCCValue
		mvccValue.Value.SetBytes(value)
		mvccValue.Value.InitChecksum(key)

		if err := writer.PutMVCC(MVCCKey{Key: key, Timestamp: sstTimestamp}, mvccValue); err != nil {
			require.NoError(b, err) // for performance
		}
	}
	require.NoError(b, writer.Finish())

	b.SetBytes(int64(numKeys * (len(key) + len(value))))
	b.ResetTimer()

	var res []byte
	for i := 0; i < b.N; i++ {
		var ms enginepb.MVCCStats
		var err error
		res, _, err = UpdateSSTTimestamps(
			ctx, st, sstFile.Bytes(), sstTimestamp, reqTimestamp, concurrency, &ms)
		if err != nil {
			require.NoError(b, err) // for performance
		}
	}
	_ = res
}
