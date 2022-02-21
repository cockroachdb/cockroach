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
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	sstFile := &MemFile{}
	sstWriter := MakeBackupSSTWriter(context.Background(), cs, sstFile)
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

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			ctx := context.Background()
			engine := engineImpl.create(Settings(cs))
			defer engine.Close()

			// Write some committed keys and intents at txn1TS.
			batch := engine.NewBatch()
			for _, key := range keys {
				require.NoError(t, batch.PutMVCC(MVCCKey{Key: roachpb.Key(key), Timestamp: txn1TS}, []byte("value")))
			}
			for _, key := range intents {
				require.NoError(t, MVCCPut(ctx, batch, nil, roachpb.Key(key), txn1TS, roachpb.MakeValueFromString("intent"), txn1))
			}
			require.NoError(t, batch.Commit(true))
			batch.Close()
			require.NoError(t, engine.Flush())

			for _, tc := range testcases {
				t.Run(fmt.Sprintf("maxIntents=%d", tc.maxIntents), func(t *testing.T) {
					// Provoke and check WriteIntentErrors.
					startKey, endKey := MVCCKey{Key: roachpb.Key(start)}, MVCCKey{Key: roachpb.Key(end)}
					_, err := CheckSSTConflicts(ctx, sstFile.Bytes(), engine, startKey, endKey,
						false /*disallowShadowing*/, hlc.Timestamp{} /*disallowShadowingBelow*/, tc.maxIntents)
					require.Error(t, err)
					writeIntentErr := &roachpb.WriteIntentError{}
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
	const (
		modeZero    = iota + 1 // all zeroes
		modeCounter            // uint64 counter in first 8 bytes
		modeRandom             // random values

		concurrency = 0 // 0 uses naïve replacement
		sstSize     = 0
		keyCount    = 500000
		valueSize   = 8
		valueMode   = modeRandom
		profile     = false // cpuprofile.pprof
	)

	if sstSize > 0 && keyCount > 0 {
		b.Fatal("Can't set both sstSize and keyCount")
	}

	b.StopTimer()

	r := rand.New(rand.NewSource(7))

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemFile{}
	writer := MakeIngestionSSTWriter(ctx, st, sstFile)
	defer writer.Close()

	key := make([]byte, 8)
	value := make([]byte, valueSize)
	sstTimestamp := hlc.Timestamp{WallTime: 1}
	var i uint64
	for i = 0; (keyCount > 0 && i < keyCount) || (sstSize > 0 && sstFile.Len() < sstSize); i++ {
		binary.BigEndian.PutUint64(key, i)

		switch valueMode {
		case modeZero:
		case modeCounter:
			binary.BigEndian.PutUint64(value, i)
		case modeRandom:
			r.Read(value)
		default:
			b.Fatalf("unknown value mode %d", valueMode)
		}

		var v roachpb.Value
		v.SetBytes(value)
		v.InitChecksum(key)

		require.NoError(b, writer.PutMVCC(MVCCKey{Key: key, Timestamp: sstTimestamp}, v.RawBytes))
	}
	writer.Close()
	b.Logf("%vMB %v keys", sstFile.Len()/1e6, i)

	if profile {
		f, err := os.Create("cpuprofile.pprof")
		require.NoError(b, err)
		defer f.Close()

		require.NoError(b, pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}

	requestTimestamp := hlc.Timestamp{WallTime: 1634899098417970999, Logical: 9}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := UpdateSSTTimestamps(
			ctx, st, sstFile.Bytes(), sstTimestamp, requestTimestamp, concurrency)
		require.NoError(b, err)
	}
}
