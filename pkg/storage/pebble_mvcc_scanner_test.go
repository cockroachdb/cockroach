// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestMVCCScanWithManyVersionsAndSeparatedIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We default to separated intents enabled.
	eng, err := Open(context.Background(), InMemory(),
		cluster.MakeClusterSettings(),
		CacheSize(1<<20))
	require.NoError(t, err)
	defer eng.Close()

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")}
	// Many versions of each key.
	for i := 1; i < 10; i++ {
		mvccValue := MVCCValue{Value: roachpb.MakeValueFromString(fmt.Sprintf("%d", i))}
		for _, k := range keys {
			mvccKey := MVCCKey{Key: k, Timestamp: hlc.Timestamp{WallTime: int64(i)}}
			require.NoError(t, eng.PutMVCC(mvccKey, mvccValue))
		}
	}
	// Write a separated lock for the latest version of each key, to make it provisional.
	uuid := uuid.FromUint128(uint128.FromInts(1, 1))
	meta := enginepb.MVCCMetadata{
		Txn: &enginepb.TxnMeta{
			ID:             uuid,
			WriteTimestamp: hlc.Timestamp{WallTime: 9},
		},
		Timestamp:           hlc.LegacyTimestamp{WallTime: 9},
		Deleted:             false,
		KeyBytes:            2, // arbitrary
		ValBytes:            2, // arbitrary
		RawBytes:            nil,
		IntentHistory:       nil,
		MergeTimestamp:      nil,
		TxnDidNotUpdateMeta: nil,
	}

	metaBytes, err := protoutil.Marshal(&meta)
	require.NoError(t, err)

	for _, k := range keys {
		lockTableKey, _ := LockTableKey{
			Key:      k,
			Strength: lock.Intent,
			TxnUUID:  uuid,
		}.ToEngineKey(nil)
		err = eng.PutEngineKey(lockTableKey, metaBytes)
		require.NoError(t, err)
	}

	reader := eng.NewReader(StandardDurability)
	defer reader.Close()
	iter, err := reader.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: keys[0], UpperBound: roachpb.Key("d")})
	require.NoError(t, err)
	defer iter.Close()

	// Look for older versions that come after the scanner has exhausted its
	// next optimization and does a seek. The seek key had a bug that caused the
	// scanner to skip keys that it desired to see.
	ts := hlc.Timestamp{WallTime: 2}
	mvccScanner := pebbleMVCCScanner{
		parent:           iter,
		memAccount:       mon.NewStandaloneUnlimitedAccount(),
		reverse:          false,
		start:            keys[0],
		end:              roachpb.Key("d"),
		ts:               ts,
		inconsistent:     false,
		tombstones:       false,
		failOnMoreRecent: false,
	}
	var results pebbleResults
	mvccScanner.init(nil /* txn */, uncertainty.Interval{}, &results)
	_, _, _, err = mvccScanner.scan(context.Background())
	require.NoError(t, err)

	kvData := results.finish()
	numKeys := results.count
	require.Equal(t, 3, int(numKeys))
	type kv struct {
		k MVCCKey
		v []byte
	}
	kvs := make([]kv, numKeys)
	var i int
	require.NoError(t, MVCCScanDecodeKeyValues(kvData, func(k MVCCKey, v []byte) error {
		kvs[i].k = k
		kvs[i].v = v
		i++
		return nil
	}))
	expectedKVs := make([]kv, len(keys))
	for i := range expectedKVs {
		expectedKVs[i].k = MVCCKey{Key: keys[i], Timestamp: hlc.Timestamp{WallTime: 2}}
		expectedKVs[i].v = roachpb.MakeValueFromString("2").RawBytes
	}
	require.Equal(t, expectedKVs, kvs)
}

func TestMVCCScanWithLargeKeyValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test has been observed to trip the disk-stall detector under -race.
	skip.UnderRace(t, "large copies and memfs mutexes can cause excessive delays within VFS stack")

	eng := createTestPebbleEngine()
	defer eng.Close()

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"), roachpb.Key("d")}
	largeValue := bytes.Repeat([]byte("l"), 150<<20)
	// Alternate small and large values.
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[0], Timestamp: hlc.Timestamp{WallTime: 1}},
		MVCCValue{Value: roachpb.MakeValueFromBytes([]byte("a"))}))
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[1], Timestamp: hlc.Timestamp{WallTime: 1}},
		MVCCValue{Value: roachpb.MakeValueFromBytes(largeValue)}))
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[2], Timestamp: hlc.Timestamp{WallTime: 1}},
		MVCCValue{Value: roachpb.MakeValueFromBytes([]byte("c"))}))
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[3], Timestamp: hlc.Timestamp{WallTime: 1}},
		MVCCValue{Value: roachpb.MakeValueFromBytes(largeValue)}))

	reader := eng.NewReader(StandardDurability)
	defer reader.Close()
	iter, err := reader.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: keys[0], UpperBound: roachpb.Key("e")})
	require.NoError(t, err)
	defer iter.Close()

	ts := hlc.Timestamp{WallTime: 2}
	mvccScanner := pebbleMVCCScanner{
		parent:     iter,
		memAccount: mon.NewStandaloneUnlimitedAccount(),
		reverse:    false,
		start:      keys[0],
		end:        roachpb.Key("e"),
		ts:         ts,
	}
	var results pebbleResults
	mvccScanner.init(nil /* txn */, uncertainty.Interval{}, &results)
	_, _, _, err = mvccScanner.scan(context.Background())
	require.NoError(t, err)

	kvData := results.finish()
	numKeys := results.count
	require.Equal(t, 4, int(numKeys))
	require.Equal(t, 4, len(kvData))
	require.Equal(t, 25, len(kvData[0]))
	require.Equal(t, 32, cap(kvData[0]))
	require.Equal(t, 157286424, len(kvData[1]))
	require.Equal(t, 157286424, cap(kvData[1]))
	require.Equal(t, 25, len(kvData[2]))
	require.Equal(t, 32, cap(kvData[2]))
	require.Equal(t, 157286424, len(kvData[3]))
	require.Equal(t, 157286424, cap(kvData[3]))
}

func scannerWithAccount(
	ctx context.Context, st *cluster.Settings, scanner *pebbleMVCCScanner, limitBytes int64,
) (cleanup func()) {
	m := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test"),
		Increment: 1,
		Settings:  st,
	})
	m.Start(ctx, nil, mon.NewStandaloneBudget(limitBytes))
	ba := m.MakeBoundAccount()
	scanner.memAccount = &ba
	return func() {
		ba.Close(ctx)
		m.Stop(ctx)
	}
}

func TestMVCCScanWithMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	eng := createTestPebbleEngine()
	defer eng.Close()

	// Write 10 key-value pairs of 1000 bytes each.
	txnID1 := uuid.FromUint128(uint128.FromInts(0, 1))
	ts1 := hlc.Timestamp{WallTime: 50}
	txn1 := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             txnID1,
			Key:            []byte("foo"),
			WriteTimestamp: ts1,
			MinTimestamp:   ts1,
		},
		Status:                 roachpb.PENDING,
		ReadTimestamp:          ts1,
		GlobalUncertaintyLimit: ts1,
	}
	ui1 := uncertainty.Interval{GlobalLimit: txn1.GlobalUncertaintyLimit}
	val := roachpb.Value{RawBytes: bytes.Repeat([]byte("v"), 1000)}
	func() {
		batch := eng.NewBatch()
		defer batch.Close()
		for i := 0; i < 10; i++ {
			key := makeKey(nil, i)
			_, err := MVCCPut(context.Background(), batch, key, ts1, val, MVCCWriteOptions{Txn: &txn1})
			require.NoError(t, err)
		}
		require.NoError(t, batch.Commit(true))
	}()

	// iterator that can span over all the written keys.
	iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: makeKey(nil, 0), UpperBound: makeKey(nil, 11)})
	require.NoError(t, err)
	defer iter.Close()

	// Narrow scan succeeds with a budget of 6000.
	scanner := &pebbleMVCCScanner{
		parent: iter,
		start:  makeKey(nil, 9),
		end:    makeKey(nil, 11),
		ts:     hlc.Timestamp{WallTime: 50},
	}
	scanner.init(&txn1, ui1, &pebbleResults{})
	cleanup := scannerWithAccount(ctx, st, scanner, 6000)
	resumeSpan, resumeReason, resumeNextBytes, err := scanner.scan(ctx)
	require.Nil(t, resumeSpan)
	require.Zero(t, resumeReason)
	require.Zero(t, resumeNextBytes)
	require.Nil(t, err)
	cleanup()

	// Wider scan fails with a budget of 6000.
	scanner = &pebbleMVCCScanner{
		parent: iter,
		start:  makeKey(nil, 0),
		end:    makeKey(nil, 11),
		ts:     hlc.Timestamp{WallTime: 50},
	}
	scanner.init(&txn1, ui1, &pebbleResults{})
	cleanup = scannerWithAccount(ctx, st, scanner, 6000)
	resumeSpan, resumeReason, resumeNextBytes, err = scanner.scan(ctx)
	require.Nil(t, resumeSpan)
	require.Zero(t, resumeReason)
	require.Zero(t, resumeNextBytes)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "memory budget exceeded")
	cleanup()

	// Inconsistent and consistent scans that see intents fails with a budget of 200 (each
	// intent causes 57 bytes to be reserved).
	for _, inconsistent := range []bool{false, true} {
		scanner = &pebbleMVCCScanner{
			parent:       iter,
			start:        makeKey(nil, 0),
			end:          makeKey(nil, 11),
			ts:           hlc.Timestamp{WallTime: 50},
			inconsistent: inconsistent,
		}
		scanner.init(nil, uncertainty.Interval{}, &pebbleResults{})
		cleanup = scannerWithAccount(ctx, st, scanner, 100)
		resumeSpan, resumeReason, resumeNextBytes, err = scanner.scan(ctx)
		require.Nil(t, resumeSpan)
		require.Zero(t, resumeReason)
		require.Zero(t, resumeNextBytes)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "memory budget exceeded")
		cleanup()
	}
}

// TestMVCCScanWithMVCCValueHeaders tests that when the rawMVCCValues
// option is given to pebbleMVCCScanner, the returned values can be
// parsed using the extended encoding and the value header is
// preserved.
func TestMVCCScanWithMVCCValueHeaders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	eng, err := Open(context.Background(), InMemory(),
		cluster.MakeClusterSettings(),
		CacheSize(1<<20))
	require.NoError(t, err)
	defer eng.Close()

	// We write
	//
	// a@1                       with ValueHeader
	// d@1                       without ValueHeader
	//
	// d-e@2 (DelRange)          with ValueHeader
	//
	// c1@3                      with ValueHeader
	// c2@3                      with ValueHeader
	// c2@4                      without ValueHeader
	//
	// b@3 Seq = 0 (provisional) with ValueHeader
	// b@3 Seq = 1 (provisional) without ValueHeader
	//
	// We then read with tombstones at ts3,seq=0 and expect to see
	// 5 values all with value headers:
	//
	// a@1
	// b@3 Seq = 0
	// c1@3
	// c2@3
	// d@2 (synthesized from range key)
	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC1 := roachpb.Key("c1")
	keyC2 := roachpb.Key("c2")
	keyD := roachpb.Key("d")
	keyE := roachpb.Key("e")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}
	expectedOriginID := uint32(42)

	writeValue := func(key roachpb.Key, ts hlc.Timestamp, txn *roachpb.Transaction, originID uint32) {
		_, err := MVCCPut(ctx, eng, key, ts,
			roachpb.MakeValueFromString(fmt.Sprintf("%s-val", key)),
			MVCCWriteOptions{
				Txn:      txn,
				OriginID: originID,
			},
		)
		require.NoError(t, err)
	}

	writeValue(keyA, ts1, nil, expectedOriginID)
	writeValue(keyD, ts1, nil, 0)

	require.NoError(t, eng.PutMVCCRangeKey(MVCCRangeKey{StartKey: keyD, EndKey: keyE, Timestamp: ts2}, MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{OriginID: expectedOriginID},
	}))

	txn1 := roachpb.MakeTransaction("test", nil, isolation.Serializable, roachpb.NormalUserPriority,
		ts3, 1, 1, 0, false /* omitInRangefeeds */)
	writeValue(keyB, ts3, &txn1, expectedOriginID)
	txn1.Sequence++
	writeValue(keyB, ts3, &txn1, 0)
	txn1.Sequence--

	writeValue(keyC1, ts3, nil, expectedOriginID)
	writeValue(keyC2, ts3, nil, expectedOriginID)
	writeValue(keyC2, ts4, nil, 0)

	reader := eng.NewReader(StandardDurability)
	defer reader.Close()

	createScanner := func(startKey roachpb.Key) (*pebbleMVCCScanner, func()) {
		iter, err := reader.NewMVCCIterator(ctx, MVCCKeyAndIntentsIterKind, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: startKey,
			UpperBound: keyE})
		require.NoError(t, err)
		return &pebbleMVCCScanner{
			parent:        iter,
			memAccount:    mon.NewStandaloneUnlimitedAccount(),
			start:         keyA,
			end:           keyE,
			ts:            ts3,
			tombstones:    true,
			rawMVCCValues: true,
		}, iter.Close
	}
	checkKVData := func(kvData [][]byte) {
		require.NoError(t, MVCCScanDecodeKeyValues(kvData, func(k MVCCKey, v []byte) error {
			mvccValue, err := DecodeMVCCValue(v)
			require.NoError(t, err)
			require.Equal(t, expectedOriginID, mvccValue.OriginID)
			return nil
		}))
	}

	t.Run("scan", func(t *testing.T) {
		t.Run("maxIterBeforeSeek=0", func(t *testing.T) {
			oldMaxItersBeforeSeek := maxItersBeforeSeek
			defer func() { maxItersBeforeSeek = oldMaxItersBeforeSeek }()
			maxItersBeforeSeek = 0

			mvccScanner, cleanup := createScanner(keyA)
			defer cleanup()

			var results pebbleResults
			mvccScanner.init(&txn1, uncertainty.Interval{}, &results)
			_, _, _, err = mvccScanner.scan(ctx)
			require.NoError(t, err)
			kvData := results.finish()
			require.Equal(t, int64(5), results.count)
			checkKVData(kvData)
		})
		t.Run("maxIterBeforeSeek=default", func(t *testing.T) {
			mvccScanner, cleanup := createScanner(keyA)
			defer cleanup()

			var results pebbleResults
			mvccScanner.init(&txn1, uncertainty.Interval{}, &results)
			_, _, _, err = mvccScanner.scan(ctx)
			require.NoError(t, err)
			kvData := results.finish()
			require.Equal(t, int64(5), results.count)
			checkKVData(kvData)
		})
	})
	t.Run("get", func(t *testing.T) {
		getKeyWithScanner := func(key roachpb.Key) {
			mvccScanner, cleanup := createScanner(key)
			defer cleanup()

			var results pebbleResults
			mvccScanner.init(&txn1, uncertainty.Interval{}, &results)
			mvccScanner.get(ctx)
			kvData := results.finish()
			require.Equal(t, int64(1), results.count)
			checkKVData(kvData)
		}
		getKeyWithScanner(keyA)
		getKeyWithScanner(keyB)
		getKeyWithScanner(keyC1)
		getKeyWithScanner(keyC2)
		getKeyWithScanner(keyD)
		// This is a key covered by a range tombstone but not
		// otherwise written to. In this case, get() will
		// still synthesize a tombstone.
		getKeyWithScanner(roachpb.Key("dd"))
	})
}
