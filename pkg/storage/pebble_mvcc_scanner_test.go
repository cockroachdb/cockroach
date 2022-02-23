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
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
		CacheSize(1<<20))
	require.NoError(t, err)
	defer eng.Close()

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")}
	// Many versions of each key.
	for i := 1; i < 10; i++ {
		for _, k := range keys {
			require.NoError(t, eng.PutMVCC(MVCCKey{Key: k, Timestamp: hlc.Timestamp{WallTime: int64(i)}},
				[]byte(fmt.Sprintf("%d", i))))
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
		err = eng.PutIntent(context.Background(), k, metaBytes, uuid)
		require.NoError(t, err)
	}

	reader := eng.NewReadOnly(StandardDurability)
	defer reader.Close()
	iter := reader.NewMVCCIterator(
		MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: keys[0], UpperBound: roachpb.Key("d")})
	defer iter.Close()

	// Look for older versions that come after the scanner has exhausted its
	// next optimization and does a seek. The seek key had a bug that caused the
	// scanner to skip keys that it desired to see.
	ts := hlc.Timestamp{WallTime: 2}
	mvccScanner := pebbleMVCCScanner{
		parent:           iter,
		reverse:          false,
		start:            keys[0],
		end:              roachpb.Key("d"),
		ts:               ts,
		inconsistent:     false,
		tombstones:       false,
		failOnMoreRecent: false,
	}
	mvccScanner.init(nil /* txn */, uncertainty.Interval{}, 0 /* trackLastOffsets */)
	_, _, _, err = mvccScanner.scan(context.Background())
	require.NoError(t, err)

	kvData := mvccScanner.results.finish()
	numKeys := mvccScanner.results.count
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
		expectedKVs[i].v = []byte("2")
	}
	require.Equal(t, expectedKVs, kvs)
}

func TestMVCCScanWithLargeKeyValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := createTestPebbleEngine()
	defer eng.Close()

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"), roachpb.Key("d")}
	largeValue := bytes.Repeat([]byte("l"), 150<<20)
	// Alternate small and large values.
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[0], Timestamp: hlc.Timestamp{WallTime: 1}},
		[]byte("a")))
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[1], Timestamp: hlc.Timestamp{WallTime: 1}},
		largeValue))
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[2], Timestamp: hlc.Timestamp{WallTime: 1}},
		[]byte("c")))
	require.NoError(t, eng.PutMVCC(MVCCKey{Key: keys[3], Timestamp: hlc.Timestamp{WallTime: 1}},
		largeValue))

	reader := eng.NewReadOnly(StandardDurability)
	defer reader.Close()
	iter := reader.NewMVCCIterator(
		MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: keys[0], UpperBound: roachpb.Key("e")})
	defer iter.Close()

	ts := hlc.Timestamp{WallTime: 2}
	mvccScanner := pebbleMVCCScanner{
		parent:  iter,
		reverse: false,
		start:   keys[0],
		end:     roachpb.Key("e"),
		ts:      ts,
	}
	mvccScanner.init(nil /* txn */, uncertainty.Interval{}, 0 /* trackLastOffsets */)
	_, _, _, err := mvccScanner.scan(context.Background())
	require.NoError(t, err)

	kvData := mvccScanner.results.finish()
	numKeys := mvccScanner.results.count
	require.Equal(t, 4, int(numKeys))
	require.Equal(t, 4, len(kvData))
	require.Equal(t, 20, len(kvData[0]))
	require.Equal(t, 32, cap(kvData[0]))
	require.Equal(t, 157286419, len(kvData[1]))
	require.Equal(t, 157286419, cap(kvData[1]))
	require.Equal(t, 20, len(kvData[2]))
	require.Equal(t, 32, cap(kvData[2]))
	require.Equal(t, 157286419, len(kvData[3]))
	require.Equal(t, 157286419, cap(kvData[3]))
}

func scannerWithAccount(
	ctx context.Context, st *cluster.Settings, scanner *pebbleMVCCScanner, limitBytes int64,
) (cleanup func()) {
	m := mon.NewMonitor("test", mon.MemoryResource, nil, nil, 1, math.MaxInt64, st)
	m.Start(ctx, nil, mon.MakeStandaloneBudget(limitBytes))
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
			require.NoError(t, MVCCPut(context.Background(), batch, nil, key, ts1, val, &txn1))
		}
		require.NoError(t, batch.Commit(true))
	}()

	// iterator that can span over all the written keys.
	iter := eng.NewMVCCIterator(MVCCKeyAndIntentsIterKind,
		IterOptions{LowerBound: makeKey(nil, 0), UpperBound: makeKey(nil, 11)})
	defer iter.Close()

	// Narrow scan succeeds with a budget of 6000.
	scanner := &pebbleMVCCScanner{
		parent: iter,
		start:  makeKey(nil, 9),
		end:    makeKey(nil, 11),
		ts:     hlc.Timestamp{WallTime: 50},
	}
	scanner.init(&txn1, ui1, 0 /* trackLastOffsets */)
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
	scanner.init(&txn1, ui1, 0 /* trackLastOffsets */)
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
		scanner.init(nil, uncertainty.Interval{}, 0 /* trackLastOffsets */)
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
