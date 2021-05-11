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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestMVCCScanWithManyVersionsAndSeparatedIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Force separated intents for writing.
	settings := makeSettingsForSeparatedIntents(
		false /* oldClusterVersion */, true /* enabled */)
	eng := createTestPebbleEngineWithSettings(settings)
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
		_, err = eng.PutIntent(
			context.Background(), k, metaBytes, NoExistingIntent, true /* txnDidNotUpdateMeta */, uuid)
		require.NoError(t, err)
	}

	reader := eng.NewReadOnly()
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
	mvccScanner.init(nil /* txn */, hlc.Timestamp{})
	_, err = mvccScanner.scan()
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

	reader := eng.NewReadOnly()
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
	mvccScanner.init(nil /* txn */, hlc.Timestamp{})
	_, err := mvccScanner.scan()
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
