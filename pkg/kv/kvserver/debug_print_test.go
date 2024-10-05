// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestStringifyWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	wb := &kvserverpb.WriteBatch{}
	_, err := decodeWriteBatch(wb)
	require.ErrorContains(t, err, "batch invalid: too small: 0 bytes")

	batch := pebble.Batch{}
	require.NoError(t, batch.Set(storage.EncodeMVCCKey(storage.MVCCKey{
		Key:       roachpb.Key("/db1"),
		Timestamp: hlc.Timestamp{WallTime: math.MaxInt64},
	}), []byte("test value"), nil /* WriteOptions */))
	wb.Data = batch.Repr()
	s, err := decodeWriteBatch(wb)
	require.NoError(t, err)
	require.Equal(t, "Put: 9223372036.854775807,0 \"/db1\" (0x2f646231007fffffffffffffff09): \"test value\"\n", s)

	batch = pebble.Batch{}
	encodedKey, err := hex.DecodeString("017a6b12c089f704918df70bee8800010003623a9318c0384d07a6f22b858594df6012")
	require.NoError(t, err)
	err = batch.SingleDelete(encodedKey, nil)
	require.NoError(t, err)
	wb.Data = batch.Repr()
	s, err = decodeWriteBatch(wb)
	require.NoError(t, err)
	require.Equal(t, "Single Delete: /Local/Lock/Table/56/1/1169/5/3054/0 "+
		"03623a9318c0384d07a6f22b858594df60 (0x017a6b12c089f704918df70bee8800010003623a9318c0384d07a6f22b858594df6012): \n",
		s)

	batch = pebble.Batch{}
	require.NoError(t, batch.RangeKeySet(
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db1")),
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db2")),
		storage.EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 1}),
		[]byte{},
		nil,
	))
	valueRaw, err := storage.EncodeMVCCValue(storage.MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp{WallTime: 1}},
	})
	require.NoError(t, err)
	require.NoError(t, batch.RangeKeySet(
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db1")),
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db2")),
		storage.EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 2}),
		valueRaw,
		nil,
	))
	wb.Data = batch.Repr()
	s, err = decodeWriteBatch(wb)
	require.NoError(t, err)
	require.Equal(t, "Set Range Key: 0.000000001,0 /db{1-2} (0x2f64623100-0x2f64623200): \"\"\n"+
		"Set Range Key: 0.000000002,0 /db{1-2} (0x2f64623100-0x2f64623200): \"\\x00\\x00\\x00\\x04e\\n\\x02\\b\\x01\"\n",
		s)

	batch = pebble.Batch{}
	require.NoError(t, batch.RangeKeyUnset(
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db1")),
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db2")),
		storage.EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 1}),
		nil,
	))
	require.NoError(t, batch.RangeKeyUnset(
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db1")),
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db2")),
		storage.EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 2}),
		nil,
	))
	wb.Data = batch.Repr()
	s, err = decodeWriteBatch(wb)
	require.NoError(t, err)
	require.Equal(t, "Unset Range Key: 0.000000001,0 /db{1-2} (0x2f64623100-0x2f64623200)\n"+
		"Unset Range Key: 0.000000002,0 /db{1-2} (0x2f64623100-0x2f64623200)\n",
		s)

	batch = pebble.Batch{}
	require.NoError(t, batch.RangeKeyDelete(
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db1")),
		storage.EncodeMVCCKeyPrefix(roachpb.Key("/db2")),
		nil,
	))
	wb.Data = batch.Repr()
	s, err = decodeWriteBatch(wb)
	require.NoError(t, err)
	require.Equal(t, "Delete Range Keys: /db{1-2} (0x2f64623100-0x2f64623200)\n", s)
}
